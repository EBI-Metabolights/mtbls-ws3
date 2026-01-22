import base64
import datetime
import hashlib
import logging
import uuid
from typing import Any, Union

import jwt
from jwt import ExpiredSignatureError, InvalidTokenError

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.enums.jwt_token_content import JwtTokenContent, JwtTokenInput
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.exceptions.auth import AuthenticationError
from mtbls.domain.shared.data_types import TokenStr
from mtbls.infrastructure.auth.standalone.standalone_authentication_config import (
    StandaloneAuthenticationConfiguration,
)

logger = logging.getLogger(__name__)


class AuthenticationServiceImpl(AuthenticationService):
    def __init__(
        self,
        config: Union[StandaloneAuthenticationConfiguration, dict[str, Any]],
        cache_service: CacheService,
        user_read_repository: UserReadRepository,
    ) -> None:
        self.cache_service = cache_service
        if isinstance(config, StandaloneAuthenticationConfiguration):
            self.config = config
        else:
            self.config: StandaloneAuthenticationConfiguration = (
                StandaloneAuthenticationConfiguration.model_validate(config)
            )
        self.user_read_repository = user_read_repository

    @validate_inputs_outputs
    async def authenticate_with_token(
        self, token_type: TokenType, token: TokenStr, username: str = None
    ) -> str:
        if token_type != TokenType.API_TOKEN:
            raise NotImplementedError()
        user = await self.user_read_repository.get_user_by_api_token(username, token)
        if not user:
            raise AuthenticationError(f"Invalid API token '{token[:3]}...{token[-3:]}'")

        return await self.create_jwt_token(
            jwt_token_input=JwtTokenInput(
                sub=user.username, scopes=["login"], role=user.role.name
            )
        )

    async def authenticate_with_password(self, username: str, password: str) -> str:
        user = await self.user_read_repository.get_user_by_username(username)
        if not user:
            raise AuthenticationError(f"Invalid username or password for '{username}'")
        if not await self._verify_password(password, user.password_hash):
            raise AuthenticationError(f"Invalid username or password for '{username}'")

        return await self.create_jwt_token(
            jwt_token_input=JwtTokenInput(
                sub=user.username, scopes=["login"], role=user.role.name
            )
        )

    async def create_jwt_token(
        self,
        jwt_token_input: JwtTokenInput,
        expires_delta_in_minutes: Union[None, datetime.timedelta] = None,
    ) -> str:
        jwt_token_content = JwtTokenContent.model_validate(
            jwt_token_input, from_attributes=True
        )
        now: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
        if expires_delta_in_minutes:
            expire: datetime.datetime = now + expires_delta_in_minutes
        else:
            expire: datetime.datetime = now + datetime.timedelta(
                minutes=self.config.access_token_expires_delta_in_minutes
            )
        jwt_token_content.iat = int(now.timestamp())
        jwt_token_content.exp = int(expire.timestamp())

        jwt_token_content.jti = str(uuid.uuid4())
        jti = jwt_token_content.jti
        key_bytes = bytes(f"{self.config.application_secret_key}-{jti}", "utf-8")
        key = hashlib.sha256(key_bytes).hexdigest()
        return jwt.encode(
            jwt_token_content.model_dump(exclude_defaults=True),
            key,
            algorithm=self.config.access_token_hash_algorithm,
        )

    async def revoke_jwt_token(self, refresh_jwt_token: str) -> bool:
        # TODO: implement it
        raise NotImplementedError()

    async def validate_token(
        self, token_type: TokenType, token: str, username: str = None
    ) -> str:
        if token_type == TokenType.JWT_TOKEN:
            jwt_token = await self.validate_jwt_token(token)
            return jwt_token.sub
        if token_type == TokenType.API_TOKEN:
            user = await self.user_read_repository.get_user_by_api_token(
                username, token
            )
            if not user:
                raise AuthenticationError("Invalid token")
            return user.username
        raise NotImplementedError()

    async def _verify_password(self, plain_password: str, hashed_password: str):
        current_hash = await self.get_password_sha1_hash(plain_password)
        if current_hash == hashed_password:
            # TODO Calculate password hash with new algorithm and update DB
            return True
        return False

    async def get_password_sha1_hash(self, password):
        # SHA1 is not secure but current db contains passwords with SHA1
        byte_value = str.encode(password)
        hash_object = hashlib.sha1(byte_value)
        hex_string_hash = hash_object.hexdigest()
        hash_bytes = bytes.fromhex(hex_string_hash)
        base64_bytes = base64.b64encode(hash_bytes)
        return base64_bytes.decode("ascii")

    async def validate_jwt_token(
        self,
        jwt_token: str,
    ) -> JwtTokenContent:
        payload = None
        options = {
            "verify_signature": False,
            "verify_exp": True,
            "verify_jti": True,
            "verify_sub": True,
            "verify_aud": False,
            "verify_iss": False,
        }
        try:
            payload = jwt.decode(
                jwt_token,
                options=options,
            )
        except (ExpiredSignatureError, InvalidTokenError) as e:
            logger.warning("Token has expired")
            logger.error(e)
            raise e
        except InvalidTokenError as e:
            logger.debug("Token is invalid: %s", str(e))
            raise e
        except Exception as e:
            logger.debug("Token decode error: %s", str(e))
            logger.error(e)
            raise e

        exp = int(payload.get("exp"))
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        if now > exp:
            raise ExpiredSignatureError("Token has expired.")
        jti = payload.get("jti")
        jwt_content = JwtTokenContent(jti=jti, exp=exp, sub=payload.get("sub"))
        key_bytes = bytes(f"{self.config.application_secret_key}-{jti}", "utf-8")
        key = hashlib.sha256(key_bytes).hexdigest()

        options["verify_signature"] = True

        try:
            payload = jwt.decode(
                jwt_token,
                key=key,
                algorithms=[self.config.access_token_hash_algorithm],
                options=options,
            )
        except ExpiredSignatureError as e:
            logger.warning("Token has expired")
            logger.error(e)
            raise e
        except Exception as e:
            logger.debug("Token decode error: %s", str(e))
            logger.error(e)
            raise e

        ############################################################################################################
        #                          Check whether token is revoked
        ############################################################################################################
        if self.config.revocation_management_enabled:
            storage_key = f"{self.config.revoked_access_token_prefix}:{jwt_token}"
            if await self.cache_service.does_key_exist(storage_key):
                InvalidTokenError("Token is revoked")
        return jwt_content
