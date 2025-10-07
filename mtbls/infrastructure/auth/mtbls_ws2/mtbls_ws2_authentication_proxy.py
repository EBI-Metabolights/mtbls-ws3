import logging
from typing import Any, Union

import jwt

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.shared.data_types import TokenStr
from mtbls.infrastructure.auth.mtbls_ws2.mtbls_ws2_authentication_config import (
    MtblsWs2AuthenticationConfiguration,
)

logger = logging.getLogger(__name__)


class MtblsWs2AuthenticationProxy(AuthenticationService):
    def __init__(
        self,
        config: Union[dict[str, Any], MtblsWs2AuthenticationConfiguration],
        cache_service: CacheService,
        user_read_repository: UserReadRepository,
        http_client: HttpClient,
    ) -> None:
        self.cache_service = cache_service
        self.http_client = http_client
        if isinstance(config, MtblsWs2AuthenticationConfiguration):
            self.config = config
        else:
            self.config: MtblsWs2AuthenticationConfiguration = (
                MtblsWs2AuthenticationConfiguration.model_validate(config)
            )
        self.backend_ws_base_url = self.config.get_url()
        self.user_read_repository = user_read_repository

    @validate_inputs_outputs
    async def authenticate_with_token(
        self, token_type: TokenType, token: TokenStr
    ) -> str:
        if token_type != TokenType.API_TOKEN:
            raise NotImplementedError()
        url = f"{self.backend_ws_base_url}/auth/login-with-token"
        try:
            response: HttpResponse = await self.http_client.send_request(
                HttpRequestType.POST,
                url,
                headers={"Content-Type": "application/json"},
                json={"token": token},
                timeout=5,
            )
            if response and response.headers and response.headers.get("Jwt"):
                return response.headers.get("Jwt")
            raise Exception("Invalid response from backend")
        except Exception as ex:
            logger.warning("error: %s", str(ex))
            raise ex

    async def authenticate_with_password(self, username: str, password: str) -> str:
        url = f"{self.backend_ws_base_url}/auth/login"
        try:
            logger.info("Login request from: %s", username)
            response: HttpResponse = await self.http_client.send_request(
                HttpRequestType.POST,
                url,
                headers={"Content-Type": "application/json"},
                json={"email": username, "secret": password},
                timeout=5,
            )
            if response and response.headers and response.headers.get("Jwt"):
                return response.headers["Jwt"]

            raise Exception("Invalid response from backend")
        except Exception as ex:
            logger.warning("error: %s %s", url, str(ex))
            raise ex

    async def revoke_jwt_token(self, jwt: str) -> bool:
        raise NotImplementedError()

    async def validate_token(self, token_type: TokenType, token: str) -> str:
        if token_type != TokenType.JWT_TOKEN:
            raise NotImplementedError("Only Jwt Tokens can be validated.")
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
                token,
                options=options,
            )
            email = payload.get("sub")
        except Exception as e:
            logger.warning("Invalid token")
            logger.error(e)
            raise e

        url = f"{self.backend_ws_base_url}/auth/validate-token"
        try:
            response: HttpResponse = await self.http_client.send_request(
                HttpRequestType.POST,
                url,
                headers={"Content-Type": "application/json"},
                json={"Jwt": token, "User": email},
                timeout=5,
            )
            if response and response.headers and response.headers.get("User"):
                return response.headers.get("User")

            raise Exception("Invalid response from backend")
        except Exception as ex:
            logger.warning("error: %s", str(ex))
            raise ex
