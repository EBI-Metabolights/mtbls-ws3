import logging
from typing import Any, Union

from keycloak import KeycloakOpenID

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.shared.data_types import TokenStr
from mtbls.infrastructure.auth.keycloak.keycloak_authentication_config import (
    KeycloakAuthenticationConfiguration,
)

logger = logging.getLogger(__name__)


class KeycloakAuthenticationService(AuthenticationService):
    def __init__(
        self,
        config: Union[dict[str, Any], KeycloakAuthenticationConfiguration],
        cache_service: CacheService,
        user_read_repository: UserReadRepository,
        http_client: HttpClient,
    ) -> None:
        self.cache_service = cache_service
        self.http_client = http_client
        if isinstance(config, KeycloakAuthenticationConfiguration):
            self.config = config
        else:
            self.config: KeycloakAuthenticationConfiguration = (
                KeycloakAuthenticationConfiguration.model_validate(config)
            )
        self.user_read_repository = user_read_repository
        self.keycloak_openid = KeycloakOpenID(
            server_url=self.config.host,
            realm_name=self.config.realm_name,
            client_id=self.config.client_id,
            client_secret_key=self.config.client_secret,
        )

    @validate_inputs_outputs
    async def authenticate_with_token(
        self, token_type: TokenType, token: TokenStr, username: str = None
    ) -> str:
        if token_type != TokenType.API_TOKEN:
            raise NotImplementedError()
        keycloak_openid: KeycloakOpenID = KeycloakOpenID(
            server_url=self.config.host,
            realm_name=self.config.realm_name,
            client_id=f"api_user-{username}",
            client_secret_key=token,
        )

        jwt_token = keycloak_openid.token(
            grant_type="client_credentials",
        )
        return jwt_token.get("access_token")

    async def authenticate_with_password(self, username: str, password: str) -> str:
        try:
            token = self.keycloak_openid.token(username, password)
            return token.get("access_token", "")
        except Exception as ex:
            logger.error("error: %s %s", username, str(ex))
            raise ex

    async def revoke_jwt_token(self, refresh_jwt_token: str) -> bool:
        try:
            self.keycloak_openid.logout(refresh_jwt_token)
            return True
        except Exception as ex:
            logger.warning("error: %s", str(ex))
            raise ex

    async def validate_token(
        self, token_type: TokenType, token: str, username: str = None
    ) -> str:
        if token_type != TokenType.JWT_TOKEN:
            raise NotImplementedError("Only Jwt Tokens can be validated.")

        try:
            user_info = self.keycloak_openid.userinfo(token)
            return user_info.get("email", "")
        except Exception as ex:
            logger.warning("error: %s", str(ex))
            raise ex
