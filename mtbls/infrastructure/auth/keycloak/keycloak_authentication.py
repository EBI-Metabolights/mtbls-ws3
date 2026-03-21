import datetime
import logging
import re
from typing import Any, Union

from keycloak import KeycloakAdmin, KeycloakOpenID

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
    UserProfileService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.domain.entities.user import UserProfile
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus
from mtbls.domain.shared.data_types import TokenStr
from mtbls.infrastructure.auth.keycloak.keycloak_authentication_config import (
    KeycloakAuthenticationConfiguration,
)

logger = logging.getLogger(__name__)


class KeycloakAuthenticationService(AuthenticationService, UserProfileService):
    def __init__(
        self,
        config: Union[dict[str, Any], KeycloakAuthenticationConfiguration],
        cache_service: CacheService,
    ) -> None:
        self.cache_service = cache_service
        if isinstance(config, KeycloakAuthenticationConfiguration):
            self.config = config
        else:
            self.config: KeycloakAuthenticationConfiguration = (
                KeycloakAuthenticationConfiguration.model_validate(config)
            )
        self.keycloak_openid = KeycloakOpenID(
            server_url=self.config.host,
            realm_name=self.config.realm_name,
            client_id=self.config.client_id,
            client_secret_key=self.config.client_secret,
        )
        self._keycloak_admin = KeycloakAdmin(
            server_url=self.config.host,
            realm_name=self.config.realm_name,
            username=self.config.admin_username,
            password=self.config.admin_password,
            verify=True,
        )

    @validate_inputs_outputs
    async def authenticate_with_token(
        self, token_type: TokenType, token: TokenStr, username: str = None
    ) -> str:
        if token_type != TokenType.API_TOKEN:
            raise NotImplementedError()

        jwt_token = self.keycloak_openid.token(
            grant_type="client_credentials",
        )
        return jwt_token.get("access_token")

    async def refresh(self, refresh_token: str) -> tuple[str, None | str]:
        if not refresh_token:
            raise NotImplementedError()
        token = self.keycloak_openid.refresh_token(
            grant_type="client_credentials",
        )
        return token.get("access_token"), token.get("refresh_token")

    async def authenticate_with_password(
        self, username: str, password: str
    ) -> tuple[str, None | str]:
        try:
            token = self.keycloak_openid.token(username, password)
            return token.get("access_token", ""), token.get("refresh_token", "")
        except Exception as ex:
            logger.error("error: %s %s", username, str(ex))
            raise ex

    async def revoke_jwt_token(self, refresh_token: str) -> bool:
        try:
            self.keycloak_openid.logout(refresh_token)
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

    async def get_user_profile(self, username: str = None) -> None | UserProfile:
        try:
            user_id = self._keycloak_admin.get_user_id(username=username)
            if not user_id:
                return None
            user = self._keycloak_admin.get_user(
                user_id=user_id, user_profile_metadata=True
            )
            if user:
                roles = self._keycloak_admin.get_composite_realm_roles_of_user(user_id)
                return self.convert_auth_user_info_from_dict(user, roles)
        except Exception as ex:
            raise ex
        return None

    def convert_auth_user_info_from_dict(
        self, dict_data: dict, roles: list[dict]
    ) -> UserProfile:
        user = UserProfile()
        if not dict_data:
            return user
        realm_roles = {x["name"] for x in roles} if roles else set()
        if "study_curation" in realm_roles or "system_maintenance" in realm_roles:
            role = UserRole.CURATOR
        elif "study_submission" in realm_roles:
            role = UserRole.SUBMITTER
        elif "study_review" in realm_roles:
            role = UserRole.REVIEWER
        else:
            role = UserRole.ANONYMOUS
        partner = "partner" in realm_roles
        payload = dict_data
        enabled = payload.get("enabled", False)
        email_verified = payload.get("emailVerified")
        if enabled:
            status = UserStatus.ACTIVE if email_verified else UserStatus.NEW
        else:
            status = UserStatus.FROZEN
        join_date = datetime.datetime.fromtimestamp(
            payload.get("createdTimestamp") / 1000.0
        )

        attributes: dict = dict_data.get("attributes", {})
        orcid = attributes.get("orcid") or [""]
        orcid = re.sub(r"https?://orcid\.org/", "", (orcid[0] or "").lower())
        user.email = payload.get("email")
        user.email_verified = email_verified
        user.first_name = payload.get("firstName")
        user.last_name = payload.get("lastName")
        user.orcid = orcid
        user.role = role
        user.enabled = enabled
        user.status = status
        user.join_date = join_date
        user.country = (attributes.get("country") or [""])[0]
        user.address = (attributes.get("affiliationAddress") or [""])[0]
        user.affiliation = (attributes.get("affiliation") or [""])[0]
        user.affiliation_url = (attributes.get("affiliationUrl") or [""])[0]
        user.globus_username = (attributes.get("globusUserName") or [""])[0]
        user.partner = partner
        return user

    async def get_users_by_query(self, key_value: dict[str, Any]) -> list[UserProfile]:
        users = []
        if not key_value:
            return users
        try:
            users = self._keycloak_admin.get_users(
                {"q": " ".join([f"{k}:{v}" for k, v in key_value.items()])}
            )
        except Exception as ex:
            logger.error("%s", ex)

        if users:
            return [self.convert_auth_user_info_from_dict(x) for x in users]
        return users
