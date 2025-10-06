import base64
import binascii
import logging

from starlette.authentication import (
    AuthCredentials,
    AuthenticationBackend,
    AuthenticationError,
)

from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.auth_user import AuthenticatedUser, UnauthenticatedUser
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.enums.user_role import UserRole

logger = logging.getLogger(__name__)


class AuthBackend(AuthenticationBackend):
    def __init__(
        self,
        authentication_service: AuthenticationService,
        user_read_repository: UserReadRepository,
    ):
        self.authentication_service = authentication_service
        self.user_read_repository = user_read_repository

    async def authenticate(self, conn):
        if "Authorization" not in conn.headers:
            return AuthCredentials({"unauthenticated"}), UnauthenticatedUser()

        auth = conn.headers["Authorization"]

        username = await self.validate_credential(auth)

        if not username:
            return AuthCredentials({"unauthenticated"}), UnauthenticatedUser()
        user: UserOutput = await self.user_read_repository.get_user_by_username(
            username
        )
        if not user:
            logger.error(
                "User role check failure. "
                "User %s details are not fetched by from database.",
                username,
            )
            raise AuthenticationError("User details are not fetched from database")
        scopes = {"authenticated"}
        if user.role == UserRole.SUBMITTER:
            scopes.add("submitter")
        elif user.role == UserRole.CURATOR:
            scopes.add("curator")
            scopes.add("submitter")
        elif user.role == UserRole.SYSTEM_ADMIN:
            scopes.add("admin")

        return AuthCredentials(scopes), AuthenticatedUser(user)

    async def validate_credential(self, auth: str) -> str:
        username = ""
        password = ""
        jwt = ""
        scheme = ""
        try:
            scheme, credentials = auth.split()
            if scheme.lower() == "basic":
                decoded = base64.b64decode(credentials).decode("ascii")
                username, _, password = decoded.partition(":")
            elif scheme.lower() == "bearer":
                jwt = credentials
            else:
                return ""
        except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
            raise AuthenticationError("Invalid auth credentials") from exc
        if scheme.lower() == "basic" and username:
            username = await self.authentication_service.authenticate_with_password(
                username, password
            )
        elif scheme.lower() == "bearer" and jwt:
            username = await self.authentication_service.validate_token(
                TokenType.JWT_TOKEN, jwt
            )

        return username
