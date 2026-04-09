import abc
from typing import Any

from mtbls.domain.entities.user import UserProfile
from mtbls.domain.enums.token_type import TokenType


class UserProfileService(abc.ABC):
    @abc.abstractmethod
    async def get_user_profile(self, username: str = None) -> None | UserProfile:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get_users_by_query(self, key_value: dict[str, Any]) -> list[UserProfile]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get_all_users(
        self, page_size: int = 1000, fetch_roles: bool = False
    ) -> list[UserProfile]:
        raise NotImplementedError()


class AuthenticationService(abc.ABC):
    @abc.abstractmethod
    async def authenticate_with_token(
        self, token_type: TokenType, token: str, username: str
    ) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def authenticate_with_password(
        self, username: str, password: str
    ) -> tuple[str, None | str]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def revoke_jwt_token(self, refresh_jwt_token: str) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def validate_token(
        self, token_type: TokenType, token: str, username: str = None
    ) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def refresh(self, refresh_token: str) -> tuple[str, None | str]: ...
