import abc

from mtbls.domain.enums.token_type import TokenType


class AuthenticationService(abc.ABC):
    @abc.abstractmethod
    async def authenticate_with_token(
        self, token_type: TokenType, token: str, username: str
    ) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def authenticate_with_password(self, username: str, password: str) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def revoke_jwt_token(self, refresh_jwt_token: str) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def validate_token(
        self, token_type: TokenType, token: str, username: str = None
    ) -> str:
        raise NotImplementedError()
