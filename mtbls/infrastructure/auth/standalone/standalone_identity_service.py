from mtbls.application.services.interfaces.auth.identity_service import IdentityService
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_write_repository import (  # noqa: E501
    UserWriteRepository,
)
from mtbls.domain.entities.user import IdentityOutput, UserInput, UserOutput


class DbIdentityService(IdentityService):
    def __init__(
        self,
        user_read_repository: UserReadRepository,
        user_write_repository: UserWriteRepository,
    ) -> None:
        self.user_read_repository: UserReadRepository = user_read_repository
        self.user_write_repository: UserWriteRepository = user_write_repository

    def create_identity(self, identity: UserInput) -> UserOutput: ...

    def delete_identity(self, id: int) -> bool: ...

    def update_identity(self, identity: UserOutput) -> UserOutput: ...

    def update_password(self, password: str) -> bool: ...

    def update_api_token(self, api_token: str) -> bool: ...

    def get_identity_by_email(self, email: str) -> IdentityOutput: ...

    def get_identity_by_username(self, username: str) -> IdentityOutput: ...

    def get_identity_by_api_token(self, api_token: str) -> IdentityOutput: ...

    def get_identity_by_orcid(self, orcid: str) -> IdentityOutput: ...

    def get_identity_by_id(self, id: int) -> IdentityOutput: ...
