from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (
    AbstractWriteRepository,
)
from mtbls.domain.entities.user import UserInput, UserOutput


class UserWriteRepository(AbstractWriteRepository[UserInput, UserOutput, int]): ...
