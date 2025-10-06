import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput


class UserReadRepository(AbstractReadRepository[UserOutput, int]):
    @abc.abstractmethod
    async def get_studies_by_username(self, username: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_email(self, email: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_orcid(self, email: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_user_by_api_token(
        self, api_token: str
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_orcid(self, orcid: str) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_email(self, email: str) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_username(self, username: str) -> Union[None, UserOutput]: ...
