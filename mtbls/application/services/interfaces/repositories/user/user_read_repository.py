import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.entities.user import UserOutput


class UserReadRepository(AbstractReadRepository[UserOutput, int]):
    @abc.abstractmethod
    async def get_user_by_id(
        self,
        id_: int,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_api_token(
        self,
        username: str,
        api_token: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_orcid(
        self,
        orcid: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_email(
        self,
        email: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_user_by_username(
        self,
        username: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]: ...

    @abc.abstractmethod
    async def get_study_submitters_by_accession(self, id_: int) -> list[UserOutput]: ...

    @abc.abstractmethod
    async def get_study_submitters_by_study_table_id(
        self, id_: int
    ) -> list[UserOutput]: ...
