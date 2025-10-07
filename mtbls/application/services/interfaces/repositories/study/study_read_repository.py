import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)  # noqa: E501
from mtbls.domain.entities.study import StudyOutput


class StudyReadRepository(AbstractReadRepository[StudyOutput, int], abc.ABC):
    @abc.abstractmethod
    async def get_study_by_accession(
        self, accession: str
    ) -> Union[None, StudyOutput]: ...

    @abc.abstractmethod
    async def get_study_by_obfuscation_code(
        self, obfuscation_code: str
    ) -> Union[None, StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_username(self, username: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_email(self, email: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]: ...
