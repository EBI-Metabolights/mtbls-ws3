import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)  # noqa: E501
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.shared.repository.entity_filter import EntityFilter


class StudyReadRepository(AbstractReadRepository[StudyOutput, int], abc.ABC):
    @abc.abstractmethod
    async def get_study_by_id(
        self,
        id_: int,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]: ...

    @abc.abstractmethod
    async def get_study_by_accession(
        self,
        accession: str,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]: ...

    @abc.abstractmethod
    async def get_study_accessions(
        self,
        filters: Union[None, list[EntityFilter]],
    ) -> list[str]: ...

    @abc.abstractmethod
    async def get_studies(
        self,
        filters: Union[None, list[EntityFilter]],
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> list[str]: ...

    @abc.abstractmethod
    async def get_study_by_obfuscation_code(
        self,
        obfuscation_code: str,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_username(self, username: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_email(self, email: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]: ...

    @abc.abstractmethod
    async def get_studies_by_user_id(self, id_: str) -> list[StudyOutput]: ...
