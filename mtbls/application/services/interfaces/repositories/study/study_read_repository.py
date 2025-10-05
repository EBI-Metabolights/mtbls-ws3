import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)  # noqa: E501
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput


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
    async def get_study_submitters_by_id(self, id_: int) -> list[UserOutput]: ...

    @abc.abstractmethod
    async def get_study_submitters_by_accession(self, id_: int) -> list[UserOutput]: ...
