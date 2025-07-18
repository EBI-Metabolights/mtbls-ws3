import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.file_object.base import (
    BaseFileObjectRepository,
)
from mtbls.domain.entities.study_file import StudyFileOutput
from mtbls.domain.shared.repository.study_bucket import StudyBucket


class FileObjectReadRepository(BaseFileObjectRepository, abc.ABC):
    @abc.abstractmethod
    def get_bucket(self) -> StudyBucket: ...

    @abc.abstractmethod
    async def list(
        self,
        resource_id: str,
        object_key: Union[None, str] = None,
    ) -> list[StudyFileOutput]: ...

    @abc.abstractmethod
    async def exists(
        self,
        resource_id: str,
        object_key: Union[None, str],
    ) -> bool: ...

    @abc.abstractmethod
    async def get_info(self, resource_id: str, object_key: str) -> StudyFileOutput: ...

    @abc.abstractmethod
    async def get_uri(
        self,
        resource_id: str,
        object_key: Union[None, str],
    ) -> str: ...

    @abc.abstractmethod
    async def download(
        self,
        resource_id: str,
        object_key: Union[None, str],
        target_path: str,
    ) -> str: ...
