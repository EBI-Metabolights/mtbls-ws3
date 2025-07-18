import abc

from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (
    FileObjectReadRepository,
)


class FileObjectWriteRepository(FileObjectReadRepository, abc.ABC):
    @abc.abstractmethod
    async def put_object(
        self,
        resource_id: str,
        object_key: str,
        source_uri: str,
        override: bool = True,
    ) -> bool: ...

    @abc.abstractmethod
    async def create_folder_object(
        self,
        resource_id: str,
        object_key: str,
        exist_ok: bool = True,
    ) -> bool: ...

    @abc.abstractmethod
    async def delete_object(
        self,
        resource_id: str,
        object_key: str,
        ignore_not_exist: bool = True,
    ) -> bool: ...
