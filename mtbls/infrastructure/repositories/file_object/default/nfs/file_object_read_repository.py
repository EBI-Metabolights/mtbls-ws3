import logging
import pathlib
import shutil
from datetime import datetime
from typing import Union

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (  # noqa: E501
    FileObjectReadRepository,
)
from mtbls.application.utils.size_utils import get_size_in_str
from mtbls.domain.entities.study_file import (
    ResourceCategory,
    StudyFileOutput,
)
from mtbls.domain.exceptions.repository import (
    StudyBucketNotFoundError,
    StudyObjectIsNotFolderError,
    StudyObjectNotFoundError,
    StudyResourceNotFoundError,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_manager import (  # noqa: E501
    StudyFolderManager,
)

logger = logging.getLogger(__name__)


class FileSystemObjectReadRepository(FileObjectReadRepository):
    def __init__(
        self,
        folder_manager: StudyFolderManager,
        study_bucket: StudyBucket,
        observer: FileObjectObserver,
    ):
        super().__init__(study_bucket=study_bucket, observers=[observer])
        self.folder_manager = folder_manager
        self.study_bucket = study_bucket

    def get_bucket(self) -> StudyBucket:
        return self.study_bucket

    async def list(
        self, resource_id: str, object_key: Union[None, str] = None
    ) -> list[StudyFileOutput]:
        resources = []

        study_path, directory_path = await self._get_directory_path(
            resource_id, object_key
        )
        directory_path: pathlib.Path = directory_path
        for object_path in directory_path.iterdir():
            if await self._does_path_exist(object_path):
                object_key = (
                    str(object_path).replace(str(study_path), "", 1).lstrip("/")
                )
                resource = await self.get_study_object(
                    resource_id=resource_id,
                    bucket_name=self.study_bucket.value,
                    object_key=object_key,
                    dest_path=object_path,
                )

                resources.append(resource)

        return resources

    async def exists(
        self,
        resource_id: str,
        object_key: Union[None, str] = None,
    ) -> bool:
        if not resource_id:
            return False
        try:
            _, object_path = await self._get_object_path(resource_id, object_key)
            return await self._does_path_exist(object_path)
        except (
            StudyResourceNotFoundError,
            StudyBucketNotFoundError,
            StudyObjectNotFoundError,
        ):
            return False

    async def get_info(self, resource_id: str, object_key: str) -> StudyFileOutput:
        _, object_path = await self._get_object_path(resource_id, object_key)
        return await self.get_study_object(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
            dest_path=object_path,
        )

    async def get_uri(self, resource_id: str, object_key: str) -> str:
        _, object_path = await self._get_object_path(
            resource_id, self.study_bucket.value, object_key
        )
        return f"file://{str(object_path)}"

    async def download(
        self, resource_id: str, object_key: str, target_path: str
    ) -> StudyFileOutput:
        _, object_path = await self._get_object_path(resource_id, object_key)
        shutil.copy(object_path, target_path)

    async def _does_path_exist(self, object_path: pathlib.Path) -> bool:
        if not object_path:
            return False
        if object_path.is_symlink():
            if not object_path.exists():
                logger.warning(
                    "Invalid symlink %s It points to a non-existent file: %s",
                    str(object_path),
                    object_path.resolve(),
                )
                return False
        if object_path.exists():
            return True
        return False

    async def _get_resource_metadata(
        self,
        study_path: pathlib.Path,
        object_path: pathlib.Path,
        resource_id: str,
        max_suffix_length: int = 6,
    ) -> StudyFileOutput:
        # Get file or directory metadata
        object_key = str(object_path).replace(str(study_path), "", 1).lstrip("/")
        return self.get_study_object(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
            dest_path=object_path,
            max_suffix_length=max_suffix_length,
        )

    async def get_study_object(
        self,
        resource_id: str,
        bucket_name: str,
        object_key: str,
        dest_path: pathlib.Path,
        resource_category: ResourceCategory = ResourceCategory.UNKNOWN_RESOURCE,
        tags: dict = None,
        max_suffix_length: int = 6,
    ) -> Union[None, StudyFileOutput]:
        object_path = dest_path
        if not object_path.exists():
            logger.warning("File does not exist: %s", object_path)
            return None
        # Get file or directory metadata
        parent_object_key = ""
        if object_key:
            parent_object_key = str(pathlib.Path(object_key).parent)
            if parent_object_key == ".":
                parent_object_key = ""

        stat = object_path.stat()
        is_symlink = object_path.is_symlink()
        created_at = datetime.fromtimestamp(stat.st_ctime)
        updated_at = datetime.fromtimestamp(stat.st_mtime)
        size_in_bytes = stat.st_size if object_path.is_file() else None
        size_in_str = (
            get_size_in_str(size_in_bytes) if size_in_bytes is not None else ""
        )
        is_directory = object_path.is_dir()
        suffix = ""
        if not is_directory:
            suffixes = [x for x in object_path.suffixes if len(x) <= max_suffix_length]
            if suffixes and len(suffixes) == object_path.suffixes:
                suffix = "".join(suffixes)
            else:
                suffix = object_path.suffix if object_path.suffix else ""

        permission_in_oct = oct(stat.st_mode & 0o777)
        numeric_resource_id = int(resource_id.removeprefix("MTBLS").removeprefix("REQ"))
        return StudyFileOutput(
            bucket_name=bucket_name,
            resource_id=resource_id,
            numeric_resource_id=numeric_resource_id,
            object_key=object_key,
            parent_object_key=parent_object_key,
            created_at=created_at,
            updated_at=updated_at,
            size_in_bytes=size_in_bytes,
            is_directory=is_directory,
            is_link=is_symlink,
            size_in_str=size_in_str,
            permission_in_oct=permission_in_oct,
            basename=object_path.name,
            extension=suffix,
            category=resource_category,
            tags=tags if tags else {},
        )

    async def _get_directory_path(
        self, resource_id: str, object_key: Union[None, str]
    ) -> tuple[pathlib.Path, pathlib.Path]:
        study_path, object_path = await self._get_object_path(resource_id, object_key)
        if not object_path.is_dir():
            logger.warning("Study object does not exist: %s", object_path)
            raise StudyObjectIsNotFolderError(
                resource_id, self.study_bucket.value, object_key
            )
        return study_path, object_path

    async def _get_object_path(
        self, resource_id: str, object_key: Union[None, str]
    ) -> tuple[pathlib.Path, pathlib.Path]:
        study_path = self.folder_manager.get_study_folder_path(
            resource_id, self.study_bucket.value
        )

        if not study_path.exists():
            logger.warning("Study path does not exist: %s", study_path)
            raise StudyResourceNotFoundError(resource_id)

        object_path = self.folder_manager.get_study_folder_path(
            resource_id, self.study_bucket.value, object_key
        )

        if not object_path.exists():
            logger.warning("Study path does not exist: %s", object_path)
            raise StudyObjectNotFoundError(
                resource_id, self.study_bucket.value, object_key
            )
        return study_path, object_path
