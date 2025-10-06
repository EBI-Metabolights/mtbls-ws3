import logging
import pathlib
import re
import shutil

import httpx

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.domain.entities.study_file import (
    ResourceCategory,
    StudyFileOutput,
)
from mtbls.domain.exceptions.repository import (
    StudyBucketNotFoundError,
    StudyObjectAlreadyExistsError,
    StudyResourceError,
    UnaccessibleUriError,
    UnsupportedUriError,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.repositories.file_object.default.nfs.file_object_read_repository import (  # noqa: E501
    FileSystemObjectReadRepository,
)
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_manager import (  # noqa: E501
    StudyFolderManager,
)

logger = logging.getLogger(__name__)


class FileSystemObjectWriteRepository(
    FileSystemObjectReadRepository, FileObjectWriteRepository
):
    def __init__(
        self,
        folder_manager: StudyFolderManager,
        study_bucket: StudyBucket,
        observer: FileObjectObserver,
    ):
        super().__init__(
            folder_manager=folder_manager, study_bucket=study_bucket, observer=observer
        )

    async def put_object(
        self,
        resource_id: str,
        object_key: str,
        source_uri: str,
        override: bool = True,
    ) -> bool:
        source_uri = (
            f"file://{source_uri}" if source_uri.startswith("/") else source_uri
        )
        if not source_uri:
            raise StudyResourceError("source_uri is required")
        if not await self.exists(resource_id=resource_id):
            self.folder_manager.create_study_folder_path(
                resource_id=resource_id, bucket_name=self.study_bucket.value
            )
            # raise StudyBucketNotFoundError(resource_id, self.study_bucket.value)

        dest_path = self.folder_manager.get_study_folder_path(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
        )
        if not override and dest_path.exists():
            raise StudyObjectAlreadyExistsError(
                resource_id, self.study_bucket.value, object_key
            )
        file_path = self.folder_manager.get_study_folder_path(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
        )
        file_exists = file_path.exists()
        uploaded = False

        if source_uri.startswith("file://"):
            result = await self.put_with_local_file_provider(source_uri, dest_path)
            uploaded = True
        elif re.match(r"^https?://", source_uri):
            result = await self.put_with_http_file_provider(source_uri, dest_path)
            uploaded = True

        if uploaded:
            study_object = await self.get_study_object(
                resource_id=resource_id,
                bucket_name=self.study_bucket.value,
                object_key=object_key,
                dest_path=dest_path,
                resource_category=ResourceCategory.INTERNAL_RESOURCE,
            )
            if file_exists:
                await self.object_updated(study_object)
            else:
                await self.object_created(study_object)
            return result

        raise NotImplementedError(source_uri)

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

    async def create_folder_object(
        self,
        resource_id: str,
        object_key: str,
        exist_ok: bool = True,
    ) -> bool:
        if not await self.exists(resource_id=resource_id):
            raise StudyBucketNotFoundError(resource_id, self.study_bucket.value)

        dest_path = self.folder_manager.get_study_folder_path(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
        )
        if not exist_ok and dest_path.exists():
            raise StudyObjectAlreadyExistsError(
                resource_id, self.study_bucket.value, object_key
            )

        dest_path.mkdir(parents=True, exist_ok=exist_ok)
        study_object = await self.get_study_object(
            resource_id=resource_id,
            object_key=object_key,
            bucket_name=self.study_bucket.value,
            dest_path=dest_path,
        )
        await self.object_created(study_object)
        return True

    async def put_with_local_file_provider(
        self, source_uri: str, dest_path: pathlib.Path
    ) -> bool:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        source_path = await self._convert_uri_to_path(source_uri)
        return self._copy_file(source_path, dest_path)

    def _copy_file(self, source_path: pathlib.Path, dest_path: pathlib.Path):
        if source_path.is_file():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, dest_path)
        elif source_path.is_dir():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_path, dest_path)
        else:
            return False
        return True

    async def _convert_uri_to_path(self, uri: str) -> pathlib.Path:
        if not uri or not uri.startswith("file://"):
            raise UnsupportedUriError(uri)

        uri_path = pathlib.Path(uri.replace("file://", "", 1))
        if not uri_path.exists():
            raise UnaccessibleUriError(uri_path)
        return uri_path

    async def put_with_http_file_provider(
        self, source_uri: str, dest_path: pathlib.Path
    ) -> bool:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if not source_uri or not re.match(r"^https?://", source_uri):
            raise UnsupportedUriError(source_uri)

        with httpx.stream("GET", source_uri) as response:
            response.raise_for_status()

            with dest_path.open("wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
        return True

    async def delete_object(
        self,
        resource_id: str,
        object_key: str,
        ignore_not_exist: bool = True,
    ) -> bool:
        if not await self.exists(resource_id=resource_id):
            raise StudyBucketNotFoundError(resource_id, self.study_bucket.value)

        self.folder_manager.delete(
            resource_id=resource_id,
            bucket_name=self.study_bucket.value,
            object_key=object_key,
            ignore_not_exist=ignore_not_exist,
        )

        study_object = StudyFileOutput(
            bucket_name=self.study_bucket.value,
            resource_id=resource_id,
            object_key=object_key,
        )
        await self.object_deleted(study_object)
