import uuid
from pathlib import Path
from typing import Union

from metabolights_utils.provider.study_provider import AbstractMetadataFileProvider

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (
    FileObjectWriteRepository,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket


class RepositoryStudyMetadataFileProvider(AbstractMetadataFileProvider):
    def __init__(
        self,
        resource_id: str,
        metadata_files_object_repository: FileObjectWriteRepository,
        download_path: Union[None, Path] = None,
    ):
        self.metadata_files_object_repository = metadata_files_object_repository
        self.resource_id = resource_id
        self.download_path = download_path
        if not self.download_path:
            task_name = f"file-provider-{str(uuid.uuid4())}"
            self.download_path = Path("/tmp") / Path(task_name) / Path(self.resource_id)
        self.download_path.mkdir(parents=True, exist_ok=True)

    async def get_study_metadata_path(
        self, resource_id: str, file_relative_path: Union[None, str] = None
    ) -> str:
        if self.resource_id != resource_id:
            raise ValueError(f"{resource_id} is not valid. Expected {self.resource_id}")

        if file_relative_path is None:
            target_file_path = str(self.download_path)
        else:
            downloaded_file_path = self.download_path / Path(file_relative_path)
            if not downloaded_file_path.exists():
                await self.metadata_files_object_repository.download(
                    resource_id=self.resource_id,
                    bucket_name=StudyBucket.PRIVATE_METADATA_FILES.value,
                    object_key=file_relative_path,
                    target_path=str(downloaded_file_path),
                )
            target_file_path = str(downloaded_file_path)
        return target_file_path

    async def exists(
        self, resource_id: str, file_relative_path: Union[None, str] = None
    ) -> bool:
        file_path = Path(
            await self.get_study_metadata_path(resource_id, file_relative_path)
        )
        return file_path.resolve().exists()
