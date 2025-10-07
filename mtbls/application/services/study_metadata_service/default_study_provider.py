import datetime
import enum
import json
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Union

from metabolights_utils.models.common import (
    GenericMessage,
)
from metabolights_utils.models.metabolights.model import (
    StudyFileDescriptor,
    StudyFolderMetadata,
)
from metabolights_utils.provider.async_provider.study_provider import (
    AbstractFolderMetadataCollector,
    AsyncMetabolightsStudyProvider,
)
from metabolights_utils.provider.local_folder_metadata_collector import (
    LocalFolderMetadataCollector,
)
from pydantic import BaseModel

from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (  # noqa: E501
    FileObjectReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.study_metadata_service.db_metadata_collector import (
    DefaultAsyncDbMetadataCollector,
)

logger = logging.getLogger(__name__)


class AsyncLocalFolderMetadataCollector(AbstractFolderMetadataCollector):
    def __init__(self):
        self.collector = LocalFolderMetadataCollector()

    async def get_folder_metadata(
        self,
        study_path,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
    ) -> tuple[Union[None, StudyFolderMetadata], list[GenericMessage]]:
        return self.collector.get_folder_metadata(
            study_path=study_path,
            calculate_data_folder_size=calculate_data_folder_size,
            calculate_metadata_size=calculate_metadata_size,
        )


class DefaultMetabolightsStudyProvider(AsyncMetabolightsStudyProvider):
    def __init__(self, study_read_repository: StudyReadRepository) -> None:
        super().__init__(
            db_metadata_collector=DefaultAsyncDbMetadataCollector(
                study_read_repository=study_read_repository
            ),
            folder_metadata_collector=AsyncLocalFolderMetadataCollector(),
        )


class FileDifference(enum.StrEnum):
    NEW = "NEW"
    DELETED = "DELETED"
    MODIFIED = "MODIFIED"


class FileDescriptor(BaseModel):
    file_difference: Union[None, FileDifference] = None
    name: str = ""
    parent_relative_path: str = ""
    relative_path: str = ""
    modified_time: float = 0
    is_dir: bool = False
    extension: str = ""
    is_stop_folder: bool = False
    sub_filename: str = ""
    is_empty: bool = False
    file_size: int = 0


class DataFileIndexMetadataCollector(AbstractFolderMetadataCollector):
    def __init__(
        self,
        resource_id: str,
        internal_files_object_repository: FileObjectReadRepository,
        data_file_index_file_key: str,
    ):
        self.resource_id = resource_id
        self.internal_files_object_repository = internal_files_object_repository
        self.data_file_index_file_key = data_file_index_file_key

    async def load_from_data_index_file(self) -> Dict[str, StudyFileDescriptor]:
        metadata: Dict[str, StudyFileDescriptor] = {}
        current = int(datetime.datetime.now().timestamp())
        temp_file_path = Path(
            f"/tmp/data_files/{self.resource_id}/{current}_" + str(uuid.uuid4())
        )
        file_content = {}
        try:
            temp_file_path.parent.mkdir(parents=True, exist_ok=True)
            exist = await self.internal_files_object_repository.exists(
                self.resource_id, object_key=self.data_file_index_file_key
            )
            if exist:
                await self.internal_files_object_repository.download(
                    self.resource_id,
                    object_key=self.data_file_index_file_key,
                    target_path=str(temp_file_path),
                )

                with temp_file_path.open() as f:
                    file_content = json.load(f)
            else:
                logger.error(
                    "%s %s does not exist on %s bucket.",
                    self.resource_id,
                    self.data_file_index_file_key,
                    self.internal_files_object_repository.get_bucket().name,
                )
        except Exception as ex:
            logger.error(str(ex))
            raise ex
        finally:
            if temp_file_path.exists():
                temp_file_path.unlink()

        private_data_files = file_content.get("private_data_files")
        public_data_files = file_content.get("public_data_files")

        data_files = {}

        if public_data_files:
            data_files.update({x: public_data_files[x] for x in public_data_files})

        if private_data_files:
            data_files.update({x: private_data_files[x] for x in private_data_files})

        for relative_path, item in data_files.items():
            file_descriptor = FileDescriptor.model_validate(item)
            descriptor = StudyFileDescriptor(
                file_path=relative_path,
                base_name=file_descriptor.name,
                parent_directory=file_descriptor.parent_relative_path,
                extension=file_descriptor.extension,
                is_directory=file_descriptor.is_dir,
                modified_at=int(file_descriptor.modified_time),
                size_in_bytes=file_descriptor.file_size,
            )
            metadata[relative_path] = descriptor
        return metadata

    async def get_folder_metadata(
        self,
        study_path,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
    ) -> Tuple[Union[None, StudyFolderMetadata], List[GenericMessage]]:
        messages: List[GenericMessage] = []
        study_folder_metadata = StudyFolderMetadata()
        data_files: Dict[
            str, StudyFileDescriptor
        ] = await self.load_from_data_index_file()

        data_folder_size = 0
        for relative_path, item in data_files.items():
            if item.is_directory:
                study_folder_metadata.folders[relative_path] = item
            else:
                study_folder_metadata.files[relative_path] = item
                data_folder_size = item.size_in_bytes if item.size_in_bytes else 0

        study_folder_metadata.folder_size_in_bytes = data_folder_size

        return study_folder_metadata, messages


class DataFileIndexMetabolightsStudyProvider(AsyncMetabolightsStudyProvider):
    def __init__(
        self,
        resource_id: str,
        study_read_repository: StudyReadRepository,
        user_read_repository: UserReadRepository,
        internal_files_object_repository: FileObjectReadRepository,
        data_file_index_file_key: str,
    ) -> None:
        super().__init__(
            db_metadata_collector=DefaultAsyncDbMetadataCollector(
                study_read_repository=study_read_repository,
                user_read_repository=user_read_repository,
            ),
            folder_metadata_collector=DataFileIndexMetadataCollector(
                resource_id, internal_files_object_repository, data_file_index_file_key
            ),
        )
