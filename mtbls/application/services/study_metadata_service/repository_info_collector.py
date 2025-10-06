from typing import List, Union

from metabolights_utils.models.common import GenericMessage
from metabolights_utils.models.metabolights.model import (
    StudyFileDescriptor,
    StudyFolderMetadata,
)
from metabolights_utils.provider.study_provider import AbstractFolderMetadataCollector

from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.domain.entities.study_file import StudyFileOutput
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.domain.shared.repository.study_bucket import StudyBucket


class RepositoryInfoCollector(AbstractFolderMetadataCollector):
    def __init__(
        self,
        resource_id: str,
        study_file_repository: StudyFileRepository,
    ):
        self.study_file_repository = study_file_repository
        self.resource_id = resource_id

    async def get_folder_metadata(
        self,
        study_path,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
    ) -> tuple[Union[StudyFolderMetadata, None], List[GenericMessage]]:
        study_files_metadata = StudyFolderMetadata()
        messages: List[GenericMessage] = []

        await self.collect_repository_info(
            resource_id=self.resource_id,
            bucket_name=StudyBucket.PRIVATE_METADATA_FILES.value,
            study_files_metadata=study_files_metadata,
            messages=messages,
        )
        await self.collect_repository_info(
            resource_id=self.resource_id,
            bucket_name=StudyBucket.PRIVATE_DATA_FILES.value,
            study_files_metadata=study_files_metadata,
            messages=messages,
            subfolder="FILES",
        )
        return study_files_metadata, messages

    def convert_to_file_descriptor(
        self, source: StudyFileOutput, subfolder: str = ""
    ) -> StudyFileDescriptor:
        parent_directory = (
            f"{subfolder}/{source.parent_object_key}"
            if subfolder
            else source.parent_object_key,
        )
        return StudyFileDescriptor(
            file_path=f"{subfolder}/{source.object_key}"
            if subfolder
            else source.object_key,
            created_at=source.created_at,
            updated_at=source.updated_at,
            is_directory=source.is_directory,
            parent_directory=parent_directory,
            is_link=source.is_link,
            mode=source.permission_in_oct,
            size_in_bytes=source.size_in_bytes,
            base_name=source.basename,
            file_extension=source.extension,
        )

    async def collect_repository_info(
        self,
        resource_id: str,
        bucket_name: str,
        study_files_metadata: StudyFolderMetadata,
        messages: List[GenericMessage],
        subfolder: str = "",
    ):
        content = await self.study_file_repository.find(
            query_options=QueryOptions(
                filters=[
                    EntityFilter(key="resourceId", value=resource_id),
                    EntityFilter(key="bucketName", value=bucket_name),
                ],
                sort_options=[SortOption(key="objectKey")],
            )
        )

        for item in content.data:
            descriptor = self.convert_to_file_descriptor(item, subfolder)
            if item.is_directory:
                study_files_metadata.folders[item.object_key] = descriptor
            else:
                study_files_metadata.files[item.object_key] = descriptor
