from typing import Union

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service import (
    FileObjectStudyMetadataService,
)


class FileObjectStudyMetadataServiceFactory(StudyMetadataServiceFactory):
    def __init__(
        self,
        study_file_repository: StudyFileRepository,
        metadata_files_object_repository: FileObjectWriteRepository,
        audit_files_object_repository: FileObjectWriteRepository,
        internal_files_object_repository: FileObjectWriteRepository,
        study_read_repository: StudyReadRepository,
        user_read_repository: UserReadRepository,
        temp_path: Union[None, str] = None,
    ):
        self.user_read_repository = user_read_repository
        self.internal_files_object_repository = internal_files_object_repository
        self.study_file_repository = study_file_repository
        self.metadata_files_object_repository = metadata_files_object_repository
        self.audit_files_object_repository = audit_files_object_repository
        self.study_read_repository = study_read_repository
        self.temp_path = temp_path

    async def create_service(
        self,
        resource_id: str,
    ) -> StudyMetadataService:
        return FileObjectStudyMetadataService(
            resource_id=resource_id,
            study_file_repository=self.study_file_repository,
            metadata_files_object_repository=self.metadata_files_object_repository,
            audit_files_object_repository=self.audit_files_object_repository,
            internal_files_object_repository=self.internal_files_object_repository,
            study_read_repository=self.study_read_repository,
            user_read_repository=self.user_read_repository,
            temp_path=self.temp_path,
        )
