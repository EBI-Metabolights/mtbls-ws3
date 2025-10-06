import abc

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.study_metadata.metadata_repository import (  # noqa: E501
    InvestigationObjectRepository,
    IsaTableObjectRepository,
    IsaTableRowObjectRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.validation.validation_override_repository import (  # noqa: E501
    ValidationOverrideRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.validation.validation_report_repository import (  # noqa: E501
    ValidationReportRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_write_repository import (  # noqa: E501
    StudyWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_write_repository import (  # noqa: E501
    UserWriteRepository,
)
from mtbls.application.services.interfaces.repository_provider import RepositoryProvider


class DefaultRepositoryProvider(RepositoryProvider):
    def __init__(
        self,
        study_file_repository: StudyFileRepository,
        study_repository: StudyWriteRepository,
        user_repository: UserWriteRepository,
        validation_override_repository: ValidationOverrideRepository,
        validation_report_repository: ValidationReportRepository,
        investigation_object_repository: InvestigationObjectRepository,
        isa_table_object_repository: IsaTableObjectRepository,
        isa_table_row_object_repository: IsaTableObjectRepository,
        file_object_repositories: dict[str, FileObjectWriteRepository],
    ):
        self.study_file_repository = study_file_repository
        self.study_repository = study_repository
        self.user_repository = user_repository
        self.validation_override_repository = validation_override_repository
        self.validation_report_repository = validation_report_repository
        self.investigation_object_repository = investigation_object_repository
        self.isa_table_object_repository = isa_table_object_repository
        self.isa_table_row_object_repository = isa_table_row_object_repository
        self.file_object_repositories = file_object_repositories

    async def get_study_file_repository(self) -> StudyFileRepository:
        return self.study_file_repository

    async def get_study_repository(self) -> StudyWriteRepository:
        return self.study_repository

    async def get_user_repository(self) -> UserWriteRepository:
        return self.user_repository

    async def get_validation_override_repository(
        self,
    ) -> ValidationOverrideRepository:
        return self.validation_override_repository

    @abc.abstractmethod
    async def get_validation_report_repository(self) -> ValidationReportRepository:
        return self.validation_report_repository

    @abc.abstractmethod
    async def get_investigation_object_repository(
        self,
    ) -> InvestigationObjectRepository:
        return self.investigation_object_repository

    @abc.abstractmethod
    async def get_isa_table_object_repository(self) -> IsaTableObjectRepository:
        return self.isa_table_object_repository

    @abc.abstractmethod
    async def get_isa_table_row_object_repository(self) -> IsaTableRowObjectRepository:
        return self.isa_table_row_object_repository

    @abc.abstractmethod
    async def get_file_object_repository(
        self, bucket_name: str
    ) -> FileObjectWriteRepository:
        if bucket_name not in self.file_object_repositories:
            raise NotImplementedError(bucket_name, "not implemented")
        return self.file_object_repositories[bucket_name]
