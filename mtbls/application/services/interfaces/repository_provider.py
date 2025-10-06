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


class RepositoryProvider:
    @abc.abstractmethod
    async def get_study_file_repository(self) -> StudyFileRepository: ...

    @abc.abstractmethod
    async def get_study_repository(self) -> StudyWriteRepository: ...

    @abc.abstractmethod
    async def get_user_repository(self) -> UserWriteRepository: ...

    @abc.abstractmethod
    async def get_validation_override_repository(
        self,
    ) -> ValidationOverrideRepository: ...

    @abc.abstractmethod
    async def get_validation_report_repository(
        self,
    ) -> ValidationReportRepository: ...

    @abc.abstractmethod
    async def get_investigation_object_repository(
        self,
    ) -> InvestigationObjectRepository: ...

    @abc.abstractmethod
    async def get_isa_table_object_repository(self) -> IsaTableObjectRepository: ...

    @abc.abstractmethod
    async def get_isa_table_row_object_repository(
        self,
    ) -> IsaTableRowObjectRepository: ...

    @abc.abstractmethod
    async def get_file_object_repository(
        self, bucket_name: str
    ) -> FileObjectWriteRepository: ...
