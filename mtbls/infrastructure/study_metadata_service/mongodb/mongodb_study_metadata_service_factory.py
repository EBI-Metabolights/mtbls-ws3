from typing import Union

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.investigation_file_repository import (  # noqa: E501
    MongoDbInvestigationObjectRepository,
)
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.isa_table_file_repository import (  # noqa: E501
    MongoDbIsaTableObjectRepository,
)
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.isa_table_file_row_repository import (  # noqa: E501
    MongoDbIsaTableRowObjectRepository,
)
from mtbls.infrastructure.repositories.study_file.mongodb.study_file_repository import (
    MongoDbStudyFileRepository,
)
from mtbls.infrastructure.study_metadata_service.mongodb.mongodb_study_metadata_service import (  # noqa: E501
    MongoDbStudyMetadataService,
)


class MongoDbStudyMetadataServiceFactory(StudyMetadataServiceFactory):
    def __init__(
        self,
        study_read_repository: StudyReadRepository,
        study_file_repository: MongoDbStudyFileRepository,
        investigation_object_repository: MongoDbInvestigationObjectRepository,
        isa_table_object_repository: MongoDbIsaTableObjectRepository,
        isa_table_row_object_repository: MongoDbIsaTableRowObjectRepository,
        temp_path: Union[None, str] = None,
    ):
        self.study_read_repository = study_read_repository
        self.study_file_repository = study_file_repository
        self.investigation_object_repository = investigation_object_repository
        self.isa_table_object_repository = isa_table_object_repository
        self.isa_table_row_object_repository = isa_table_row_object_repository
        self.temp_path = temp_path

    async def create_service(
        self,
        resource_id: str,
    ) -> StudyMetadataService:
        return MongoDbStudyMetadataService(
            resource_id=resource_id,
            study_read_repository=self.study_read_repository,
            study_file_repository=self.study_file_repository,
            investigation_object_repository=self.investigation_object_repository,
            isa_table_object_repository=self.isa_table_object_repository,
            isa_table_row_object_repository=self.isa_table_row_object_repository,
            temp_path=self.temp_path,
        )
