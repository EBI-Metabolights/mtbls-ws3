import abc

from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)


class StudyMetadataServiceFactory(abc.ABC):
    async def create_service(
        self,
        resource_id: str,
    ) -> StudyMetadataService: ...
