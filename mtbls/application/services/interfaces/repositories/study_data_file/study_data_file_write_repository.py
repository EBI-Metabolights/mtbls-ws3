import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study_data_file.study_data_file_read_repository import (  # noqa: E501
    StudyFileReadRepository,
)
from mtbls.domain.entities.study_file import StudyDataFileInput, StudyDataFileOutput
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput


class StudyDataFileRepository(
    AbstractWriteRepository[StudyDataFileInput, StudyDataFileOutput, str],
    StudyFileReadRepository,
    abc.ABC,
):
    @abc.abstractmethod
    async def create_objects(self, entities: list[StudyDataFileInput]) -> list[str]: ...

    @abc.abstractmethod
    async def update_object(
        self, entity: StudyDataFileOutput
    ) -> Union[None, StudyDataFileOutput]: ...

    @abc.abstractmethod
    async def delete_object(
        self, entity: StudyDataFileOutput
    ) -> Union[None, StudyDataFileOutput]: ...

    @abc.abstractmethod
    async def get_children(
        self, resource_id: str, bucket_name: str, parent_object_key: str
    ) -> PaginatedOutput[StudyDataFileOutput]: ...

    @abc.abstractmethod
    async def get_root_object(
        self, resource_id: str, bucket_name: str
    ) -> StudyDataFileOutput: ...
