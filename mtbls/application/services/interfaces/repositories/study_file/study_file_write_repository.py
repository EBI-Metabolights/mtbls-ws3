import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_read_repository import (  # noqa: E501
    StudyFileReadRepository,
)
from mtbls.domain.entities.study_file import StudyFileInput, StudyFileOutput
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput


class StudyFileRepository(
    AbstractWriteRepository[StudyFileInput, StudyFileOutput, str],
    StudyFileReadRepository,
    abc.ABC,
):
    @abc.abstractmethod
    async def create_objects(self, entities: list[StudyFileInput]) -> list[str]: ...

    @abc.abstractmethod
    async def update_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]: ...

    @abc.abstractmethod
    async def delete_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]: ...

    @abc.abstractmethod
    async def get_children(
        self, resource_id: str, bucket_name: str, parent_object_key: str
    ) -> PaginatedOutput[StudyFileOutput]: ...

    @abc.abstractmethod
    async def get_root_object(
        self, resource_id: str, bucket_name: str
    ) -> StudyFileOutput: ...
