import datetime
import logging
from typing import OrderedDict, Union

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    DefaultFileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.domain.entities.study_file import StudyFileInput, StudyFileOutput
from mtbls.domain.enums.entity import Entity
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model import study_models as db_models
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.default.db.default_read_repository import (
    SqlDbDefaultReadRepository,
)
from mtbls.infrastructure.repositories.default.db.default_write_repository import (
    SqlDbDefaultWriteRepository,
)
from mtbls.infrastructure.repositories.study_file.sql_db.models import (
    SqlDbStudyFileInput,
    SqlDbStudyFileOutput,
)

logger = logging.getLogger(__name__)


class SqlDbStudyFileRepository(
    SqlDbDefaultWriteRepository[SqlDbStudyFileInput, SqlDbStudyFileOutput, int],
    SqlDbDefaultReadRepository[SqlDbStudyFileOutput, int],
    StudyFileRepository,
    DefaultFileObjectObserver,
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)

        self.registered_repositories = set()
        self.user_type_alias_dict = OrderedDict()
        for field in SqlDbStudyFileOutput.model_fields:
            entity_type: Entity = SqlDbStudyFileOutput.model_config.get("entity_type")
            value = alias_generator.get_alias(entity_type, field)
            self.user_type_alias_dict[field] = value

    async def object_updated(self, study_object: StudyFileOutput):
        await self.update_object(study_object)

    async def object_deleted(self, study_object: StudyFileOutput):
        await self.delete_object(study_object)

    async def object_created(self, study_object: StudyFileInput):
        await self.update_object(study_object)

    async def get_children(
        self, resource_id: str, bucket_name: str, parent_object_key: str
    ) -> PaginatedOutput[StudyFileOutput]:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="parent_object_key",
                    value=parent_object_key,
                ),
                EntityFilter(
                    key="resource_id",
                    value=resource_id,
                ),
                EntityFilter(
                    key="bucket_name",
                    value=bucket_name,
                ),
            ],
            sort_options=[SortOption(key="object_key")],
        )
        result = await self.find(query_options=query_options)
        updated_data = [
            SqlDbStudyFileOutput.model_validate(x, from_attributes=True)
            for x in result.data
        ]
        return PaginatedOutput(data=updated_data)

    async def get_root_object(
        self, resource_id: str, bucket_name: str
    ) -> StudyFileOutput:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="object_key",
                    value="",
                ),
                EntityFilter(
                    key="resource_id",
                    value=resource_id,
                ),
                EntityFilter(
                    key="bucket_name",
                    value=bucket_name,
                ),
            ],
            sort_options=SortOption(key="object_key"),
        )
        result = await self.find(query_options=query_options)
        updated_data = [
            SqlDbStudyFileOutput.model_validate(x, from_attributes=True)
            for x in result.data
        ]
        if updated_data:
            return updated_data[0]

    async def create_objects(self, entities: list[StudyFileInput]) -> list[str]:
        db_entities = []
        for entity in entities:
            value = SqlDbStudyFileInput.model_validate(entity, from_attributes=True)
            values = value.model_dump(by_alias=False)
            db_entities.append(db_models.StudyFile(**values))
        results = await self.create_many(db_entities)
        return [str(x.id_ for x in results)]

    async def update_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]:
        value = SqlDbStudyFileOutput.model_validate(entity, from_attributes=True)
        # values = value.model_dump(by_alias=False)
        # db_entity = db_models.StudyFile(**values)
        current = await self.find_one(
            resource_id=entity.resource_id,
            bucket_name=entity.bucket_name,
            object_key=entity.object_key,
        )
        if current:
            value.id_ = current.id_
            result = await self.update(value)
        else:
            value.id_ = None
            value.created_at = datetime.datetime.now(datetime.timezone.utc)
            result = await self.create(value)
        return SqlDbStudyFileOutput.model_validate(result, from_attributes=True)

    async def delete_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]:
        await self.delete(entity.id_)

    async def find_one(self, resource_id: str, bucket_name: str, object_key: str):
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="object_key",
                    value=object_key,
                ),
                EntityFilter(
                    key="resource_id",
                    value=resource_id,
                ),
                EntityFilter(
                    key="bucket_name",
                    value=bucket_name,
                ),
            ],
            sort_options=[SortOption(key="object_key")],
        )
        result = await self.find(query_options=query_options)

        updated_data = [
            SqlDbStudyFileOutput.model_validate(x, from_attributes=True)
            for x in result.data
        ]
        if updated_data:
            return updated_data[0]
        return None
