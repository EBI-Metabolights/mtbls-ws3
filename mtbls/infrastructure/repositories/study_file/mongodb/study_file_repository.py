import datetime
import logging
from pathlib import Path
from typing import Any, Union

from bson import ObjectId
from pymongo.results import DeleteResult, UpdateResult

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    DefaultFileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.study_file import (
    ResourceCategory,
    StudyFileInput,
    StudyFileOutput,
)
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.sort_order import SortOrder
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryFieldOptions, QueryOptions
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class MongoDbStudyFileRepository(
    MongoDbDefaultWriteRepository, StudyFileRepository, DefaultFileObjectObserver
):
    def __init__(
        self,
        connection: MongoDbConnection,
        study_objects_collection_name: str = "study_objects",
        output_entity_class: type[BaseEntity] = StudyFileOutput,
    ):
        super(MongoDbDefaultWriteRepository, self).__init__(
            connection=connection,
            collection_name=study_objects_collection_name,
            output_entity_class=output_entity_class,
        )
        super(DefaultFileObjectObserver, self).__init__()

    async def object_updated(self, study_object: StudyFileOutput):
        await self.update_object(study_object)

    async def object_deleted(self, study_object: StudyFileOutput):
        await self.delete_object(study_object)

    async def object_created(self, study_object: StudyFileInput):
        parent = study_object.parent_object_key
        result = await self.find(
            query_options=QueryFieldOptions(
                filters=[
                    EntityFilter(key="objectKey", value=parent),
                    EntityFilter(key="resourceId", value=study_object.resource_id),
                    EntityFilter(key="bucketName", value=study_object.bucket_name),
                ]
            )
        )
        if not result or result.data:
            object_path = Path(study_object.object_key)
            parent_of_parent = Path(object_path).parent.parent
            if parent_of_parent == ".":
                parent_of_parent = ""
            now = datetime.datetime.now(datetime.timezone.utc)
            parent_object = StudyFileInput(
                bucket_name=study_object.bucket_name,
                resource_id=study_object.resource_id,
                object_key=study_object.parent_object_key,
                parent_object_key=parent_of_parent,
                basename=object_path.parent.name,
                numeric_resource_id=study_object.numeric_resource_id,
                created_at=now,
                category=ResourceCategory.FOLDER_RESOURCE,
                is_directory=True,
            )
            await self.object_created(parent_object)

        await self.create(study_object)

    def convert_to_mongo_filter(self, filter: EntityFilter) -> dict[str, Any]:
        if filter.operand in (
            FilterOperand.EQ,
            FilterOperand.GE,
            FilterOperand.LE,
            FilterOperand.GT,
            FilterOperand.LT,
        ):
            return {filter.key: {f"${filter.operand.name.lower()}": filter.value}}
        if filter.operand == FilterOperand.LIKE:
            return {filter.key: {"$regex": filter.value, "$options": "i"}}
        if filter.operand == FilterOperand.LE:
            return {filter.key: {"$in": filter.value}}

    async def get_by_id(self, id_: str) -> StudyFileOutput:
        filter = {"_id": id_}
        result = self.collection.find_one(filter)
        if result:
            return StudyFileOutput.model_validate(result)
        return None

    async def get_ids(
        self,
        filters: Union[None, list[EntityFilter]],
    ) -> list[str]:
        if not filters:
            return self.collection.find({}, {"_id": 1})
        filter = {}
        for item in filters:
            filter.update(self.convert_to_mongo_filter(item))
        result = self.collection.find(filter, {"_id": 1})
        return [x["_id"] for x in result]

    async def find(
        self,
        query_options: QueryOptions,
    ) -> PaginatedOutput[StudyFileOutput]:
        filter = {}
        for item in query_options.filters:
            filter.update(self.convert_to_mongo_filter(item))
        result = self.collection.find(filter)
        if query_options.sort_options:
            sort_options = []
            for item in query_options.sort_options:
                sort_options.append(
                    {item.key, 1 if item.order == SortOrder.ASC else -1}
                )
            if sort_options:
                result = result.sort(sort_options)
        offset = query_options.offset if query_options.offset else 0
        result = result.skip(offset)

        if query_options.limit is not None:
            result = result.limit(query_options.limit)
        items = [StudyFileOutput.model_validate(x) for x in result]
        return PaginatedOutput(offset=offset, size=len(items), data=items)

    async def select_fields(
        self, query_field_options: QueryFieldOptions
    ) -> PaginatedOutput[tuple[Any, ...]]:
        options = query_field_options
        filter = {}
        for item in options.filters:
            filter.update(self.convert_to_mongo_filter(item))
        if query_field_options.selected_fields:
            fields = {x: 1 for x in query_field_options.selected_fields}
            result = self.collection.find(filter, fields)
        else:
            result = self.collection.find(filter)
        sort_options = []
        for item in options.sort_options:
            sort_options.append({item.key, 1 if item.order == SortOrder.ASC else -1})
        if sort_options:
            result = result.sort(sort_options)
        offset = options.offset if options.offset else 0
        result = result.skip(offset)
        if options.limit is not None:
            result = result.limit(options.limit)
        items = [StudyFileOutput.model_validate(x) for x in result]
        return PaginatedOutput(offset=offset, size=len(items), data=items)

    async def select_field(
        self, field_name, query_options: QueryOptions
    ) -> PaginatedOutput[tuple[Any, ...]]:
        query_field_options = QueryFieldOptions.model_validate(query_options)
        query_field_options.selected_fields = [field_name]
        return await self.select_fields(query_field_options=query_field_options)

    async def get_children(
        self, resource_id: str, bucket_name: str, parent_object_key: str
    ) -> PaginatedOutput[StudyFileOutput]:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="parentObjectKey",
                    operand=FilterOperand.EQ,
                    value=parent_object_key,
                ),
                EntityFilter(
                    key="resourceId",
                    operand=FilterOperand.EQ,
                    value=resource_id,
                ),
                EntityFilter(
                    key="bucketName",
                    operand=FilterOperand.EQ,
                    value=bucket_name,
                ),
            ]
        )
        return await self.find(query_options)

    async def get_root_object(
        self, resource_id: str, bucket_name: str
    ) -> StudyFileOutput:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="resourceId",
                    operand=FilterOperand.EQ,
                    value=resource_id,
                ),
                EntityFilter(
                    key="objectKey",
                    operand=FilterOperand.EQ,
                    value="",
                ),
                EntityFilter(
                    key="bucketName",
                    operand=FilterOperand.EQ,
                    value=bucket_name,
                ),
            ]
        )
        result = await self.find(query_options)

        return result.data if result.data else None

    async def create(self, entity: StudyFileInput) -> StudyFileOutput:
        input_json = entity.model_dump(by_alias=True, exclude="id_")
        filters = {
            "resourceId": entity.resource_id,
            "bucketName": entity.bucket_name,
            "objectKey": entity.object_key,
        }
        current = self.collection.find_one(filters)
        if not current:
            result = self.collection.insert_one(input_json)
            entity.id_ = result.inserted_id
        else:
            input_json["_id"] = current["_id"]
            result = self.collection.update_one(filters, {"$set": input_json})

            entity.id_ = current["_id"]
        return entity

    async def create_objects(self, entities: list[StudyFileInput]) -> list[str]:
        return await self.create_many(entities=entities)

    async def update_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]:
        current_ids = await self.get_ids(
            filters=[
                EntityFilter(key="resourceId", value=entity.resource_id),
                EntityFilter(key="bucketName", value=entity.bucket_name),
                EntityFilter(key="objectKey", value=entity.object_key),
            ]
        )
        if current_ids:
            current_id = current_ids[0]

            input_json = entity.model_dump(by_alias=True, exclude="id_")
            result: UpdateResult = self.collection.update_one(
                {"_id": current_id}, {"$set": input_json}
            )
            if result.modified_count > 0:
                entity.id_ = current_id
                return entity
        return None

    async def delete_object(
        self, entity: StudyFileOutput
    ) -> Union[None, StudyFileOutput]:
        current_ids = await self.get_ids(
            filters=[
                EntityFilter(key="resourceId", value=entity.resource_id),
                EntityFilter(key="bucketName", value=entity.bucket_name),
                EntityFilter(key="objectKey", value=entity.object_key),
            ]
        )
        if current_ids:
            current_id = current_ids[0]

            input_json = entity.model_dump(by_alias=True, exclude="id_")
            result: DeleteResult = self.collection.delete_one(
                {"_id": current_id}, {"$set": input_json}
            )
            if result.deleted_count > 0:
                entity.id_ = current_id
                return entity
        return None

    async def update(self, entity: StudyFileOutput) -> StudyFileOutput:
        input_json = entity.model_dump()
        result: UpdateResult = self.collection.update_one(
            {"_id": ObjectId(entity.id_)}, input_json
        )
        return self.collection.find_one({"_id": result.inserted_id})

    async def delete(self, id_: str) -> bool:
        result: DeleteResult = self.collection.delete_one({"_id": id_})
        if result.deleted_count > 0:
            return True
        return False
