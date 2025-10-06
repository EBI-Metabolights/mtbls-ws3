import logging
from typing import Any, Generic, Union

import pymongo

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.sort_order import SortOrder
from mtbls.domain.shared.data_types import ID_TYPE, OUTPUT_TYPE
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryFieldOptions, QueryOptions
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)

logger = logging.getLogger(__name__)


class MongoDbDefaultReadRepository(
    AbstractReadRepository[OUTPUT_TYPE, ID_TYPE], Generic[OUTPUT_TYPE, ID_TYPE]
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str,
        output_entity_class: type[BaseEntity],
    ):
        cn = connection
        self.connection = connection
        self.url = (
            f"{cn.url_scheme}://{cn.user}:{cn.password}" + f"@{cn.host}:{cn.port}"
        )
        self.url_repr = f"{cn.url_scheme}://{cn.user}:***@{cn.host}:{cn.port}"
        self.client = pymongo.MongoClient(self.url)
        self.db = self.client[cn.database]
        self.collection = self.db[collection_name]
        self.output_entity_class = output_entity_class

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

    async def get_by_id(self, id_: str) -> OUTPUT_TYPE:
        filter = {"_id": id_}
        result = self.collection.find_one(filter)
        if result:
            return self.output_entity_class.model_validate(result)
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
    ) -> PaginatedOutput[OUTPUT_TYPE]:
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
        result_data = [x for x in result]
        items = [self.output_entity_class.model_validate(x) for x in result_data]
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
        items = [self.output_entity_class.model_validate(x) for x in result]
        return PaginatedOutput(offset=offset, size=len(items), data=items)

    async def select_field(
        self, field_name, query_options: QueryOptions
    ) -> PaginatedOutput[tuple[Any, ...]]:
        query_field_options = QueryFieldOptions.model_validate(query_options)
        query_field_options.selected_fields = [field_name]
        return await self.select_fields(query_field_options=query_field_options)

    async def get_children(
        self, resource_id: str, bucket_name: str, parent_object_key: str
    ) -> PaginatedOutput[OUTPUT_TYPE]:
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

    async def get_root_object(self, resource_id: str) -> OUTPUT_TYPE:
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
            ]
        )
        result = await self.find(query_options)

        return result.data if result.data else None
