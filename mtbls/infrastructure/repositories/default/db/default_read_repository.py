import enum
import logging
from typing import Any, Union

from pydantic import BaseModel
from pydantic.alias_generators import to_snake
from sqlalchemy import select
from typing_extensions import OrderedDict

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.enums.entity import Entity
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.sort_order import SortOrder
from mtbls.domain.shared.data_types import ID_TYPE, OUTPUT_TYPE
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryFieldOptions, QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.persistence.db.model.study_models import Base

logger = logging.getLogger(__name__)


class SqlDbDefaultReadRepository(AbstractReadRepository[OUTPUT_TYPE, ID_TYPE]):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        self.entity_mapper = entity_mapper
        self.alias_generator = alias_generator
        self.database_client = database_client

        generics = self.__orig_bases__[0].__args__

        self.output_type: BaseModel = generics[0]
        self.output_type_alias_dict = OrderedDict()
        for field in self.output_type.model_fields:
            entity_type: Entity = self.output_type.__entity_type__
            value = alias_generator.get_alias(entity_type, field)
            self.output_type_alias_dict[field] = value
        self.id_type = generics[1]
        self.managed_table = self.entity_mapper.get_table_model(
            self.output_type.__entity_type__
        )

    async def convert_to_output_type(self, db_object: Base) -> OUTPUT_TYPE:
        if not db_object:
            return None
        object_dict = db_object.__dict__

        value = {
            x: object_dict[self.output_type_alias_dict[x]]
            for x in self.output_type_alias_dict
        }
        return self.output_type.model_validate(value)

    async def convert_to_output_type_list(
        self, db_objects: list[Base]
    ) -> list[OUTPUT_TYPE]:
        if not db_objects:
            return []
        result = []
        for db_object in db_objects:
            result.append(await self.convert_to_output_type(db_object))
        return result

    @validate_inputs_outputs
    async def get_by_id(self, id_: ID_TYPE) -> Union[None, OUTPUT_TYPE]:
        table = self.managed_table
        async with self.database_client.session() as session:
            stmt = select(table).where(table.id == id_)
            result = await session.execute(stmt)
            db_object = result.scalars().one_or_none()
            return await self.convert_to_output_type(db_object)

    @validate_inputs_outputs
    async def get_ids(
        self,
        filters: Union[None, list[EntityFilter]] = None,
    ) -> list[ID_TYPE]:
        params = QueryFieldOptions(
            selected_fields=["id_"],
            filters=filters,
            sort_options=[
                SortOption(
                    key="id_",
                    direction=SortOrder.ASC,
                ),
            ],
        )
        result = await self.select_fields(query_field_options=params)
        return result.data

    @validate_inputs_outputs
    async def find(
        self,
        query_options: QueryOptions,
    ) -> PaginatedOutput:
        params = QueryFieldOptions.model_validate(query_options, from_attributes=True)
        return await self.select_fields(query_field_options=params)

    @validate_inputs_outputs
    async def select_field(
        self,
        field_name: str,
        query_options: QueryOptions,
    ) -> PaginatedOutput:
        params = QueryFieldOptions.model_validate(query_options, from_attributes=True)
        params.selected_fields = [field_name]
        result = await self.select_fields(query_field_options=params)
        result.data = [x[0] for x in result.data]
        return result

    @validate_inputs_outputs
    async def select_fields(
        self,
        query_field_options: QueryFieldOptions,
    ) -> PaginatedOutput:
        async with self.database_client.session() as a_session:
            return await self._find_entities(
                a_session, query_field_options=query_field_options
            )

    @validate_inputs_outputs
    async def _find_entities(
        self, session, query_field_options: QueryFieldOptions
    ) -> PaginatedOutput:
        if not session:
            raise Exception("Session is not provided.")
        output = PaginatedOutput()
        stmt = await self._build_query(query_field_options=query_field_options)

        result = await session.execute(stmt)

        if result:
            if not query_field_options.selected_fields:
                db_objects = result.scalars().all()
                if db_objects:
                    output.data = await self.convert_to_output_type_list(db_objects)
            else:
                all_data = result.all()
                if all_data:
                    output.data = all_data
            output.size = len(output.data)
            output.offset = (
                query_field_options.offset if query_field_options.offset else 0
            )
        return output

    @validate_inputs_outputs
    async def _get_first_by_field_name(
        self, field_name: str, value: Any
    ) -> Union[None, OUTPUT_TYPE]:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key=to_snake(field_name),
                    operand=FilterOperand.EQ,
                    value=value,
                )
            ]
        )
        result = await self.find(query_options)
        if result and result.data:
            return result.data[0]
        return None

    @validate_inputs_outputs
    async def _build_query(self, query_field_options: QueryFieldOptions):
        table = self.managed_table
        selected_fields = await self._select_query_fields(
            table, self.output_type, query_field_options.selected_fields
        )
        stmt = select(*selected_fields)
        stmt = await self._filter_query(
            stmt, table, self.output_type, query_field_options.filters
        )
        stmt = await self._sort_query(
            stmt, table, self.output_type, query_field_options.sort_options
        )
        if query_field_options.limit:
            stmt = stmt.limit(query_field_options.limit)
        if query_field_options.offset:
            stmt = stmt.offset(query_field_options.offset)
        return stmt

    @validate_inputs_outputs
    async def _select_query_fields(
        self,
        model_class: type[Base],
        scheme_class: type[BaseModel],
        selected_fields: Union[None, list[str]],
    ) -> Any:
        if not selected_fields:
            return [model_class]
        fields = []
        for field in selected_fields:
            if field not in self.output_type_alias_dict:
                logger.warning("%s is not in %s", field, model_class.__name__)
            column_name = self.output_type_alias_dict[field]
            column = getattr(model_class, column_name, None)
            fields.append(column)
        return fields

    @validate_inputs_outputs
    async def _sort_query(
        self,
        query,
        model_class: type[Base],
        scheme_class: type[BaseModel],
        sort_options: Union[None, list[SortOption]],
    ) -> Any:
        if not sort_options:
            return query
        for sort in sort_options:
            sort_key = to_snake(sort.key)
            if sort_key not in self.output_type_alias_dict:
                logger.warning(
                    "Sort field %s is not in %s", sort_key, model_class.__name__
                )
            column_name = self.output_type_alias_dict[sort_key]
            column = getattr(model_class, column_name, None)
            if not column:
                raise Exception("Invalid sort column: %s" % column_name)
            if sort.order == SortOrder.ASC:
                query = query.order_by(column.asc())
            else:
                query = query.order_by(column.desc())
        return query

    @validate_inputs_outputs
    async def _filter_query(
        self,
        query,
        model_class: type[Base],
        scheme_class: type[BaseModel],
        filter_conditions: Union[None, list[EntityFilter]],
    ) -> Any:
        if not filter_conditions:
            return query
        for filter in filter_conditions:
            try:
                filter_key, op, value = (
                    to_snake(filter.key),
                    filter.operand,
                    filter.value,
                )
            except ValueError:
                raise Exception("Invalid filter: %s" % filter.model_dump_json())
            if filter_key not in self.output_type_alias_dict:
                logger.warning(
                    ("Sort field %s is not in %s", filter_key, model_class.__name__)
                )
            column_name = self.output_type_alias_dict[filter_key]
            column = getattr(model_class, column_name, None)
            value = value.value if isinstance(value, enum.Enum) else value
            if not column:
                raise Exception("Invalid filter column: %s" % column_name)
            if op == FilterOperand.IN:
                if isinstance(value, list):
                    filt = column.in_(value)
                else:
                    filt = column.in_(value.split(","))
            else:
                try:
                    operation = f"__{op.name.lower()}__"
                    attr = getattr(column, operation)
                except IndexError:
                    raise Exception("Invalid filter operator: %s" % op)
                if value == "null":
                    value = None
                filt = attr(value)
            query = query.filter(filt)
        return query
