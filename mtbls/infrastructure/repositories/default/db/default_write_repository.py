import logging
from typing import Any, Type

from pydantic import BaseModel
from sqlalchemy import select
from typing_extensions import OrderedDict

from mtbls.application.decorators.validate import validate_inputs_outputs
from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.enums.entity import Entity
from mtbls.domain.shared.data_types import ID_TYPE, INPUT_TYPE, OUTPUT_TYPE
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.persistence.db.model.study_models import Base

logger = logging.getLogger(__name__)


class SqlDbDefaultWriteRepository(
    AbstractWriteRepository[INPUT_TYPE, OUTPUT_TYPE, ID_TYPE]
):
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

        self.input_type: Type[BaseModel] = generics[0]
        self.input_entity_type: Entity = self.input_type.model_config.get("entity_type")

        self.input_type_alias_dict = OrderedDict()
        for field in self.input_type.model_fields:
            value = alias_generator.get_alias(self.input_entity_type, field)
            self.input_type_alias_dict[field] = value

        self.output_type: Type[BaseModel] = generics[1]
        self.output_entity_type: Entity = self.output_type.model_config.get(
            "entity_type"
        )
        self.output_type_alias_dict = OrderedDict()
        for field in self.output_type.model_fields:
            value = alias_generator.get_alias(self.output_entity_type, field)
            self.output_type_alias_dict[field] = value

        self.id_type = generics[2]
        self.managed_table = self.entity_mapper.get_table_model(self.output_entity_type)

    async def convert_to_input_dict(self, entity: Type[BaseEntity]) -> dict[str, Any]:
        value_dict = {}
        values = entity.model_dump(by_alias=False)
        for field_name in entity.model_fields:
            alias = self.alias_generator.get_alias(self.input_entity_type, field_name)
            if hasattr(self.managed_table, alias):
                value_dict[alias] = values[field_name]
            value_dict[alias] = values[field_name]
        return value_dict

    async def update_table_row(self, table_row: Base, entity: BaseEntity) -> Base:
        values = entity.model_dump(by_alias=False)
        for field_name in entity.model_fields_set:
            alias = self.alias_generator.get_alias(self.input_entity_type, field_name)
            if hasattr(self.managed_table, alias):
                setattr(table_row, alias, values[field_name])
        return table_row

    @validate_inputs_outputs
    async def create(self, entity: INPUT_TYPE) -> OUTPUT_TYPE:
        value_dict = await self.convert_to_input_dict(entity)
        table_row = self.managed_table(**value_dict)
        async with self.database_client.session() as session:
            async with session.begin():
                session.add(table_row)
            await session.commit()
            await session.refresh(table_row)

            return await self.entity_mapper.convert_to_output_type(
                table_row, self.output_type
            )

    @validate_inputs_outputs
    async def create_many(self, entities: list[INPUT_TYPE]) -> list[OUTPUT_TYPE]:
        table_rows = []
        for entity in entities:
            value_dict = await self.convert_to_input_dict(entity)
            table_row = self.managed_table(**value_dict)
            table_rows.append(table_row)

        outputs = []
        async with self.database_client.session() as session:
            async with session.begin():
                session.add_all(table_rows)
            await session.commit()
            for table_row in table_rows:
                await session.refresh(table_row)
                outputs.append(
                    await self.entity_mapper.convert_to_output_type(
                        table_row, self.output_type
                    )
                )
        return outputs

    async def update(self, entity: OUTPUT_TYPE) -> OUTPUT_TYPE:
        table = self.managed_table
        async with self.database_client.session() as session:
            try:
                stmt = select(table).where(table.id == entity.id_)
                result = await session.execute(stmt)
                table_row_result = result.scalars().one_or_none()
                table_row = await self.update_table_row(table_row_result, entity)
                session.add(table_row)
                await session.commit()
                return await self.entity_mapper.convert_to_output_type(
                    table_row, self.output_type
                )
            except Exception as ex:
                logger.error(str(ex))
                await session.rollback()

    async def delete(self, id_: ID_TYPE) -> bool:
        table = self.managed_table
        async with self.database_client.session() as session:
            stmt = select(table).where(table.id == id_)
            result = await session.execute(stmt)
            table_row = result.scalars().one_or_none()
            if table_row:
                await session.delete(table_row)
                await session.commit()
                return True
            return False
