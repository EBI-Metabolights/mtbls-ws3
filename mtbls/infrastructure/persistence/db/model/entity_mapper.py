from typing import Type, TypeVar

from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.enums.entity import Entity
from mtbls.infrastructure.persistence.db.model.study_models import (
    Base,
    Statistic,
    Study,
    StudyFile,
    StudyRevision,
    User,
)

T = TypeVar("T", bound=BaseEntity)


class EntityMapper:
    entity_table_map: dict[Entity, Base] = {
        Entity.User: User,
        Entity.Study: Study,
        Entity.Statistic: Statistic,
        Entity.StudyFile: StudyFile,
        Entity.StudyRevision: StudyRevision,
    }

    def __init__(self):
        self.table_entity_map: dict[str, Entity] = {
            v.__name__: k for k, v in EntityMapper.entity_table_map.items()
        }
        self.mapped_tables: dict[Entity, dict[str, str]] = {}

    def get_table_model(self, entity_type: Entity) -> Base:
        return self.entity_table_map[entity_type]

    async def convert_to_output_type(self, db_object: Base, output_class: Type[T]) -> T:
        if not db_object:
            return None
        entity_type = output_class.model_config.get("entity_type")
        maped_fields = self.get_field_map(output_class, entity_type)

        object_dict = db_object.__dict__
        values = {
            k: object_dict[v] for k, v in maped_fields.items() if v in object_dict
        }
        return output_class.model_validate(values)

    async def convert_to_output_type_list(
        self, db_objects: list[Base], output_class: Type[T]
    ) -> list[T]:
        if not db_objects:
            return []
        result = []
        for db_object in db_objects:
            result.append(await self.convert_to_output_type(db_object, output_class))
        return result

    def get_field_map(self, entity: Type[T], entity_type: Entity) -> dict[str, str]:
        field_name_map = self.mapped_tables.get(entity_type, {})
        if field_name_map:
            return field_name_map

        mapped_table: Base = self.get_table_model(entity_type)
        for field in entity.model_fields:
            value = mapped_table.get_field_alias(field)
            if value:
                field_name_map[field] = value
        self.mapped_tables[entity_type] = field_name_map
        return field_name_map
