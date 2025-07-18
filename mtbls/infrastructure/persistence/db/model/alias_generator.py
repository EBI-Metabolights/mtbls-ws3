from mtbls.domain.enums.entity import Entity
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper


class DbTableAliasGeneratorImpl(AliasGenerator):
    def __init__(self, entity_mapper: EntityMapper):
        self.entity_mapper = entity_mapper

    def get_alias(self, entity_type: Entity, value: str) -> str:
        return self.entity_mapper.get_table_model(entity_type).get_field_alias(value)
