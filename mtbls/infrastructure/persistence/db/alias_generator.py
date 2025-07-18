import abc

from mtbls.domain.enums.entity import Entity


class AliasGenerator(abc.ABC):
    @abc.abstractmethod
    def get_alias(self, entity_type: Entity, value: str) -> str: ...
