import abc

from mtbls.domain.entities.validation.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.validation import (
    VersionedValidationsMap,
)


class ValidationOverrideService(abc.ABC):
    @abc.abstractmethod
    async def get_validation_definitions(self) -> VersionedValidationsMap: ...

    @abc.abstractmethod
    async def get_validation_overrides(
        self, resource_id: str
    ) -> ValidationOverrideList: ...

    @abc.abstractmethod
    async def save_validation_overrides(
        self, resource_id: str, validation_overrides: ValidationOverrideList
    ) -> bool: ...
