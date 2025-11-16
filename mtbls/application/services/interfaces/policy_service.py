import abc
from typing import Union

from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
from mtbls.domain.shared.validator.policy import ValidationResult
from mtbls.domain.shared.validator.validation import VersionedValidationsMap


class PolicyService(abc.ABC):
    @abc.abstractmethod
    async def get_templates(self) -> None | FileTemplates: ...

    @abc.abstractmethod
    async def get_control_lists(self) -> None | ValidationControls: ...

    @abc.abstractmethod
    async def get_rule_definitions(
        self, version: Union[None, str] = None
    ) -> VersionedValidationsMap: ...

    @abc.abstractmethod
    async def validate_study(
        self,
        resource_id: str,
        model: MetabolightsStudyModel,
        validate_schema: None | bool = None,
        timeout_in_seconds: None | int = None,
    ) -> ValidationResult: ...

    async def get_supported_validation_versions(self) -> list[str]: ...

    async def get_service_url(self) -> str: ...
