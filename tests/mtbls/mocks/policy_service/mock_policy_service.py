import json
import logging
from pathlib import Path
from typing import Any, Union

from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
from mtbls.domain.shared.validator.policy import ValidationResult
from mtbls.domain.shared.validator.validation import Validation, VersionedValidationsMap

logger = logging.getLogger(__name__)


class MockPolicyService(PolicyService):
    def __init__(
        self,
        versions: Union[None, list[str]] = None,
    ):
        self.versions = versions
        if not self.versions:
            self.versions = ["v0.0-mock"]
        with Path("tests/mtbls/mocks/policy_service/rule_definitions.json").open() as f:
            self.rule_definitions = VersionedValidationsMap(
                validation_version=self.versions[0],
                validations={
                    x["rule_id"]: Validation.model_validate(x)
                    for x in json.load(f)["result"]
                },
            )
        with Path("tests/mtbls/mocks/policy_service/templates.json").open() as f:
            self.templates = FileTemplates.model_validate(
                json.load(f)["result"], by_alias=True
            )
        with Path("tests/mtbls/mocks/policy_service/control_lists.json").open() as f:
            self.control_lists = ValidationControls.model_validate(
                json.load(f)["result"], by_alias=True
            )

        self.validation_result = ValidationResult()

    async def set_result(self, validation_result: ValidationResult):
        self.validation_result = validation_result

    async def get_service_url(self):
        return "localhost"

    async def get_templates(self) -> FileTemplates:
        return self.templates

    async def get_rule_definitions(self) -> dict[str, Any]:
        return self.rule_definitions

    async def get_control_lists(self) -> None | ValidationControls:
        return self.control_lists

    async def get_supported_validation_versions(self) -> list[str]:
        return self.versions

    async def validate_study(
        self,
        resource_id: str,
        model: MetabolightsStudyModel,
        validate_schema: None | bool = None,
        timeout_in_seconds: None | int = None,
    ) -> ValidationResult:
        return self.validation_result
