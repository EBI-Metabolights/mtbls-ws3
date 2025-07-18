import datetime
import json
import logging
from typing import Any, Union

import httpx
import jsonschema
from metabolights_utils.models.metabolights import get_study_model_schema
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.utils.http_utils import get_http_response
from mtbls.domain.shared.validator.policy import PolicyInput, ValidationResult
from mtbls.domain.shared.validator.validation import Validation, VersionedValidationsMap
from mtbls.infrastructure.policy_service.opa.opa_configuration import OpaConfiguration

logger = logging.getLogger(__name__)


class OpaPolicyService(PolicyService):
    def __init__(
        self,
        config: Union[OpaConfiguration, dict[str, Any]],
        max_polling_in_seconds: int = 60,
    ):
        super().__init__()
        self.config = config
        if isinstance(self.config, dict):
            self.config = OpaConfiguration.model_validate(config)
        self.control_lists_last_update_time: float = 0.0
        self.templates_last_update_time: float = 0.0
        self.version_update_time: float = 0.0
        self.rule_definitions_update_time: float = 0.0
        self.max_polling_in_seconds = max_polling_in_seconds
        self.versions: list[str] = []
        self.rule_definitions: dict[str, Validation] = {}

    async def get_service_url(self):
        return self.config.validation_url

    async def get_templates(self) -> dict[str, Any]:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if now - self.templates_last_update_time > self.max_polling_in_seconds:
            self.templates = await get_http_response(
                self.config.templates_url, "result"
            )
            self.templates_last_update_time = now
        return self.templates

    async def get_rule_definitions(self) -> dict[str, Any]:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if now - self.rule_definitions_update_time > self.max_polling_in_seconds:
            rules = await get_http_response(self.config.rule_definitions_url, "result")
            self.rule_definitions_update_time = now
            validations_map = VersionedValidationsMap(
                validation_version=rules["validation_version"],
                validations={
                    x["rule_id"]: Validation.model_validate(x)
                    for x in rules["violations"]
                },
            )
            self.rule_definitions = validations_map

        return self.rule_definitions

    async def get_control_lists(self) -> dict[str, Any]:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if now - self.control_lists_last_update_time > self.max_polling_in_seconds:
            self.control_lists = await get_http_response(
                self.config.control_lists_url, "result"
            )
            self.control_lists_last_update_time = now
        return self.control_lists

    async def get_supported_validation_versions(self) -> list[str]:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if now - self.version_update_time > self.max_polling_in_seconds:
            versions_result = await get_http_response(self.config.version_url, "result")
            self.versions = [versions_result]
            self.version_update_time = now
        return self.versions

    async def validate_study(
        self,
        resource_id: str,
        model: MetabolightsStudyModel,
        validate_schema: None | bool = None,
        timeout_in_seconds: None | int = None,
    ) -> ValidationResult:
        opa = PolicyInput()
        opa.input = model
        logger.debug("Loading study model schema to validate %s", resource_id)
        timeout_in_seconds = (
            timeout_in_seconds if timeout_in_seconds else self.config.timeout_in_seconds
        )
        validate_schema = (
            validate_schema if validate_schema else self.config.validate_schema
        )
        #
        dict_value = opa.model_dump(by_alias=True)
        if validate_schema:
            opa.model_json_schema(mode="serialization")
            logger.debug("Validating input model")
            study_model_json_schema = get_study_model_schema()
            jsonschema.validate(
                instance=dict_value["input"], schema=study_model_json_schema
            )
        logger.debug(
            "Sending %s validation request to %s",
            resource_id,
            self.config.validation_url,
        )
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.config.validation_url, json=dict_value, timeout=timeout_in_seconds
            )
            result = json.loads(response.text)
        messages = ValidationResult.model_validate(result["result"])
        logger.debug("Validation report is received for %s", resource_id)
        return messages
