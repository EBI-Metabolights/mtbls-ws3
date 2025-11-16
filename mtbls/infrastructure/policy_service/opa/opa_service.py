import logging
from typing import Any, Union

import jsonschema
from cachetools import TTLCache
from cachetools_async import cached
from metabolights_utils.models.metabolights import get_study_model_schema
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.domain.shared.validator.policy import PolicyInput, ValidationResult
from mtbls.domain.shared.validator.validation import Validation, VersionedValidationsMap
from mtbls.infrastructure.policy_service.opa.opa_configuration import OpaConfiguration

logger = logging.getLogger(__name__)


class OpaPolicyService(PolicyService):
    def __init__(
        self,
        http_client: HttpClient,
        config: Union[OpaConfiguration, dict[str, Any]],
        max_polling_in_seconds: int = 60,
    ):
        super().__init__()
        self.http_client = http_client
        self.config = config
        if isinstance(self.config, dict):
            self.config = OpaConfiguration.model_validate(config)
        self.versions: list[str] = []
        self.rule_definitions: dict[str, Validation] = {}
        self.control_lists: None | ValidationControls = None
        self.templates: None | dict[str, Any] = None

    async def get_service_url(self):
        return self.config.validation_url

    @cached(cache=TTLCache(maxsize=10, ttl=60))
    async def get_templates(self) -> None | FileTemplates:
        try:
            result = await self.get_http_response(self.config.templates_url, "result")
            try:
                self.control_lists = FileTemplates.model_validate(result, by_alias=True)
            except Exception as ex:
                logger.error("Validation templates conversion error: %s", ex)
                return None

        except Exception as ex:
            logger.warning("Validation templates fetch error: %s", ex)
            return None
        return self.templates

    @cached(cache=TTLCache(maxsize=10, ttl=60))
    async def get_rule_definitions(self) -> dict[str, Any]:
        rules = await self.get_http_response(self.config.rule_definitions_url, "result")
        validations_map = VersionedValidationsMap(
            validation_version=rules["validation_version"],
            validations={
                x["rule_id"]: Validation.model_validate(x) for x in rules["violations"]
            },
        )
        self.rule_definitions = validations_map
        return self.rule_definitions

    @cached(cache=TTLCache(maxsize=10, ttl=60))
    async def get_control_lists(self) -> None | ValidationControls:
        try:
            result = await self.get_http_response(
                self.config.control_lists_url, "result"
            )
            try:
                self.control_lists = ValidationControls.model_validate(
                    result, by_alias=True
                )
            except Exception as ex:
                logger.error("Validation control list conversion error: %s", ex)
                return None

        except Exception as ex:
            logger.warning("Validation control list fetch error: %s", ex)
            return None
        return self.control_lists

    @cached(cache=TTLCache(maxsize=10, ttl=60))
    async def get_supported_validation_versions(self) -> list[str]:
        versions_result = await self.get_http_response(
            self.config.version_url, "result"
        )
        self.versions = [versions_result]
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

        response: HttpResponse = await self.http_client.send_request(
            HttpRequestType.POST,
            url=self.config.validation_url,
            json=dict_value,
            timeout=timeout_in_seconds,
        )
        messages = ValidationResult.model_validate(response.json_data.get("result", {}))
        logger.debug("Validation report is received for %s", resource_id)
        return messages

    async def get_http_response(
        self, url: str, root_dict_key: Union[None, str] = None
    ) -> Union[Any, dict[str, Any]]:
        if not url:
            raise ValueError("url is required")

        url = url.rstrip("/")
        if not url:
            return {}

        try:
            response: HttpResponse = await self.http_client.send_request(
                HttpRequestType.GET, url, timeout=10
            )

            if not root_dict_key:
                return response.json_data or {}

            if response.json_data and response.json_data.get(root_dict_key):
                return response.json_data.get(root_dict_key)

            if response.json_data and not response.json_data.get(root_dict_key):
                logger.warning("There is no '%s' in response", root_dict_key)

        except Exception as ex:
            logger.error("Error call %s: %s", url, str(ex))

        return {}
