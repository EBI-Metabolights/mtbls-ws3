import datetime
from typing import Dict, List, Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from pydantic import Field, field_validator
from typing_extensions import Annotated

from mtbls.domain.entities.validation_override import ValidationOverrideList
from mtbls.domain.shared.modifier import UpdateLog
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase


class PolicyInput(CamelCaseModel):
    input: MetabolightsStudyModel = Field(MetabolightsStudyModel())


class PolicyMessage(CamelCaseModel):
    identifier: str = ""
    title: str = ""
    description: str = ""
    section: str = ""
    priority: str = ""
    type: PolicyMessageType = PolicyMessageType.INFO
    violation: str = ""
    source_file: str = ""
    source_column_header: str = ""
    source_column_index: Union[str, int] = ""
    has_more_violations: bool = False
    total_violations: int = 0
    values: List[str] = []
    technique: str = ""
    overridden: bool = False
    override_comment: str = ""

    @field_validator("type", mode="before")
    @classmethod
    def new_type_validator(cls, value):
        if isinstance(value, PolicyMessageType):
            return value
        if isinstance(value, str):
            return PolicyMessageType(value)

        return PolicyMessageType.INFO


class PhasedPolicyMessage(PolicyMessage):
    phase: str = ""


class HierarchicalPolicyMessages(CamelCaseModel):
    result: Dict[str, List[Dict[str, List[PolicyMessage]]]] = Field({})


class ValidationResult(CamelCaseModel):
    violations: Annotated[List[PolicyMessage], Field([])] = []
    summary: Annotated[List[PolicyMessage], Field([])] = []


class PolicyMessageList(CamelCaseModel):
    result: Annotated[PolicyMessage, Field()] = PolicyMessage()


class PolicyResult(CamelCaseModel):
    messages: ValidationResult = ValidationResult()
    resource_id: str = ""
    phases: List[ValidationPhase] = []
    page: int = 1
    total_page: int = 1
    start_time: Union[None, str, datetime.datetime] = None
    completion_time: Union[None, str, datetime.datetime] = None
    assay_file_techniques: Dict[str, str] = {}
    maf_file_techniques: Dict[str, str] = {}
    metadata_updates: List[UpdateLog] = []
    metadata_modifier_enabled: bool = False

    @field_validator("phases")
    @classmethod
    def new_type_validator(cls, value):
        if value is None:
            return []
        if isinstance(value, str) and value:
            return [ValidationPhase(value)]

        if isinstance(value, list) and value:
            return [ValidationPhase(x) for x in value]
        return []


class PolicyResultList(CamelCaseModel):
    results: List[PolicyResult] = []


class PolicySummaryResult(CamelCaseModel):
    resource_id: str = ""
    task_id: str = ""
    status: PolicyMessageType = PolicyMessageType.ERROR
    start_time: Union[None, str, datetime.datetime] = None
    completion_time: Union[None, str, datetime.datetime] = None
    duration_in_seconds: float = 0
    messages: ValidationResult = ValidationResult()
    metadata_updates: List[UpdateLog] = []
    metadata_modifier_enabled: bool = False
    assay_file_techniques: Dict[str, str] = {}
    maf_file_techniques: Dict[str, str] = {}
    overrides: ValidationOverrideList = ValidationOverrideList()

    @field_validator("status", mode="before")
    @classmethod
    def new_type_validator(cls, value):
        if isinstance(value, PolicyMessageType):
            return value
        if isinstance(value, str):
            return PolicyMessageType(value)

        return PolicyMessageType.INFO

    @field_validator("metadata_updates", mode="before")
    @classmethod
    def metadata_updates_validator(cls, value):
        result = []
        if isinstance(value, list):
            for item in value:
                if isinstance(item, UpdateLog):
                    result.append(item)
                elif isinstance(item, str):
                    result.append(
                        UpdateLog(action=item, source="", old_value="", new_value="")
                    )
        else:
            if isinstance(value, UpdateLog):
                result.append(item)
            elif isinstance(value, str):
                result.append(
                    UpdateLog(action=value, source="", old_value="", new_value="")
                )
        return result
