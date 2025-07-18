import datetime
from typing import Union

from metabolights_utils.common import CamelCaseModel
from pydantic import field_serializer, field_validator

from mtbls.domain.shared.validator.types import PolicyMessageType


class ValidationOverrideUpdate(CamelCaseModel):
    enabled: bool = True
    new_type: PolicyMessageType = PolicyMessageType.WARNING
    curator: str = ""
    comment: str = ""


class ValidationOverrideInput(CamelCaseModel):
    override_id: str = ""
    rule_id: str = ""
    source_file: str = ""
    source_column_header: str = ""
    source_column_index: Union[str, int, None] = ""
    update: ValidationOverrideUpdate = ValidationOverrideUpdate()

    @field_serializer("source_column_index")
    @classmethod
    def datetime_serializer(cls, value):
        if value is None:
            return ""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value) if value.isnumeric() else ""
        return ""

    @field_validator("source_column_index")
    @classmethod
    def datetime_validator(cls, value):
        if value is None:
            return ""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value) if value.isnumeric() else ""
        return ""


class ValidationOverride(CamelCaseModel):
    override_id: str = ""
    rule_id: str = ""
    source_file: str = ""
    source_column_header: str = ""
    source_column_index: Union[str, int, None] = ""
    title: str = ""
    description: str = ""
    enabled: bool = True
    curator: str = ""
    comment: str = ""
    new_type: PolicyMessageType = PolicyMessageType.WARNING
    old_type: PolicyMessageType = PolicyMessageType.ERROR
    created_at: Union[datetime.datetime, str, None] = None
    modified_at: Union[datetime.datetime, str, None] = None

    @field_validator("new_type", mode="before")
    @classmethod
    def new_type_validator(cls, value):
        if isinstance(value, PolicyMessageType):
            return value
        if isinstance(value, str):
            return PolicyMessageType(value)

        return PolicyMessageType.WARNING

    @field_validator("old_type", mode="before")
    @classmethod
    def old_type_validator(cls, value):
        if isinstance(value, PolicyMessageType):
            return value
        if isinstance(value, str):
            return PolicyMessageType(value)

        return PolicyMessageType.ERROR


class Validation(CamelCaseModel):
    rule_id: str = ""
    title: str = ""
    description: str = ""
    priority: str = ""
    type: PolicyMessageType = PolicyMessageType.INFO
    section: str = ""


class VersionedValidations(CamelCaseModel):
    validation_version: str = ""
    validations: list[Validation] = []


class VersionedValidationsMap(CamelCaseModel):
    validation_version: str = ""
    validations: dict[str, Validation] = {}
