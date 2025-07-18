from metabolights_utils.common import CamelCaseModel

from mtbls.domain.entities.base_file_object import BaseFileObject
from mtbls.domain.shared.validator.validation import ValidationOverride


class ValidationOverrideList(CamelCaseModel):
    validation_version: str = ""
    validation_overrides: list[ValidationOverride] = []


class ValidationOverrideFileObject(BaseFileObject[ValidationOverrideList]): ...
