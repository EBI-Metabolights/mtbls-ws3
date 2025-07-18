import datetime
from typing import Annotated, Literal

import annotated_types
from pydantic import BeforeValidator
from typing_extensions import TypeVar

from mtbls.domain.entities.base_entity import (
    BaseInputEntity,
    BaseOutputEntity,
)
from mtbls.domain.shared.model_validators import validate_datetime, validate_integer

UtcDatetime = Annotated[datetime.datetime, BeforeValidator(validate_datetime)]
Integer = Annotated[int, BeforeValidator(validate_integer)]
ZeroOrPositiveInt = Annotated[int, annotated_types.Ge(0)]
PositiveInt = Annotated[int, annotated_types.Ge(1)]
TokenStr = Annotated[str, annotated_types.MinLen(20), annotated_types.MaxLen(10000)]
TaskIdStr = Annotated[str, annotated_types.MinLen(20), annotated_types.MaxLen(500)]
JsonPathOperation = Literal["get", "insert", "delete", "update", "patch"]
IsaTableOperation = Literal[
    "update-cells",
    "add-rows",
    "delete-rowsupdate-columns",
    "add-columns",
    "remove-columns",
]

INPUT_TYPE = TypeVar("INPUT_TYPE", bound=BaseInputEntity)
OUTPUT_TYPE = TypeVar("OUTPUT_TYPE", bound=BaseOutputEntity)
ID_TYPE = TypeVar("ID_TYPE")
