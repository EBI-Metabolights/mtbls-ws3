from typing import Any, Union

from pydantic import BaseModel

from mtbls.domain.enums.filter_operand import FilterOperand


class EntityFilter(BaseModel):
    key: str
    operand: FilterOperand = FilterOperand.EQ
    value: Union[None, Any]
