from typing import Generic

from pydantic import BaseModel
from typing_extensions import TypeVar

from mtbls.domain.shared.data_types import ZeroOrPositiveInt

T = TypeVar("T")


class PaginatedOutput(BaseModel, Generic[T]):
    offset: ZeroOrPositiveInt = 0
    size: ZeroOrPositiveInt = 0
    data: list[T] = []
