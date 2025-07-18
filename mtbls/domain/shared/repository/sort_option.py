from pydantic import BaseModel

from mtbls.domain.enums.sort_order import SortOrder


class SortOption(BaseModel):
    key: str
    order: SortOrder = SortOrder.ASC
