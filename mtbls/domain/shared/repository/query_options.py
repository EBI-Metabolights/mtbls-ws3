from typing import Union

from pydantic import BaseModel

from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.sort_option import SortOption


class QueryOptions(BaseModel):
    filters: Union[None, list[EntityFilter]] = None
    sort_options: Union[None, list[SortOption]] = None
    offset: Union[None, ZeroOrPositiveInt] = None
    limit: Union[None, ZeroOrPositiveInt] = None


class QueryFieldOptions(QueryOptions):
    selected_fields: Union[None, list[str]] = None
