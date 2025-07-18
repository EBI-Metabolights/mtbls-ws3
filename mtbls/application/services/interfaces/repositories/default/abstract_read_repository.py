import abc
from typing import Any, Generic, Union

from mtbls.domain.shared.data_types import ID_TYPE, OUTPUT_TYPE
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryFieldOptions, QueryOptions


class AbstractReadRepository(abc.ABC, Generic[OUTPUT_TYPE, ID_TYPE]):
    @abc.abstractmethod
    async def get_by_id(self, id_: ID_TYPE) -> OUTPUT_TYPE: ...

    @abc.abstractmethod
    async def get_ids(
        self,
        filters: Union[None, list[EntityFilter]],
    ) -> list[ID_TYPE]: ...

    @abc.abstractmethod
    async def find(
        self,
        query_options: QueryOptions,
    ) -> PaginatedOutput[OUTPUT_TYPE]: ...

    @abc.abstractmethod
    async def select_fields(
        self, query_field_options: QueryFieldOptions
    ) -> PaginatedOutput[tuple[Any, ...]]: ...

    @abc.abstractmethod
    async def select_field(
        self, field_name, query_options: QueryOptions
    ) -> PaginatedOutput[tuple[Any, ...]]: ...
