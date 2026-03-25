import abc
import datetime
from typing import Any

from metabolights_utils.common import CamelCaseModel


class IndexDocumentInfo(CamelCaseModel):
    id: str
    updated_at: datetime.datetime


class IndexClientResponse(CamelCaseModel):
    raw: dict[str, Any]
    body: dict[str, Any]


class SearchIndexManagementGateway(abc.ABC):
    @abc.abstractmethod
    async def create_index(
        self,
        index: str,
        mappings: None | dict[str, Any],
        settings: None | dict[str, Any],
        delete_before: bool = False,
        max_retries: int = 1,
        **kwargs,
    ) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def delete_index(self, index: str, **kwargs) -> IndexClientResponse: ...

    async def search(
        self, index, body: dict[str, Any], **kwargs
    ) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def exists(self, index: str, **kwargs) -> bool: ...

    @abc.abstractmethod
    async def get_document(self, index: str, id: str) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def bulk(
        self, index: str, operations: Any, **kwargs
    ) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def index_document(
        self, index: str, id: str, body: dict[str, Any], **kwargs
    ) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def delete_document(self, index: str, id: str) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def delete_by_query(
        self, index: str, body: dict[str, Any] = False, **kwargs
    ) -> IndexClientResponse: ...

    @abc.abstractmethod
    async def delete_by_id(
        self, index: str, id: str, **kwargs
    ) -> IndexClientResponse: ...

    async def get_document_ids(
        self,
        index: str,
        update_field_name: None | str,
        from_: int = 0,
        size: int = 20000,
    ) -> list[IndexDocumentInfo]: ...

    @abc.abstractmethod
    async def get_all_documents_with_fields(
        self,
        index: str,
        fields: None | list[str] = None,
        from_: int = 0,
        size: int = 10000,
    ) -> tuple[list[str], list[str], list[str]]: ...
