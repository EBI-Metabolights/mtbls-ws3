import abc
import datetime
import pathlib
from typing import Any

from metabolights_utils.common import CamelCaseModel


class IndexDocumentInfo(CamelCaseModel):
    id: str
    updated_at: datetime.datetime


class SearchIndexManagementGateway(abc.ABC):
    @abc.abstractmethod
    async def delete_index(self, index: str) -> bool: ...

    @abc.abstractmethod
    async def create_index(
        self,
        index: str,
        delete_before: bool = False,
        mappings_file_path: None | pathlib.Path = None,
    ) -> bool: ...

    @abc.abstractmethod
    async def index_document(self, index: str, id: str, body: dict[str, Any]) -> bool: ...

    @abc.abstractmethod
    async def remove_document(self, index: str, id: str) -> bool: ...

    @abc.abstractmethod
    async def get_document(self, index: str, id: str) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def get_document_ids(self, index: str, update_field_name: str) -> list[IndexDocumentInfo]: ...
