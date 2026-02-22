import abc
from typing import Any


class DataIndexClient(abc.ABC):
    @abc.abstractmethod
    async def search(self, index, body: dict[str, Any], **kwargs) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def ensure_started() -> None: ...

    @abc.abstractmethod
    async def bulk(self, index: str, operations: Any, **kwargs) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def delete(
        self, index: str, ignore_status: bool = False, **kwargs
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def exists(self, index: str, **kwargs) -> bool: ...

    @abc.abstractmethod
    async def delete_by_query(
        self, index: str, body: dict[str, Any] = None, **kwargs
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def delete_by_id(self, index: str, id: str, **kwargs) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def create(
        self, index: str, mappings: dict[str, Any], max_retries: int = 1, **kwargs
    ) -> dict[str, Any]: ...
