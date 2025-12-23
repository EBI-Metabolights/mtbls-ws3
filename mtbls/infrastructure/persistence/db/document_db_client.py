import abc
from typing import Any, AsyncGenerator

from pymongo import MongoClient


class DocumentDatabaseClient(abc.ABC):
    @abc.abstractmethod
    async def get_connection_repr(self) -> str: ...

    @abc.abstractmethod
    async def client(self) -> AsyncGenerator[MongoClient, None]: ...

    @abc.abstractmethod
    async def database(self) -> AsyncGenerator[Any, None]: ...

    @abc.abstractmethod
    async def ping(self) -> bool: ...
