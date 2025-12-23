import abc
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Union

from pymongo import MongoClient
from pymongo.database import Database

from mtbls.infrastructure.persistence.db.mongodb.config import MongoDbConnection
from mtbls.infrastructure.persistence.db.document_db_client import DocumentDatabaseClient

logger = logging.getLogger(__name__)


class MongoDatabaseClientImpl(DocumentDatabaseClient):
    def __init__(
        self,
        db_connection: Union[MongoDbConnection, dict[str, Any]],
    ) -> None:
        self.db_connection = db_connection
        if isinstance(db_connection, dict):
            self.db_connection = MongoDbConnection.model_validate(db_connection)
        cn = self.db_connection

        auth_part = ""
        auth_part_repr = ""
        if cn.user and cn.password:
            auth_part = f"{cn.user}:{cn.password}@"
            auth_part_repr = f"{cn.user}:***@"

        host_port = f"{cn.host}:{cn.port}" if cn.port and cn.port > 0 else cn.host
        db_suffix = f"/{cn.database}" if cn.database else ""
        query_params = []
        if cn.auth_source:
            query_params.append(f"authSource={cn.auth_source}")
        query_suffix = f"?{'&'.join(query_params)}" if query_params else ""

        self.db_url = f"{cn.url_scheme}://{auth_part}{host_port}{db_suffix}{query_suffix}"
        self.db_url_repr = (
            f"{cn.url_scheme}://{auth_part_repr}{host_port}{db_suffix}{query_suffix}"
        )
        self.database_name = cn.database

    async def get_connection_repr(self) -> str:
        return self.db_url_repr

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[MongoClient, None]:
        client = MongoClient(self.db_url)
        try:
            yield client
        except Exception as ex:
            logger.exception(ex)
            raise
        finally:
            client.close()

    @asynccontextmanager
    async def database(self) -> AsyncGenerator[Database, None]:
        async with self.client() as client:
            yield client[self.database_name]

    async def ping(self) -> bool:
        try:
            async with self.client() as client:
                client.admin.command("ping")
                return True
        except Exception:
            logger.exception("Mongo ping failed for %s", self.db_url_repr)
            return False
