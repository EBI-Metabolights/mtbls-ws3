import datetime
import json
import pathlib
from typing import Any, OrderedDict

import aiofiles
from elasticsearch import AsyncElasticsearch

from mtbls.application.services.interfaces.search_index_management_gateway import (
    IndexDocumentInfo,
    SearchIndexManagementGateway,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClientConfig


class ElasticsearchIndexManagementGateway(SearchIndexManagementGateway):
    def __init__(self, config: None | ElasticsearchClientConfig | dict[str, Any]):
        self._config = config
        if not self._config:
            self._config = ElasticsearchClientConfig()
        elif isinstance(self._config, dict):
            self._config = ElasticsearchClientConfig.model_validate(config)

        # Determine auth method: basic_auth (username/password) takes precedence over api_key
        basic_auth = None
        if self._config.username and self._config.password:
            basic_auth = (self._config.username, self._config.password)

        self.es = AsyncElasticsearch(
            hosts=self._config.hosts or None,
            basic_auth=basic_auth,
            api_key=self._config.api_key if not basic_auth else None,
            request_timeout=self._config.request_timeout,
            verify_certs=self._config.verify_certs,
        )

    async def load_file(self, file_path: pathlib.Path):
        async with aiofiles.open(file_path, "rb") as file:
            contents = await file.read()
            json_file = json.loads(contents)
            return json_file

    async def delete_index(self, index: str) -> bool:
        await self.es.options(ignore_status=404).indices.delete(index=index)
        return True

    async def create_index(
        self,
        index: str,
        delete_before: bool = False,
        mappings_file_path: None | pathlib.Path = None,
    ) -> bool:
        mappings = None
        if delete_before:
            await self.es.options(ignore_status=404, request_timeout=30, retry_on_timeout=True).indices.delete(
                index=index
            )
        if mappings_file_path:
            mappings = await self.load_file(mappings_file_path)
        await self.es.options(ignore_status=400, request_timeout=30, retry_on_timeout=True).indices.create(
            index=index, body=mappings
        )
        return True

    async def index_document(self, index: str, id: str, body: dict[str, Any]) -> bool:
        await self.es.index(index=index, id=id, body=body)
        return True

    async def remove_document(self, index: str, id: str) -> bool:
        await self.es.options(ignore_status=404).indices.delete(index=index, id=id)
        return True

    async def get_document(self, index: str, id: str) -> dict[str, Any]:
        return await self.es.get(index=index, id=id)

    async def get_document_ids(
        self,
        index: str,
        update_field_name: None | str,
        from_: int = 0,
        size: int = 10000,
    ) -> list[IndexDocumentInfo]:
        update_time_fields = [update_field_name] if update_field_name else None
        documents = await self.get_all_documents_with_fields(
            index=index, fields=update_time_fields, from_=from_, size=size
        )
        return [
            IndexDocumentInfo(id=x, updated_at=datetime.datetime.fromisoformat(x[update_field_name])) for x in documents
        ]

    async def get_all_documents_with_fields(
        self,
        index: str,
        fields: None | list[str] = None,
        from_: int = 0,
        size: int = 10000,
    ) -> tuple[list[str], list[str], list[str]]:
        fields = fields or []
        query = {
            "from": from_,
            "size": size,
            "query": {"match_all": {}},
            "_source": False,
        }
        if fields:
            query["fields"] = fields

        result = await self.es.search(index=index, body=query)
        documents = OrderedDict()
        for x in result.raw.get("hits", {}).get("hits", {}):
            fields_dict = {}
            for field in fields:
                fields_dict[field] = x.get("fields", {}).get(field, [None])[0]
            documents[x["_id"]] = fields_dict

        return documents
