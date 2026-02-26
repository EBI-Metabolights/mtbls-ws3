import datetime
from typing import Any, OrderedDict

from elasticsearch import AsyncElasticsearch

from mtbls.application.services.interfaces.search_index_management_gateway import (
    IndexClientResponse,
    IndexDocumentInfo,
    SearchIndexManagementGateway,
)
from mtbls.infrastructure.search.es.es_client_config import ElasticsearchClientConfig


class EsIndexManagementGateway(SearchIndexManagementGateway):
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
            request_timeout=self._config.request_timeout,
            verify_certs=self._config.verify_certs,
        )

    async def search(
        self, index, body: dict[str, Any], **kwargs
    ) -> IndexClientResponse:
        result = await self.es.search(index=index, body=body, **kwargs)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def create_index(
        self,
        index: str,
        mappings: None | dict[str, Any],
        settings: None | dict[str, Any],
        delete_before: bool = False,
        max_retries: int = 1,
        **kwargs,
    ) -> IndexClientResponse:
        if delete_before:
            await self.delete_index(index=index)
        result = await self.es.options(
            max_retries=max_retries,
        ).indices.create(index=index, mappings=mappings, settings=settings, **kwargs)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def delete_index(self, index: str, **kwargs) -> IndexClientResponse:
        result = await self.es.options(
            ignore_status=404, request_timeout=30, retry_on_timeout=True
        ).indices.delete(index=index, **kwargs)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def exists(self, index: str, **kwargs) -> bool:
        result = await self.es.indices.exists(index=index, **kwargs)
        return result.raw

    async def get_document(self, index: str, id: str) -> dict[str, Any]:
        result = await self.es.get(index=index, id=id)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def bulk(self, index: str, operations: Any, **kwargs) -> IndexClientResponse:
        return await self.es.bulk(index=index, operations=operations, **kwargs)

    async def index_document(
        self, index: str, id: str, body: dict[str, Any], **kwargs
    ) -> IndexClientResponse:
        result = await self.es.index(index=index, id=id, body=body, **kwargs)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def delete_document(self, index: str, id: str) -> IndexClientResponse:
        result = await self.es.options(ignore_status=404).indices.delete(
            index=index, id=id
        )
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def delete_by_query(
        self, index: str, body: dict[str, Any] = False, **kwargs
    ) -> IndexClientResponse:
        result = await self.es.delete_by_query(
            index=index, body=body, refresh=True, **kwargs
        )
        return IndexClientResponse.model_validate(result, from_attributes=True)

    async def delete_by_id(self, index: str, id: str, **kwargs) -> IndexClientResponse:
        result = await self.es.delete(index=index, id=id, refresh=True, **kwargs)
        return IndexClientResponse.model_validate(result, from_attributes=True)

    # async def load_file(self, file_path: pathlib.Path):
    #     async with aiofiles.open(file_path, "rb") as file:
    #         contents = await file.read()
    #         json_file = json.loads(contents)
    #         return json_file

    async def get_document_ids(
        self,
        index: str,
        update_field_name: None | str,
        from_: int = 0,
        size: int = 20000,
    ) -> list[IndexDocumentInfo]:
        update_time_fields = [update_field_name] if update_field_name else None
        documents = await self.get_all_documents_with_fields(
            index=index, fields=update_time_fields, from_=from_, size=size
        )
        return [
            IndexDocumentInfo(
                id=x, updated_at=datetime.datetime.fromisoformat(x[update_field_name])
            )
            for x in documents
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
