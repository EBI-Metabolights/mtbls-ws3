

from typing import Any, Dict, List, Optional

from elasticsearch import ApiError, AsyncElasticsearch
from pydantic import BaseModel, Field

class ElasticsearchClientConfig(BaseModel):
    hosts: List[str] | str = Field(default_factory=list, description="List of Elasticsearch host URLs")
    api_key: Optional[str] = Field(None, description="API key for Elasticsearch authentication")    
    request_timeout: Optional[float] = Field(5.0, description="Request timeout in seconds")
    verify_certs: bool = Field(default=True, description="Verify SSL certificates for HTTPS connections")
    
    
class ElasticsearchClient:
    def __init__(self, config: None | ElasticsearchClientConfig | dict[str, Any]):
        self._config = config
        if not self._config:
            self._config = ElasticsearchClientConfig()
        elif isinstance(self._config, dict):
            self._config = ElasticsearchClientConfig.model_validate(config)
        self._es: Optional[AsyncElasticsearch] = None
    
    async def start(self) -> None:
        if self._es is not None:
            return  # Already started
        print(f"current hosts value: {self._config.hosts}  ")
        self._es = AsyncElasticsearch(
            hosts=self._config.hosts or None,
            api_key=self._config.api_key,
            request_timeout=self._config.request_timeout,
            verify_certs=self._config.verify_certs,
        )
        try: 
            ok = await self._es.ping()
            if not ok:
                raise ConnectionError("Elasticsearch ping failed")
        except ApiError as e:
            raise RuntimeError(f"Elasticsearch connection error: {e}") from e
    
    async def close(self) -> None:
        if self._es is not None:
            await self._es.close()
            self._es = None
    
    async def search(self, index, body: Dict[str, any]) -> Dict[str, any]:
        assert self._es is not None, "Elasticsearch client not connected. Has start() been called?"
        return await self._es.search(index=index, body=body)
    
    # no current usecase for multiple search, but adding for completeness / the future.
    async def msearch(self, index, body: Dict[str, any]) -> Dict[str, any]:
        assert self._es is not None, "Elasticsearch client not connected. Has start() been called?"
        return await self._es.msearch(index=index, body=body)

    async def count(self, index, body: Optional[Dict[str, any]]) -> int:
        assert self._es is not None, "Elasticsearch client not connected. Has start() been called?"
        resp = await self._es.count(index=index, body=body or {})
        return int(resp.get("count", 0))
    
    async def get_info(self) -> Dict[str, any]:
        assert self._es is not None, "Elasticsearch client not connected. Has start() been called?"
        return await self._es.info()
