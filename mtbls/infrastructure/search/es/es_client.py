import logging
from typing import Any, Dict, List, Optional

from elasticsearch import ApiError, AsyncElasticsearch
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ElasticsearchClientConfig(BaseModel):
    hosts: List[str] | str = Field(
        default_factory=list, description="List of Elasticsearch host URLs"
    )
    api_key: Optional[str] = Field(
        None, description="Deprecated single API key for Elasticsearch authentication"
    )
    api_keys: Optional[Dict[str, str]] = Field(
        default=None,
        description="Named API keys for Elasticsearch authentication",
    )
    request_timeout: Optional[float] = Field(
        5.0, description="Request timeout in seconds"
    )
    verify_certs: bool = Field(
        default=True, description="Verify SSL certificates for HTTPS connections"
    )


class ElasticsearchClient:
    def __init__(self, config: None | ElasticsearchClientConfig | dict[str, Any]):
        self._config = config
        if not self._config:
            self._config = ElasticsearchClientConfig()
        elif isinstance(self._config, dict):
            self._config = ElasticsearchClientConfig.model_validate(config)
        self._clients: Dict[Optional[str], AsyncElasticsearch] = {}

    def _configured_api_key_names(self) -> List[Optional[str]]:
        if self._config.api_keys:
            return list(self._config.api_keys.keys())
        return [None]

    def _effective_api_key_name(self, api_key_name: Optional[str]) -> Optional[str]:
        if api_key_name is not None:
            return api_key_name
        if self._config.api_keys:
            return next(iter(self._config.api_keys.keys()))
        return None

    def _resolve_api_key_value(self, api_key_name: Optional[str]) -> Optional[str]:
        if api_key_name:
            if self._config.api_keys and api_key_name in self._config.api_keys:
                return self._config.api_keys[api_key_name]
            raise ValueError(
                f"API key '{api_key_name}' is not configured; "
                f"available keys: {list(self._config.api_keys or {})}"
            )

        if self._config.api_keys:
            return next(iter(self._config.api_keys.values()))
        return self._config.api_key

    async def start(self, api_key_name: Optional[str] = None) -> None:
        target_keys = (
            [api_key_name]
            if api_key_name is not None
            else self._configured_api_key_names()
        )

        for key_name in target_keys:
            if key_name in self._clients:
                continue

            api_key_value = self._resolve_api_key_value(key_name)
            logger.info(
                "Connecting to Elasticsearch hosts: %s (using configured API key, timeout=%s, verify_certs=%s)",
                self._config.hosts,
                self._config.request_timeout,
                self._config.verify_certs,
            )
            es = AsyncElasticsearch(
                hosts=self._config.hosts or None,
                api_key=api_key_value,
                request_timeout=self._config.request_timeout,
                verify_certs=self._config.verify_certs,
            )
            try:
                ok = await es.ping()
                if not ok:
                    # Likely a restricted API key without cluster privileges; keep client and proceed.
                    logger.warning(
                        "Elasticsearch ping failed; proceeding anyway (restricted key or connectivity issue)."
                    )
                self._clients[key_name] = es
                if ok:
                    logger.info(
                        "Elasticsearch connection established successfully using a configured API key."
                    )
            except ApiError as e:
                status = getattr(e, "status_code", None)
                if status in (401, 403):
                    # Restricted API key cannot ping cluster; keep client and continue.
                    logger.warning(
                        "Elasticsearch ping unauthorized for a configured API key (status=%s); continuing.",
                        status,
                    )
                    self._clients[key_name] = es
                    continue
                await es.close()
                logger.exception("Elasticsearch API error during startup: %s", e)
                raise RuntimeError(f"Elasticsearch connection error: {e}") from e
            except Exception as exc:
                await es.close()
                logger.exception(
                    "Unexpected Elasticsearch connection failure: %s", exc
                )
                raise

    async def ensure_started(self, api_key_name: Optional[str] = None) -> None:
        target_keys = (
            [api_key_name]
            if api_key_name is not None
            else self._configured_api_key_names()
        )
        for key_name in target_keys:
            if key_name in self._clients:
                continue
            await self.start(key_name)

    async def _get_started_client(
        self, api_key_name: Optional[str]
    ) -> AsyncElasticsearch:
        effective_name = self._effective_api_key_name(api_key_name)
        await self.ensure_started(effective_name)
        assert effective_name in self._clients, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return self._clients[effective_name]

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()
        self._clients.clear()

    async def search(
        self, index, body: Dict[str, Any], api_key_name: Optional[str] = None
    ) -> Dict[str, Any]:
        client = await self._get_started_client(api_key_name)
        return await client.search(index=index, body=body)

    # no current usecase for multiple search, but adding for completeness / the future.
    async def msearch(
        self, index, body: Dict[str, Any], api_key_name: Optional[str] = None
    ) -> Dict[str, Any]:
        client = await self._get_started_client(api_key_name)
        return await client.msearch(index=index, body=body)

    async def count(
        self, index, body: Optional[Dict[str, Any]], api_key_name: Optional[str] = None
    ) -> int:
        client = await self._get_started_client(api_key_name)
        resp = await client.count(index=index, body=body or {})
        return int(resp.get("count", 0))

    async def get_info(self, api_key_name: Optional[str] = None) -> Dict[str, Any]:
        client = await self._get_started_client(api_key_name)
        return await client.info()

    async def get_mapping(
        self, index: str, api_key_name: Optional[str] = None
    ) -> Dict[str, Any]:
        client = await self._get_started_client(api_key_name)
        return await client.indices.get_mapping(index=index)
