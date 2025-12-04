import logging
from typing import Annotated, Any, Optional

from elasticsearch import ApiError, AsyncElasticsearch
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ElasticsearchClientConfig(BaseModel):
    hosts: Annotated[
        list[str] | str,
        Field(default_factory=list, description="List of Elasticsearch host URLs"),
    ]
    api_key: Annotated[
        Optional[str],
        Field(None, description="API key for Elasticsearch authentication"),
    ]
    request_timeout: Annotated[
        Optional[float], Field(5.0, description="Request timeout in seconds")
    ] = None
    verify_certs: Annotated[
        bool,
        Field(
            default=True, description="Verify SSL certificates for HTTPS connections"
        ),
    ] = False


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
        logger.info(
            "Connecting to Elasticsearch hosts: %s (timeout=%s, verify_certs=%s)",
            self._config.hosts,
            self._config.request_timeout,
            self._config.verify_certs,
        )
        self._es = AsyncElasticsearch(
            hosts=self._config.hosts or None,
            api_key=self._config.api_key,
            request_timeout=self._config.request_timeout,
            verify_certs=self._config.verify_certs,
        )
        try:
            ok = await self._es.ping()
            if not ok:
                logger.exception(
                    "Elasticsearch hosts %s reachable but ping returned False.",
                    self._config.hosts,
                )
                raise ConnectionError("Elasticsearch ping failed")
            logger.info("Elasticsearch connection established successfully.")
        except ApiError as e:
            logger.exception("Elasticsearch API error during startup: %s", e)
            raise RuntimeError(f"Elasticsearch connection error: {e}") from e
        except Exception as exc:
            logger.exception("Unexpected Elasticsearch connection failure: %s", exc)
            raise

    async def ensure_started(self) -> None:
        if self._es is None:
            await self.start()

    async def close(self) -> None:
        if self._es is not None:
            await self._es.close()
            self._es = None

    async def search(self, index, body: dict[str, Any]) -> dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.search(index=index, body=body)

    # no current usecase for multiple search, but adding for completeness / the future.
    async def msearch(self, index, body: dict[str, Any]) -> dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.msearch(index=index, body=body)

    async def count(self, index, body: Optional[dict[str, Any]]) -> int:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        resp = await self._es.count(index=index, body=body or {})
        return int(resp.get("count", 0))

    async def get_info(self) -> dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.info()

    async def get_mapping(self, index: str) -> dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.indices.get_mapping(index=index)
