import logging
from typing import Any, Dict, List, Optional, Self

from elasticsearch import ApiError, AsyncElasticsearch
from pydantic import BaseModel, Field, model_validator

from mtbls.application.services.interfaces.data_index_client import DataIndexClient

logger = logging.getLogger(__name__)


class ElasticsearchClientConfig(BaseModel):
    hosts: List[str] | str = Field(
        default_factory=list, description="List of Elasticsearch host URLs"
    )
    api_key: Optional[str] = Field(
        None, description="API key for Elasticsearch authentication"
    )
    request_timeout: Optional[float] = Field(
        5.0, description="Request timeout in seconds"
    )
    verify_certs: bool = Field(
        default=True, description="Verify SSL certificates for HTTPS connections"
    )
    port: int
    username: str
    password: str

    @staticmethod
    def append_port_to_hosts(hosts, port) -> str:
        if not hosts or port in (None, "", 0):
            return hosts

        try:
            port_str = str(int(port))
        except (TypeError, ValueError):
            return hosts

        def _format(host: str) -> str:
            if not host:
                return host
            host_str = str(host)
            scheme_split = host_str.split("://", 1)
            host_body = scheme_split[-1]
            if ":" in host_body:
                return host_str  # already has a port
            return f"{host_str}:{port_str}"

        if isinstance(hosts, (list, tuple)):
            return [_format(h) for h in hosts]
        return _format(hosts)

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Self:
        if isinstance(v, ElasticsearchClientConfig):
            v.hosts = ElasticsearchClientConfig.append_port_to_hosts(v.hosts, v.port)
        elif isinstance(v, dict):
            v["hosts"] = ElasticsearchClientConfig.append_port_to_hosts(
                v.get("hosts"), v.get("port")
            )

        return handler(v)


class ElasticsearchClient(DataIndexClient):
    def __init__(
        self,
        config: None | ElasticsearchClientConfig | dict[str, Any],
        auth_method: None | str = None,
        api_key: None | str = None,
    ):
        self._config = config
        self.auth_method = auth_method
        self.api_key = api_key
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
        basic_auth = None
        if self.auth_method == "basic_auth":
            basic_auth = (
                self._config.username,
                self._config.password,
            )

            self._es = AsyncElasticsearch(
                hosts=self._config.hosts or None,
                basic_auth=basic_auth,
                # request_timeout=self._config.request_timeout,
                request_timeout=60,  # default is often too small for heavy ops
                max_retries=3,
                retry_on_timeout=True,
                verify_certs=self._config.verify_certs,
            )
        else:
            self._es = AsyncElasticsearch(
                hosts=self._config.hosts or None,
                api_key=self.api_key,
                request_timeout=self._config.request_timeout,
                verify_certs=self._config.verify_certs,
            )
        try:
            ok = await self._es.info(human=True, pretty=True)
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

    async def search(self, index, body: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.search(index=index, body=body, **kwargs)

    # no current usecase for multiple search, but adding for completeness / the future.
    async def msearch(self, index, body: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.msearch(index=index, body=body, **kwargs)

    async def count(self, index, body: Optional[Dict[str, Any]]) -> int:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        resp = await self._es.count(index=index, body=body or {})
        return int(resp.get("count", 0))

    async def get_info(self) -> Dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.info()

    async def get_mapping(self, index: str) -> Dict[str, Any]:
        await self.ensure_started()
        assert self._es is not None, (
            "Elasticsearch client not connected. Has start() been called?"
        )
        return await self._es.indices.get_mapping(index=index)

    async def bulk(self, index: str, operations: Any, **kwargs) -> dict[str, Any]:
        await self.ensure_started()
        return await self._es.bulk(index=index, operations=operations, **kwargs)

    async def delete(
        self, index: str, ignore_status: bool = False, **kwargs
    ) -> dict[str, Any]:
        await self.ensure_started()
        return await self._es.options(ignore_status=ignore_status).indices.delete(
            index=index, **kwargs
        )

    async def exists(self, index: str, **kwargs) -> bool:
        await self.ensure_started()
        result = await self._es.indices.exists(index=index, **kwargs)
        return result.raw

    async def delete_by_query(
        self, index: str, body: dict[str, Any] = False, **kwargs
    ) -> dict[str, Any]:
        await self.ensure_started()
        return await self._es.delete_by_query(
            index=index, body=body, refresh=True, **kwargs
        )

    async def delete_by_id(self, index: str, id: str, **kwargs) -> dict[str, Any]:
        await self.ensure_started()
        return await self._es.delete(index=index, id=id, refresh=True, **kwargs)

    async def create(
        self, index: str, mappings: dict[str, Any], max_retries: int = 1, **kwargs
    ) -> dict[str, Any]:
        await self.ensure_started()
        return await self._es.options(max_retries=max_retries).indices.create(
            index=index, mappings=mappings, **kwargs
        )
