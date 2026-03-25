import logging
from typing import Any, Dict, List, Optional, Self

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


class ElasticsearchClientConfig(BaseModel):
    hosts: List[str] | str = Field(
        default_factory=list, description="List of Elasticsearch host URLs"
    )
    api_keys: Optional[Dict[str, str]] = Field(
        default=None,
        description="Named API keys for Elasticsearch authentication",
    )
    username: Optional[str] = Field(
        default=None, description="Username for basic authentication"
    )
    password: Optional[str] = Field(
        default=None, description="Password for basic authentication"
    )
    request_timeout: Optional[float] = Field(
        15.0, description="Request timeout in seconds"
    )
    verify_certs: bool = Field(
        default=True, description="Verify SSL certificates for HTTPS connections"
    )
    port: int

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
