from typing import Optional, List

from pydantic import BaseModel, Field


class MongoDbConnection(BaseModel):
    url_scheme: str = "mongodb"
    host: str = ""
    port: int = -1
    hosts: List[str] = Field(default_factory=list)  # optional: host:port entries for replica sets
    user: str = ""
    password: str = ""
    database: str = ""
    auth_source: Optional[str] = "admin"
    replica_set: Optional[str] = None
    tls: Optional[bool] = None

    def build_uri(self, mask_password: bool = False) -> str:
        """
        Build a Mongo connection URI.
        - Supports single host/port or a list of host:port strings for replica sets.
        - Adds replicaSet/tls/authSource query params when provided.
        """
        effective_scheme = self.url_scheme or "mongodb"

        auth_part = ""
        if self.user and self.password:
            pwd = "***" if mask_password else self.password
            auth_part = f"{self.user}:{pwd}@"

        # Choose hosts: explicit list wins; fallback to single host/port
        if self.hosts:
            host_list = self.hosts
        else:
            # Allow comma-separated hosts in the single host field for backward compatibility
            if "," in self.host:
                host_list = [h.strip() for h in self.host.split(",") if h.strip()]
            else:
                base = self.host if self.port is None or self.port < 0 else f"{self.host}:{self.port}"
                host_list = [base]
        hosts_str = ",".join(host_list)

        db_suffix = f"/{self.database}" if self.database else ""

        query_params = []
        if self.auth_source:
            query_params.append(f"authSource={self.auth_source}")
        if self.replica_set:
            query_params.append(f"replicaSet={self.replica_set}")
        if self.tls is not None:
            query_params.append(f"tls={'true' if self.tls else 'false'}")

        query_suffix = f"?{'&'.join(query_params)}" if query_params else ""

        return f"{effective_scheme}://{auth_part}{hosts_str}{db_suffix}{query_suffix}"


class MongoDbConfiguration(BaseModel):
    connection: MongoDbConnection = MongoDbConnection()
