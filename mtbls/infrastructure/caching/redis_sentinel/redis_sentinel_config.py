from typing import List

from pydantic import BaseModel

from mtbls.infrastructure.caching.redis.redis_config import RedisService


class RedisSentinelConnection(BaseModel):
    master_name: str = "master-redis-ws"
    password: str = ""
    db: int = 10
    sentinel_services: List[RedisService] = []
    socket_timeout: float = 0.5
