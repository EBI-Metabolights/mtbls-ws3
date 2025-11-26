# Caching

There are different level caching:

- Method level caching: Method caching means storing results of function/method calls so that repeated calls with the same arguments are fast. Benefits.
  - Avoid recomputing expensive operations
  - Speed up repeated lookups
  - Reduce I/O or computation time
- Service (local application) level caching: Caching data across the local service, not just a single method.
- Application (External distributed) level caching: Caching data across the entire app, not just a single method or local service.

### General design and coding principles

- Use python lru_cache method, cachetools & cachetools_async libraries to implement singleton pattern.

```python
# cache async method with TTLCache
from cachetools import TTLCache
from cachetools_async import cached

@cached(cache=TTLCache(maxsize=1, ttl=600))
async def get_version_info():
    return default_response
```

```python
# cache sync method with TTLCache
from cachetools import TTLCache, cached

@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_version_info():
    return default_response
```

- Use cachetools and cachetools_async libraries with TTLCache to cache methods calling external dependencies and services.
  e.g., OLS search, CHEBI search, datamover worker cache, elasticsearch search, policy service endpoints, etc.

```python
# cache a service or external dependency results periodically

from cachetools import TTLCache
from cachetools_async import cached

@cached(cache=TTLCache(maxsize=10, ttl=60))
async def get_rule_definitions(self) -> dict[str, Any]:
    return await self.get_http_response(self.config.rule_definitions_url, "result")

```

- Use redis for application level caching. Assign timeout while updating cache data.

```python

async def start_study_validation_task(  # noqa: PLR0913
    resource_id: str,
    async_task_service: AsyncTaskService,
    cache_service: CacheService,
    apply_modifiers: bool = False,
    override_ready_task_results: bool = False,
    cache_expiration_in_seconds: int = 10 * 60,
) -> AsyncTaskStatus:
    key = f"validation_task:current:{resource_id}"
    task_id = await cache_service.get_value(key)

    await cache_service.set_value(
        key=key,
        value=task_id,
        expiration_time_in_seconds=cache_expiration_in_seconds,
    )
```
