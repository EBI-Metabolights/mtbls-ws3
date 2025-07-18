# Logging

Logging mechanism is based on Python logging module. It is extended to implement the following logging requirements:

- Each log will be traceable to identify time, requester, source IP, endpoint and requested resource (MTBLS, REQ)
- Each log has a request id to find all logs triggered by the same request.
- Each async task will log with the task id.
- Logs will not contain personal credentials or PII (Personally Identifiable Information)


### General design and coding principles
- Each module uses its own logger  (logger with `__name__` ) to log its state and progress.
- Use logging module instead of `print` lines.
- Prefer logging statement deferred string formatting instead of f-string or string format.

```Python hl_lines="3 8"
import logging

logger = logging.getLogger(__name__)

index = 1
value = "data"

logger.error("Error at %s: %s", index, value)

# instead of
logger.error(f"Error at {index}: {value}")
# or
logger.error("Error at {}: {}".format(index, value))

```
/// hint

These design principles will be checked with linter automatically.

///

## Logging configuration
Each application can have custom logging configuration and logging filter classes.

Logging configurations have both `json` and `text` formatters. Each formatter can also print some additional information for each log:

- `resource_id` to track requested study (MTBLS or REQ id)
- `user_id` to track authenticated user. It is unique user id in database
- `route path` to track target endpoint.
- `request_id` to track all logs triggered by the same request.
- `task_id` to track all task logs triggered by the same task
- `client` to track IP address or host of the requester

An example logging configuration is below:

```yaml hl_lines="5-8 29"
logging:
  version: 1
  disable_existing_loggers: true
  formatters:
    json_formatter:
      format: '{ "level_name": "%(levelname)s", "time": "%(asctime)s",  "client": "%(client)s",  "path": "%(route_path)s", "resource_id": "%(resource_id)s", "user": %(user_id)d, "request_id": "%(request_id)s", "name": "%(name)s", "message": "%(message)s" }'
    text_formatter:
      format: '%(levelname)-8s %(asctime)s %(user_id)d %(client)s %(route_path)s %(resource_id)s %(request_id)s %(name)s "%(message)s"'
  handlers:
    console:
      class: "logging.StreamHandler"
      level: DEBUG
      formatter: "text_formatter"
      stream: "ext://sys.stdout"
      filters: [ default_filter, correlation_id ]
  root:
    level: DEBUG
    handlers: [ "console" ]
    propogate: true
  loggers:
    mtbls:
      level: DEBUG
      propogate: yes
  filters:
    correlation_id:
      (): "asgi_correlation_id.CorrelationIdFilter"
      default_value: "-"
    default_filter:
      (): "mtbls.run.rest_api.submission.log_filter.DefaultLogFilter"
```


Example JSON log

```json
{ "level_name": "DEBUG", "time": "2024-12-22 18:39:02,218",  "client": "127.0.0.1",  "path": "/auth/v1/token", "resource_id": "-", "user": 0, "request_id": "60335885-b4a0-486b-bc77-2bfa52ebaf2f", "name": "mtbls.presentation.rest_api.core.authorization_middleware", "message": "Unauthenticated user requests POST /auth/v1/token from host/IP 127.0.0.1." }
{ "level_name": "INFO", "time": "2024-12-22 18:39:02,239",  "client": "127.0.0.1",  "path": "/auth/v1/token", "resource_id": "-", "user": 0, "request_id": "60335885-b4a0-486b-bc77-2bfa52ebaf2f", "name": "mtbls.infrastructure.auth.mtbls_ws2.mtbls_ws2_authentication_proxy", "message": "Login request from: help@ebi.ac.uk" }
{ "level_name": "INFO", "time": "2024-12-22 18:39:02,370",  "client": "127.0.0.1",  "path": "/auth/v1/token", "resource_id": "-", "user": 0, "request_id": "60335885-b4a0-486b-bc77-2bfa52ebaf2f", "name": "uvicorn.access", "message": "127.0.0.1:51279 - "POST /auth/v1/token HTTP/1.1" 200" }
{ "level_name": "DEBUG", "time": "2024-12-22 18:39:16,340",  "client": "127.0.0.1",  "path": "/submissions/v1/investigation-files/MTBLS60", "resource_id": "MTBLS60", "user": 25432, "request_id": "54eb7ab4-675c-4ee7-8637-819fdf876105", "name": "mtbls.presentation.rest_api.core.authorization_middleware", "message": "User 25432 requests GET /submissions/v1/investigation-files/MTBLS60 from host/IP 127.0.0.1. Target resource id: MTBLS60" }
{ "level_name": "DEBUG", "time": "2024-12-22 18:39:16,340",  "client": "127.0.0.1",  "path": "/submissions/v1/investigation-files/MTBLS60", "resource_id": "MTBLS60", "user": 25432, "request_id": "54eb7ab4-675c-4ee7-8637-819fdf876105", "name": "mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies", "message": "User 25432 is granted to update resource MTBLS60" }
{ "level_name": "INFO", "time": "2024-12-22 18:39:16,890",  "client": "127.0.0.1",  "path": "/submissions/v1/investigation-files/MTBLS60", "resource_id": "MTBLS60", "user": 25432, "request_id": "54eb7ab4-675c-4ee7-8637-819fdf876105", "name": "uvicorn.access", "message": "127.0.0.1:51283 - "GET /submissions/v1/investigation-files/MTBLS60 HTTP/1.1" 200" }
```


A `RequestTracker` object is created for each request automatically. Custom logging filters defined in logging configuration inject additional information using the `RequestTracker` object for the request.

```Python hl_lines="16-24"

from logging import Filter, LogRecord

from mtbls.application.context.request_tracker import (
    RequestTracker,
    get_request_tracker,
)

class DefaultLogFilter(Filter):
    #...

    def filter(
        self,
        record: LogRecord,
    ) -> bool:
        #...
        context_vars = get_request_tracker()
        if context_vars and isinstance(context_vars, RequestTracker):
            model = context_vars.get_request_tracker_model()
            record.user_id = model.user_id
            record.route_path = model.route_path
            record.resource_id = model.resource_id
            record.client = model.client
            record.request_id = model.request_id
            record.task_id = model.task_id
        else:
            record.user_id = 0
            record.route_path = "-"
            record.resource_id = "-"
            record.client = "-"
            record.request_id = "-"
            record.task_id = "-"

        return True

```

#### Management of additional logging information

- REST API executables: `AuthorizationMiddleware` updates additional information.

- Async Task Workers:
    - Async task executor adds `request_tracker` parameter with `RequestTracker` data.
    - Before starting async task execution on remote worker, `request_tracker` parameter is checked and updates additional information.

```Python hl_lines="11-12 15-19"
class CeleryAsyncTaskExecutor(AsyncTaskExecutor):
    def __init__(
        self, task_method: Callable, task_name: str, id_generator: IdGenerator, **kwargs
    ):
        self.task_method = task_method
        self.kwargs = kwargs
        self.task_name = task_name
        self.id_generator = id_generator

    async def start(self, expires: Union[None, int] = None) -> AsyncTaskResult:
        request_tracker = get_request_tracker().get_request_tracker_model().model_dump()
        self.kwargs["request_tracker"] = request_tracker
        if self.id_generator:
            task_id = self.id_generator.generate_unique_id()
            task = self.task_method.apply_async(
                expires=expires,
                kwargs=self.kwargs,
                task_id=task_id,
            )
        else:
            task = self.task_method.apply_async(expires=expires, kwargs=self.kwargs)
        logger.info("Task '%s' is created.", self.task_name)
        return CeleryAsyncTaskResult(task)
```

```Python hl_lines="6-9"
class CeleryBaseTask(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error("%s failed: %s", task_id, str(exc))

    def before_start(self, task_id, args, kwargs):
        request_tracker = get_request_tracker()
        request_tracker.task_id_var.set(task_id)
        if "request_tracker" in kwargs:
            request_tracker_dict = kwargs["request_tracker"]
```
