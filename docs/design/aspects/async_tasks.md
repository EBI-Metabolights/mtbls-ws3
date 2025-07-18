# Async and Remote Tasks

Async task is an entry point method to run on another server or process. There are two implementations:

* `Celery`: Celery workers process the tasks
* `Threading`: Tasks run in a thread (local development only)


Async tasks should be decorated with `@async_task` and registered before starting application.



```python
modules = find_async_task_modules(app_name=app_name, queue_names=queue_names)
async_task_modules = load_modules(modules, module_config)

```



```python

async_task_service: AsyncTaskService = providers.Singleton(
    CeleryAsyncTaskService,
    broker=gateways.pub_sub_broker,
    backend=gateways.pub_sub_backend,
    app_name="default",
    queue_names=["common", "validation", "datamover", "compute", ""],
    async_task_registry=core.async_task_registry,
)
```
