from mtbls.application.decorators.async_task import async_task


@async_task(queue="common")
def ping_connection(
    data: str = "ping",
    **kwargs,
) -> str:
    if data == "ping":
        return "pong"
    return data
