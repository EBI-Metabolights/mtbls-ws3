import asyncio

from mtbls.application import get_worker_loop, set_worker_loop


def run_coroutine(coroutine):
    try:
        loop = get_worker_loop()
        if not loop:
            loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            result = asyncio.ensure_future(coroutine)
        else:
            result = loop.run_until_complete(coroutine)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        set_worker_loop(loop)
        result = loop.run_until_complete(coroutine)
    return result
