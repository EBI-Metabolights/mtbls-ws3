import asyncio

_loop = None


def set_worker_loop(loop: asyncio.AbstractEventLoop):
    global _loop
    _loop = loop


def get_worker_loop() -> asyncio.AbstractEventLoop:
    global _loop
    return _loop
