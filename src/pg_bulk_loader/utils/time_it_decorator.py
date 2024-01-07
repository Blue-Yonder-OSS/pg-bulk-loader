import asyncio
import functools
import time
from contextlib import contextmanager


def time_it(func):

    @contextmanager
    def wrapping_logic():
        start_time = time.time()
        yield
        print(f'Function {func.__name__} executed in {(time.time() - start_time):.4f}s')

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not asyncio.iscoroutinefunction(func):
            with wrapping_logic():
                return func(*args, **kwargs)
        else:
            async def async_func():
                with wrapping_logic():
                    return await func(*args, **kwargs)
            return async_func()
    return wrapper
