from contextlib import contextmanager
from functools import partial
from typing import Any, Type

import aiocontextvars as contextvars

from .base import DatabaseModelBackend


_backend = contextvars.ContextVar("transaction_connections", default=None)


@contextmanager
def backend(backend_cls: Type[DatabaseModelBackend], **kwargs: Any) -> None:
    token = _backend.set(partial(backend_cls, **kwargs))
    try:
        yield
    finally:
        _backend.reset(token)


def get_backend(model_cls: Any) -> DatabaseModelBackend:
    return _backend.get()(model_cls)
