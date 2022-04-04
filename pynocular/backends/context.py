"""Contains contextvar definition for the active database backend"""

from contextlib import contextmanager

import aiocontextvars as contextvars

from .base import DatabaseModelBackend


_backend = contextvars.ContextVar("database_model_backend", default=None)


@contextmanager
def backend(backend: DatabaseModelBackend) -> None:
    """Set the database backend in the aio context

    Args:
        backend: Database backend instance

    """
    token = _backend.set(backend)
    try:
        yield
    finally:
        _backend.reset(token)


def get_backend() -> DatabaseModelBackend:
    """Get the currently active database backend

    Returns:
        database backend instance

    """
    return _backend.get()
