"""Contains contextvar and helper functions to manage the active database backend"""

from contextlib import contextmanager
import contextvars
import logging

from .base import DatabaseModelBackend

logger = logging.getLogger("pynocular")
_backend = contextvars.ContextVar("database_model_backend", default=None)


@contextmanager
def set_backend(backend: DatabaseModelBackend) -> None:
    """Set the database backend in the aio context

    Args:
        backend: Database backend instance

    """
    logger.debug("Setting backend")
    token = _backend.set(backend)
    try:
        yield
    finally:
        _backend.reset(token)
    logger.debug("Reset backend")


def get_backend() -> DatabaseModelBackend:
    """Get the currently active database backend

    Returns:
        database backend instance

    """
    return _backend.get()
