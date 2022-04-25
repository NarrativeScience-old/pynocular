"""Lightweight ORM that lets you query your database using Pydantic models and asyncio"""

__version__ = "2.0.0-rc2"

from pynocular.backends.context import get_backend, set_backend
from pynocular.backends.memory import MemoryDatabaseModelBackend
from pynocular.backends.sql import Database, SQLDatabaseModelBackend
from pynocular.database_model import DatabaseModel
from pynocular.util import UUID_STR
