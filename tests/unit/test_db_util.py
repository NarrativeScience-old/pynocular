"""Tests for the db_util module"""
import os

import pytest

from pynocular.db_util import is_database_available
from pynocular.engines import DatabaseType, DBInfo

db_user_password = str(os.environ.get("DB_USER_PASSWORD"))
test_db_name = str(os.environ.get("TEST_DB_NAME", "test_db"))
test_connection_string = str(
    os.environ.get(
        "TEST_DB_CONNECTION_STRING",
        f"postgresql://postgres:{db_user_password}@localhost:5432/{test_db_name}?sslmode=disable",
    )
)
test_db = DBInfo(test_connection_string)


class TestDBUtil:
    """Test cases for DB util functions"""

    @pytest.mark.asyncio
    async def test_is_database_available(self) -> None:
        """Test successful database connection"""
        available = await is_database_available(test_db)
        assert available is True

    @pytest.mark.asyncio
    async def test_is_database_not_available(self) -> None:
        """Test db connection unavailable"""
        invalid_connection_string = f"postgresql://postgres:{db_user_password}@localhost:5432/INVALID?sslmode=disable"
        non_existing_db = DBInfo(DatabaseType.aiopg_engine, invalid_connection_string)
        available = await is_database_available(non_existing_db)
        assert available is False
