"""Tests for the db_util module"""

from databases import Database
import pytest

from pynocular.util import is_database_available


@pytest.mark.asyncio
async def test_is_database_available(postgres_database: Database) -> None:
    """Test successful database connection"""
    available = await is_database_available(str(postgres_database.url))
    assert available is True


@pytest.mark.asyncio
async def test_is_database_not_available(postgres_database: Database) -> None:
    """Test db connection unavailable"""
    invalid_connection_string = str(postgres_database.url.replace(database="INVALID"))
    available = await is_database_available(invalid_connection_string)
    assert available is False
