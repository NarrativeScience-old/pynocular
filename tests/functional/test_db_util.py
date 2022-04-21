"""Tests for the db_util module"""
import logging
import os

from databases import Database
import pytest

from pynocular.db_util import create_new_database, is_database_available

db_user_password = str(os.environ.get("DB_USER_PASSWORD"))


@pytest.fixture(scope="module")
async def test_connection_string():
    """Fixture that yields a test connection string

    Yields:
        postgres connection string

    """
    db_host = os.environ.get("DB_HOST", "localhost")
    db_user_name = os.environ.get("DB_USER_NAME", os.environ.get("USER", "postgres"))
    db_user_password = os.environ.get("DB_USER_PASSWORD", "")
    test_db_name = os.environ.get("TEST_DB_NAME", "test_db")

    maintenance_connection_string = f"postgres://{db_user_name}:{db_user_password}@{db_host}:5432/postgres?sslmode=disable"
    db_connection_string = f"postgresql://{db_user_name}:{db_user_password}@{db_host}:5432/{test_db_name}?sslmode=disable"

    try:
        await create_new_database(maintenance_connection_string, test_db_name)
    except Exception as e:
        # If this fails, assume its already created
        logging.info(str(e))

    yield db_connection_string

    async with Database(maintenance_connection_string) as db:
        try:
            await db.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        except Exception as e:
            logging.info(str(e))


@pytest.mark.asyncio
async def test_is_database_available(test_connection_string) -> None:
    """Test successful database connection"""
    available = await is_database_available(test_connection_string)
    assert available is True


@pytest.mark.asyncio
async def test_is_database_not_available() -> None:
    """Test db connection unavailable"""
    invalid_connection_string = f"postgresql://postgres:{db_user_password}@localhost:5432/INVALID?sslmode=disable"
    available = await is_database_available(invalid_connection_string)
    assert available is False
