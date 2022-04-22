"""Contains shared functional test fixtures"""

import asyncio
import logging
import os

from databases import Database
import pytest

from pynocular.util import create_new_database

logger = logging.getLogger("pynocular")


@pytest.fixture(scope="session")
def event_loop():
    """Returns the event loop so we can define async, session-scoped fixtures"""
    return asyncio.get_event_loop()


@pytest.fixture(scope="session")
async def postgres_database():
    """Fixture that manages a Postgres database fixture

    Yields:
        postgres database

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
        logger.info(str(e))

    database = Database(db_connection_string, timeout=5, command_timeout=5)
    await database.connect()
    try:
        yield database
    except Exception as e:
        logger.info(str(e))
    finally:
        logger.debug("Disconnecting")
        await asyncio.wait_for(database.disconnect(), 2)

    try:
        logger.debug(f"Dropping {test_db_name}")
        async with Database(maintenance_connection_string) as db:
            await db.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        logger.debug(f"Dropped {test_db_name}")
    except Exception as e:
        logger.info(str(e))
