"""Database utility functions"""

from functools import wraps
import logging
import re
from typing import Any, Coroutine, Generator
from uuid import UUID as stdlib_uuid

from databases.core import Database
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable, DropTable

from pynocular.backends.context import get_backend
from pynocular.exceptions import InvalidSqlIdentifierErr

logger = logging.getLogger("pynocular")


def is_valid_uuid(string: str) -> bool:
    """Check if a string is a valid UUID

    Args:
        string: the string to check

    Returns:
        Whether or not the string is a well-formed UUIDv4

    """
    try:
        stdlib_uuid(string, version=4)
        return True
    except (TypeError, AttributeError, ValueError):
        return False


class UUID_STR(str):
    """A string that represents a UUID4 value"""

    @classmethod
    def __get_validators__(cls) -> Generator:
        """Get the validators for the given class"""
        yield cls.validate

    @classmethod
    def validate(cls, v: Any) -> str:
        """Function to validate the value

        Args:
            v: The value to validate

        """
        if isinstance(v, stdlib_uuid) or (isinstance(v, str) and is_valid_uuid(v)):
            return str(v)
        else:
            raise ValueError("invalid UUID string")


async def is_database_available(connection_string: str) -> bool:
    """Check if the database is available

    Args:
        connection_string: A connection string for the database

    Returns:
        true if the DB exists

    """
    try:
        async with Database(connection_string) as db:
            await db.execute("SELECT 1")
            return True
    except Exception:
        return False


async def create_new_database(connection_string: str, db_name: str) -> None:
    """Create a new database database for testing

    Args:
        connection_string: A connection string for the database
        db_name: the name of the database to create

    """
    async with Database(connection_string) as db:
        # End existing commit
        await db.execute("COMMIT")
        # Create db
        await db.execute(f"DROP DATABASE IF EXISTS {db_name}")
        await db.execute(f"CREATE DATABASE {db_name}")


async def create_table(db: Database, table: sa.Table) -> None:
    """Create table in database

    Args:
        db: an async database connection
        table: The table to create

    """
    await db.execute(CreateTable(table))


async def drop_table(db: Database, table: sa.Table) -> None:
    """Drop table in database

    Args:
        db: an async database connection
        table: The table to create

    """
    logger.debug(f"Dropping table {table.name}")
    await db.execute(DropTable(table, if_exists=True))
    logger.debug(f"Dropped table {table.name}")


async def setup_uuid(db: Database) -> None:
    """Set up UUID support

    Args:
        db: an async database connection

    """
    await db.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')


async def setup_datetime_trigger(db: Database) -> None:
    """Set up created_at/updated_at datetime trigger

    Args:
        db: an async database connection

    """
    await db.execute('CREATE EXTENSION IF NOT EXISTS "plpgsql";')
    await db.execute(
        """
        CREATE OR REPLACE FUNCTION update_timestamp_columns()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.created_at IS NULL THEN
                NEW.created_at = now();
            END IF;

            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        """
    )


async def add_datetime_trigger(db: Database, table: str) -> None:
    """Helper method for adding created_at and updated_at datetime triggers on a table


    Args:
        db: an async database connection
        table: The name of the table to add an edit trigger for

    """
    await setup_datetime_trigger(db)
    await db.execute(
        """
        CREATE TRIGGER update_{table}_timestamps
        BEFORE INSERT OR UPDATE ON {table}
        FOR EACH ROW EXECUTE PROCEDURE update_timestamp_columns();
    """.format(
            table=table
        )
    )


async def remove_datetime_trigger(db: Database, table: str) -> None:
    """Helper method for removing datetime triggers on a table

    Args:
        db: an async database connection
        table: The name of the table to remove a trigger for

    """
    await db.execute(
        "DROP TRIGGER IF EXISTS update_{table}_timestamps on {table}".format(
            table=table
        )
    )


def get_cleaned_db_name(
    name: str,
    lowercase: bool = True,
    remove_leading_numbers: bool = True,
    replace_spaces_with_underscores: bool = True,
    replace_dashes_with_underscores: bool = True,
    remove_special_chars: bool = True,
    limit: int = 128,
) -> str:
    """Gets a name cleaned to adhere to sql naming conventions

    Args:
        name: An uncleaned name (such as a table or column name)
        lowercase: Whether all letters in the name should be lowercased
        remove_leading_numbers: Whether leading numbers should be stripped
        replace_spaces_with_underscores: Whether spaces should be replaced with underscores
        replace_dashes_with_underscores: Whether dashes should be replaced with underscores
        remove_special_chars: Whether any characters other than letters, numbers, and
            underscores should be removed from the name
        limit: the maximum allowed length of the name after cleaning. The default value
            is the Athena/Glue column name length limit.

    Returns:
        A cleaned name to be used in a relational database

    Raises:
        :py:exc:`InvalidSqlIdentifierErr`: If the name is still invalid
            after being cleaned

    """
    cleaned_name = name

    if lowercase:
        cleaned_name = cleaned_name.lower()

    if remove_leading_numbers:
        cleaned_name = cleaned_name.lstrip("0123456789")

    if replace_spaces_with_underscores:
        cleaned_name = "_".join(cleaned_name.split(" "))

    if replace_dashes_with_underscores:
        cleaned_name = "_".join(cleaned_name.split("-"))

    if remove_special_chars:
        cleaned_name = re.sub(r"[^a-zA-Z0-9_]*", "", cleaned_name)

    if len(cleaned_name) == 0 or len(cleaned_name) > limit:
        raise InvalidSqlIdentifierErr(cleaned_name)

    return cleaned_name


async def gather(*coros: Coroutine, return_exceptions: bool = False) -> list[Any]:
    """Helper function to run a collection of coroutines in sequence

    This should be used inside of database transaction instead of asyncio.gather to
    avoid issues caused by multiple concurrent queries.

    See https://github.com/encode/databases/issues/125#issuecomment-511720013

    Args:
        return_exceptions: Flag that controls whether exceptions are returned in the
            list instead of raised immediately. Defaults to False.

    Returns:
        list of results from executing the coroutines

    """
    results = []
    for coro in coros:
        try:
            result = await coro
            results.append(result)
        except Exception as e:
            if return_exceptions:
                results.append(e)
            else:
                raise

    return results


def transaction(f):
    """Helper decorator to wrap a function in a database transaction

    Args:
        f: Function to wrap

    Returns:
        wrapped function that will execute in a transaction

    """

    @wraps(f)
    async def wrapper(*args, **kwargs):
        """Wrapper function"""
        async with get_backend().transaction():
            return await f(*args, **kwargs)

    return wrapper
