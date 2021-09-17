"""Database utility functions"""

import logging
import re

from aiopg.sa.connection import SAConnection
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

from pynocular.engines import DatabaseType, DBEngine, DBInfo
from pynocular.exceptions import InvalidSqlIdentifierErr

logger = logging.getLogger()


async def create_new_database(connection_string: str, db_name: str) -> None:
    """Create a new database database for testing

    Args:
        connection_string: A connection string for the database
        db_name: the name of the database to create

    """
    existing_db = DBInfo(DatabaseType.aiopg_engine, connection_string)
    conn = await (await DBEngine.get_engine(existing_db)).acquire()
    # End existing commit
    await conn.execute("commit")
    # Create db
    await conn.execute(f"drop database if exists {db_name}")
    await conn.execute(f"create database {db_name}")
    await conn.close()


async def create_table(db_info: DBInfo, table: sa.Table) -> None:
    """Create table in database

    Args:
        db_info: Information for the database to connect to
        table: The table to create

    """
    engine = await DBEngine.get_engine(db_info)
    conn = await engine.acquire()
    await conn.execute(CreateTable(table))
    await conn.close()


async def drop_table(db_info: DBInfo, table: sa.Table) -> None:
    """Drop table in database

    Args:
        db_info: Information for the database to connect to
        table: The table to create

    """
    engine = await DBEngine.get_engine(db_info)
    conn = await engine.acquire()
    await conn.execute(f"drop table if exists {table.name}")
    await conn.close()


async def setup_datetime_trigger(conn: SAConnection) -> None:
    """Set up created_at/updated_at datetime trigger

    Args:
        conn: an async sqlalchemy connection

    """
    await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    await conn.execute('CREATE EXTENSION IF NOT EXISTS "plpgsql";')
    await conn.execute(
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


async def add_datetime_trigger(conn: SAConnection, table: str) -> None:
    """Helper method for adding created_at and updated_at datetime triggers on a table


    Args:
        conn: an async sqlalchemy connection
        table: The name of the table to add an edit trigger for

    """
    await setup_datetime_trigger(conn)
    await conn.execute(
        """
        CREATE TRIGGER update_{table}_timestamps
        BEFORE INSERT OR UPDATE ON {table}
        FOR EACH ROW EXECUTE PROCEDURE update_timestamp_columns();
    """.format(
            table=table
        )
    )


async def remove_datetime_trigger(conn: SAConnection, table: str) -> None:
    """Helper method for removing datetime triggers on a table

    Args:
        conn: an async sqlalchemy connection
        table: The name of the table to remove a trigger for

    """
    await conn.execute(
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
