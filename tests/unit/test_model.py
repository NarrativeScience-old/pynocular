from typing import List, Optional
from databases import Database
from pydantic import Field
import pytest
from sqlalchemy import asc

from pynocular.backends.sql import SQLDatabaseModelBackend
from pynocular.backends.context import backend
from pynocular.model import DatabaseModel


@pytest.fixture()
async def sql_backend():
    async with Database("sqlite:///example.db") as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS things (id INTEGER PRIMARY KEY, name TEXT)"
        )
        try:
            yield SQLDatabaseModelBackend(db)
        finally:
            await db.execute("DROP TABLE things")


class Thing(DatabaseModel, table_name="things"):
    """A test database model"""

    id: Optional[int] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field()


@pytest.mark.asyncio
async def test_thing(sql_backend):
    with backend(sql_backend):
        things: List[Thing] = await Thing.select()
        assert things == []

        things = await Thing.create_list([Thing(name="hello"), Thing(name="world")])
        assert things[0].name == "hello"
        assert things[0].id == 1
        assert things[1].name == "world"
        assert things[1].id == 2
