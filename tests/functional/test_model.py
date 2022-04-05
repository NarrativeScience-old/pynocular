"""Contains tests for the DatabaseModel backends"""

from typing import List, Optional

from databases import Database
from pydantic import Field
import pytest
from sqlalchemy import desc

from pynocular.backends.context import backend
from pynocular.backends.memory import MemoryDatabaseModelBackend
from pynocular.backends.sql import SQLDatabaseModelBackend
from pynocular.model import DatabaseModel


@pytest.fixture()
async def postgres_backend():
    """Fixture that yields a Postgres backend

    Yields:
        postgres backend

    """
    async with Database("postgres://localhost:5432/postgres") as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS things (id SERIAL PRIMARY KEY, name TEXT)"
        )
        try:
            yield SQLDatabaseModelBackend(db)
        finally:
            await db.execute("DROP TABLE things")


@pytest.fixture()
async def memory_backend():
    """Fixture that yields an in-memory backend

    Returns:
        in-memory backend

    """
    return MemoryDatabaseModelBackend()


class Thing(DatabaseModel, table_name="things"):
    """A test database model"""

    id: Optional[int] = Field(primary_key=True)
    name: str = Field()


async def _run_tests():
    """Run tests agnostic to the backend"""
    things: List[Thing] = await Thing.select()
    assert things == []

    things = await Thing.create_list([Thing(name="hello"), Thing(name="world")])
    assert [t.to_dict() for t in things] == [
        {
            "name": "hello",
            "id": 1,
        },
        {
            "name": "world",
            "id": 2,
        },
    ]

    things[1].name = "you"
    await things[1].save()

    things: List[Thing] = await Thing.select(order_by=[desc(Thing.columns.name)])
    assert [t.to_dict() for t in things] == [
        {
            "name": "you",
            "id": 2,
        },
        {
            "name": "hello",
            "id": 1,
        },
    ]

    things: List[Thing] = await Thing.get_list(name="hello")
    assert [t.to_dict() for t in things] == [
        {
            "name": "hello",
            "id": 1,
        },
    ]

    await things[0].delete()
    assert len(await Thing.get_list()) == 1

    await Thing.delete_records(name="you")
    assert len(await Thing.get_list()) == 0


@pytest.mark.asyncio
async def test_postgres(postgres_backend):
    """Should run a set of operations on a Postgres backend"""
    with backend(postgres_backend):
        await _run_tests()


@pytest.mark.asyncio
async def test_memory(memory_backend):
    """Should run a set of operations on an in-memory backend"""
    with backend(memory_backend):
        await _run_tests()
