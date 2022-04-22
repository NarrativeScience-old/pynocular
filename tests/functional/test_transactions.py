"""Test that db transaction functionality works as expected"""
import logging
from uuid import uuid4

from databases import Database
from pydantic import Field
import pytest

from pynocular.backends.context import backend, get_backend
from pynocular.backends.sql import SQLDatabaseModelBackend
from pynocular.database_model import DatabaseModel, UUID_STR
from pynocular.db_util import create_table, drop_table, gather, transaction

logger = logging.getLogger("pynocular")


class Org(DatabaseModel, table_name="organizations"):
    """A test database model"""

    id: UUID_STR = Field(primary_key=True)
    name: str = Field(max_length=45)


@pytest.fixture()
async def postgres_backend(postgres_database: Database):
    """Fixture that creates tables before yielding a Postgres backend

    Returns:
        postgres backend

    """
    await create_table(postgres_database, Org.table)
    try:
        yield SQLDatabaseModelBackend(postgres_database)
    finally:
        await drop_table(postgres_database, Org.table)


@pytest.mark.asyncio
async def test_gathered_creates(postgres_backend) -> None:
    """Test that we can update the db multiple times in a gather under a single transaction"""
    with backend(postgres_backend):
        async with get_backend().db.transaction():
            await gather(
                Org.create(id=str(uuid4()), name="orgus borgus"),
                Org.create(id=str(uuid4()), name="porgus orgus"),
            )

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.asyncio
async def test_gathered_updates_raise_error(postgres_backend) -> None:
    """Test that an error in one update rolls back the other when gathered"""
    with backend(postgres_backend):
        try:
            async with get_backend().transaction():
                await gather(
                    Org.create(id=str(uuid4()), name="orgus borgus"),
                    # The inputs aren't the right type which should throw an error
                    Org.create(id="blah", name=123),
                )
        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.asyncio
async def test_serial_updates(postgres_backend) -> None:
    """Test that we can update the db serially under a single transaction"""
    with backend(postgres_backend):
        async with get_backend().transaction():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            await Org.create(id=str(uuid4()), name="porgus orgus")

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.asyncio
async def test_serial_updates_raise_error(postgres_backend) -> None:
    """Test that an error in one update rolls back the other when run serially"""
    with backend(postgres_backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")
                await Org.create(id="blah", name=123)
        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.asyncio
async def test_nested_updates(postgres_backend) -> None:
    """Test that we can perform nested update on the db under a single transaction"""
    with backend(postgres_backend):
        async with get_backend().transaction():
            await Org.create(id=str(uuid4()), name="orgus borgus")

            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="porgus orgus")

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.asyncio
async def test_nested_updates_raise_error(postgres_backend) -> None:
    """Test that an error in one update rolls back the other when it is nested"""
    with backend(postgres_backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")

                async with get_backend().transaction():
                    await Org.create(id="blah", name=123)

        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.asyncio
async def test_nested_conditional_updates_raise_error(postgres_backend) -> None:
    """Test that an error in one update rolls back the other even if its a conditional transaction"""
    with backend(postgres_backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")

                async with get_backend().transaction():
                    await Org.create(id="blah", name=123)

        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.asyncio
async def test_open_transaction_decorator(postgres_backend) -> None:
    """Test that the open_transaction decorator will execute everything in a transaction"""
    with backend(postgres_backend):

        @transaction
        async def write_than_raise_error():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            await Org.create(id=str(uuid4()), name="orgus porgus")

        try:
            await write_than_raise_error()
        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.asyncio
async def test_open_transaction_decorator_rolls_back(postgres_backend) -> None:
    """Test that the open_transaction decorator will roll back everything in the function"""
    with backend(postgres_backend):

        @transaction
        async def write_than_raise_error():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            # This create will fail and the decorator should roll back the top one
            await Org.create(id="blah", name=123)

        try:
            await write_than_raise_error()
        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0
