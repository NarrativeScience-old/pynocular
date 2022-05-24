"""Test that db transaction functionality works as expected"""
import logging
from uuid import uuid4

from databases import Database
from pydantic import Field
import pytest

from pynocular.backends.context import get_backend, set_backend
from pynocular.backends.memory import MemoryDatabaseModelBackend
from pynocular.backends.sql import SQLDatabaseModelBackend
from pynocular.database_model import DatabaseModel, UUID_STR
from pynocular.util import create_table, drop_table, gather, transaction

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


@pytest.fixture()
async def memory_backend():
    """Fixture that yields an in-memory backend

    Returns:
        in-memory backend

    """
    return MemoryDatabaseModelBackend()


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_gathered_creates(backend) -> None:
    """Test that we can update the db multiple times in a gather under a single transaction"""
    with set_backend(backend):
        async with get_backend().transaction():
            await gather(
                Org.create(id=str(uuid4()), name="orgus borgus"),
                Org.create(id=str(uuid4()), name="porgus orgus"),
            )

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_gathered_updates_raise_error(backend) -> None:
    """Test that an error in one update rolls back the other when gathered"""
    with set_backend(backend):
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


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_serial_updates(backend) -> None:
    """Test that we can update the db serially under a single transaction"""
    with set_backend(backend):
        async with get_backend().transaction():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            await Org.create(id=str(uuid4()), name="porgus orgus")

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_serial_updates_raise_error(backend) -> None:
    """Test that an error in one update rolls back the other when run serially"""
    with set_backend(backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")
                await Org.create(id="blah", name=123)
        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_nested_updates(backend) -> None:
    """Test that we can perform nested update on the db under a single transaction"""
    with set_backend(backend):
        async with get_backend().transaction():
            await Org.create(id=str(uuid4()), name="orgus borgus")

            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="porgus orgus")

        all_orgs = await Org.select()
        assert len(all_orgs) == 2


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_nested_updates_raise_error(backend) -> None:
    """Test that an error in one update rolls back the other when it is nested"""
    with set_backend(backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")

                async with get_backend().transaction():
                    await Org.create(id="blah", name=123)

        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_nested_conditional_updates_raise_error(backend) -> None:
    """Test that an error in one update rolls back the other even if its a conditional transaction"""
    with set_backend(backend):
        try:
            async with get_backend().transaction():
                await Org.create(id=str(uuid4()), name="orgus borgus")

                async with get_backend().transaction():
                    await Org.create(id="blah", name=123)

        except Exception:
            pass

        all_orgs = await Org.select()
        assert len(all_orgs) == 0


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_open_transaction_decorator(backend) -> None:
    """Test that the open_transaction decorator will execute everything in a transaction"""
    with set_backend(backend):

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


@pytest.mark.parametrize(
    "backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_open_transaction_decorator_rolls_back(backend) -> None:
    """Test that the open_transaction decorator will roll back everything in the function"""
    with set_backend(backend):

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
