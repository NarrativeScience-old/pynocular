"""Test that db transaction functionality works as expected"""
import asyncio
import os
from uuid import uuid4

from pydantic import BaseModel, Field
import pytest

from pynocular.database_model import database_model, UUID_STR
from pynocular.db_util import create_new_database, create_table, drop_table
from pynocular.engines import DatabaseType, DBEngine, DBInfo

db_user_password = str(os.environ.get("DB_USER_PASSWORD"))
# DB to initially connect to so we can create a new db
existing_connection_string = str(
    os.environ.get(
        "EXISTING_DB_CONNECTION_STRING",
        f"postgresql://postgres:{db_user_password}@localhost:5432/postgres?sslmode=disable",
    )
)

test_db_name = str(os.environ.get("TEST_DB_NAME", "test_db"))
test_connection_string = str(
    os.environ.get(
        "TEST_DB_CONNECTION_STRING",
        f"postgresql://postgres:{db_user_password}@localhost:5432/{test_db_name}?sslmode=disable",
    )
)
testdb = DBInfo(DatabaseType.aiopg_engine, test_connection_string)


@database_model("organizations", testdb)
class Org(BaseModel):
    """A test database model"""

    id: UUID_STR = Field(primary_key=True)
    name: str = Field(max_length=45)


class TestDatabaseTransactions:
    """Test suite for testing transaction handling with DatabaseModels"""

    @classmethod
    async def _setup_class(cls):
        """Create the database and tables"""
        try:
            await create_new_database(existing_connection_string, test_db_name)
        except Exception:
            # If this fails, assume its already  created
            pass

        await create_table(testdb, Org._table)
        conn = await (await DBEngine.get_engine(testdb)).acquire()
        await conn.close()

    @classmethod
    def setup_class(cls):
        """Setup class function"""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls._setup_class())

    @classmethod
    async def _teardown_class(cls):
        """Drop database tables"""
        await drop_table(testdb, Org._table)

    @classmethod
    def teardown_class(cls):
        """Teardown class function"""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls._teardown_class())

    @pytest.mark.asyncio
    async def test_gathered_updates(self) -> None:
        """Test that we can update the db multiple times in a gather under a single transaction"""
        try:
            async with await DBEngine.transaction(testdb, is_conditional=False):
                await asyncio.gather(
                    Org.create(id=str(uuid4()), name="orgus borgus"),
                    Org.create(id=str(uuid4()), name="porgus orgus"),
                )
            all_orgs = await Org.select()
            assert len(all_orgs) == 2
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_gathered_updates_raise_error(self) -> None:
        """Test that an error in one update rolls back the other when gathered"""
        try:
            try:
                async with await DBEngine.transaction(testdb, is_conditional=False):
                    await asyncio.gather(
                        Org.create(id=str(uuid4()), name="orgus borgus"),
                        # The inputs aren't the right type which should throw an error
                        Org.create(id="blah", name=123),
                    )
            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 0
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_serial_updates(self) -> None:
        """Test that we can update the db serially under a single transaction"""
        try:
            async with await DBEngine.transaction(testdb, is_conditional=False):
                await Org.create(id=str(uuid4()), name="orgus borgus")
                await Org.create(id=str(uuid4()), name="porgus orgus")

            all_orgs = await Org.select()
            assert len(all_orgs) == 2
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_serial_updates_raise_error(self) -> None:
        """Test that an error in one update rolls back the other when run serially"""
        try:
            try:
                async with await DBEngine.transaction(testdb, is_conditional=False):
                    await Org.create(id=str(uuid4()), name="orgus borgus")
                    await Org.create(id="blah", name=123)
            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 0
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_nested_updates(self) -> None:
        """Test that we can perform nested update on the db under a single transaction"""
        try:
            async with await DBEngine.transaction(testdb, is_conditional=False):
                await Org.create(id=str(uuid4()), name="orgus borgus")

                async with await DBEngine.transaction(testdb, is_conditional=False):
                    await Org.create(id=str(uuid4()), name="porgus orgus")

            all_orgs = await Org.select()
            assert len(all_orgs) == 2
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_nested_updates_raise_error(self) -> None:
        """Test that an error in one update rolls back the other when it is nested"""
        try:
            try:
                async with await DBEngine.transaction(testdb, is_conditional=False):
                    await Org.create(id=str(uuid4()), name="orgus borgus")

                    async with await DBEngine.transaction(testdb, is_conditional=False):
                        await Org.create(id="blah", name=123)

            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 0
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_nested_conditional_updates_raise_error(self) -> None:
        """Test that an error in one update rolls back the other even if its a conditional transaction"""
        try:
            try:
                async with await DBEngine.transaction(testdb, is_conditional=False):
                    await Org.create(id=str(uuid4()), name="orgus borgus")

                    async with await DBEngine.transaction(testdb, is_conditional=True):
                        await Org.create(id="blah", name=123)

            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 0
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_open_transaction_decorator(self) -> None:
        """Test that the open_transaction decorator will execute everything in a transaction"""

        @DBEngine.open_transaction(testdb)
        async def write_than_raise_error():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            await Org.create(id=str(uuid4()), name="orgus porgus")

        try:
            try:
                await write_than_raise_error()
            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 2
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])

    @pytest.mark.asyncio
    async def test_open_transaction_decorator_rolls_back(self) -> None:
        """Test that the open_transaction decorator will roll back everything in the function"""

        @DBEngine.open_transaction(testdb)
        async def write_than_raise_error():
            await Org.create(id=str(uuid4()), name="orgus borgus")
            # This create will fail and the decorator should roll back the top one
            await Org.create(id="blah", name=123)

        try:
            try:
                await write_than_raise_error()
            except Exception:
                pass

            all_orgs = await Org.select()
            assert len(all_orgs) == 0
        finally:
            await asyncio.gather(*[org.delete() for org in all_orgs])
