"""Tests for DatabaseModel abstract class"""
import asyncio
from asyncio import gather, sleep
from datetime import datetime
from typing import Callable, Optional
import unittest
from uuid import uuid4

from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError

from ns_env_config import EnvConfig
from ns_sql_utils.engines import DBInfo, DBEngine, DatabaseType
from ns_sql_utils.database_model import database_model, UUID_STR
from ns_sql_utils.db_util import add_trigger, create_new_database, create_table
from ns_sql_utils.exceptions import DatabaseModelMissingField, DatabaseRecordNotFound


test_db_name = EnvConfig.string("TEST_DB_NAME", "test_db")
test_connection_string = EnvConfig.string(
    "TEST_DB_CONNECTION_STRING",
    f"postgresql://nssvc@localhost:5432/{test_db_name}?sslmode=disable",
)
testdb = DBInfo(DatabaseType.aiopg_engine, test_connection_string)


# This belongs in ns_test_utils
# from ns_unit_test_utils import async_test
def async_test(async_test_func: Callable[..., None]) -> Callable[..., None]:
    """Decorator for running an async test function synchronously

    This may be useful when testing coroutines or db calls with aiopg

    Args:
        async_test_func: a test function written as a coroutine

    Returns:
        the decorated function

    """

    def sync_test_func(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_test_func(*args, **kwargs))

    return sync_test_func


@database_model("organizations", testdb)
class Org(BaseModel):
    """A test class for testing. Linter won't let me not have this useless comment :)"""

    id: UUID_STR = Field(primary_key=True)
    serial_id: Optional[int]
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)


@database_model("topics", testdb)
class Topic(BaseModel):
    """A test class with a nullable JSONB field"""

    id: UUID_STR = Field(primary_key=True)
    app_id: UUID_STR = Field()
    filter: Optional[dict] = Field()
    filter_hash: str = Field(max_length=45)
    name: str = Field(max_length=45)


@async_test
async def setup():
    begin_connection_string = EnvConfig.string(
        "BEGIN_CONNECTION_STRING",
        "postgresql://postgres@localhost:5432/postgres?sslmode=disable",
    )
    await create_new_database(begin_connection_string, test_db_name)

    await create_table(testdb, Org._table)
    await create_table(testdb, Topic._table)
    conn = await (await DBEngine.get_engine(testdb)).acquire()
    await add_trigger(conn, "organizations")


setup()


class DatabaseModelSubclassTests(unittest.TestCase):
    """Test subclassing a DatabaseModel object"""

    @async_test
    async def test_select(self) -> None:
        """Test that we can select the full set of DatabaseModels"""
        org = await Org.create(
            id=str(uuid4()), name="orgus borgus", slug="orgus_borgus", serial_id=None
        )
        all_orgs = await Org.select()
        self.assertTrue(len(all_orgs) > 0)
        await org.delete()

    @async_test
    async def test_get_list(self) -> None:
        """Test that we can get_list and get a subset of DatabaseModels"""
        org1 = await Org.create(
            id=str(uuid4()), name="orgus borgus", slug="orgus_borgus", serial_id=1
        )
        org2 = await Org.create(
            id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus", serial_id=1
        )
        org3 = await Org.create(
            id=str(uuid4()), name="nonorgus borgus", slug="orgus_borgus", serial_id=2
        )
        all_orgs = await Org.select()
        subset_orgs = await Org.get_list(serial_id=org1.serial_id)
        self.assertTrue(len(subset_orgs) <= len(all_orgs))
        await org1.delete()
        await org2.delete()
        await org3.delete()

    @async_test
    async def test_get_list__none_filter_value(self) -> None:
        """Test that we can get_list based on a None filter value"""
        test_org = await Org.create(
            id=uuid4(), name="orgus borgus", slug="orgus_borgus", serial_id=None
        )
        orgs = await Org.get_list(serial_id=None)
        self.assertEqual(orgs, [test_org])
        await test_org.delete()

    @async_test
    async def test_get_list__none_json_value(self) -> None:
        """Test that we can get_list for a None value on a JSON field"""
        # The None value will be persisted as a SQL NULL value rather than a JSON-encoded
        # null value when the Topic is created, so the filter value None will work here
        base_topic = await Topic.create(
            id=uuid4(),
            app_id=str(uuid4()),
            name="base topic",
            filter_hash="fakehash123",
            filter=None,
        )
        topic = await Topic.get_list(filter=None)
        self.assertEqual(topic, [base_topic])
        await base_topic.delete()

    @async_test
    async def test_create_new_record(self) -> None:
        """Test that we can create a database record"""
        org_id = str(uuid4())
        serial_id = 100
        try:
            org = await Org.create(
                id=org_id, serial_id=serial_id, name="fake org100", slug="fake slug100"
            )
            db_obj = await Org.get(org_id)
            self.assertEqual(db_obj, org)
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()

    @async_test
    async def test_create_list(self) -> None:
        """Test that we can create a list of database records"""
        try:
            initial_orgs = [
                Org(id=str(uuid4()), name="fake org 1", slug="fake-slug-1"),
                Org(id=str(uuid4()), name="fake org 2", slug="fake-slug-2"),
            ]
            created_orgs = await Org.create_list(initial_orgs)
            self.assertEqual(
                [org.name for org in created_orgs], [org.name for org in initial_orgs]
            )
            self.assertTrue(all(org.id is not None for org in created_orgs))
        finally:
            await gather(*[org.delete() for org in created_orgs])

    @async_test
    async def test_create_list__empty(self) -> None:
        """Should return empty list for input of empty list"""
        created_orgs = await Org.create_list([])
        self.assertEqual(created_orgs, [])

    @async_test
    async def test_update_new_record__save(self) -> None:
        """Test that we can update a database record using `save`"""
        org_id = str(uuid4())
        serial_id = 101

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org101", slug="fake slug101"
        )

        try:
            await org.save()
            db_obj = await Org.get(org_id)
            self.assertEqual(db_obj, org, msg="Object should match the original")
            org.name = "new org name"
            await org.save()
            db_obj = await Org.get(org_id)
            self.assertEqual(
                db_obj.name, "new org name", msg="Object should have the new name"
            )
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()

    # TODO QPT-31660
    # @async_test
    # async def test_update_new_record__update_record(self) -> None:
    #     """Test that we can update a database record using `update_record`"""
    #     org_id = uuid4()
    #     serial_id = 999999

    #     org = Org(id=str(org_id), serial_id=serial_id, name="fake org1", slug="fake slug1")

    #     try:
    #         await org.save()
    #         org.name = "new org name"
    #         # call update_record to do the update
    #         await Org.update_record(id=org_id, name="new org name")
    #         db_obj = await Org.get(org_id)
    #         # Confirm the name got updated
    #         self.assertEqual(
    #             db_obj.name, "new org name", msg="Object should have the new name"
    #         )
    #         # Confirm that the other fields are same as the original object
    #         self.assertEqual(org, db_obj, msg="Objects should be the same")
    #     finally:
    #         # Make sure we delete org so we don't leak out of test
    #         await org.delete()

    @async_test
    async def test_delete_new_record__delete(self) -> None:
        """Test that we can delete a database record using `delete`"""
        org_id = str(uuid4())
        serial_id = 102

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org102", slug="fake slug102"
        )

        try:
            await org.save()
            db_obj = await Org.get(org_id)
            self.assertEqual(db_obj, org, msg="Object should match the original")
        finally:
            await org.delete()

        with self.assertRaises(DatabaseRecordNotFound):
            await Org.get(org_id)

    @async_test
    async def test_delete_new_record__delete_records(self) -> None:
        """Test that we can delete a database record using `delete_records`"""
        org_id = str(uuid4())
        serial_id = 103

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org103", slug="fake slug103"
        )

        try:
            await org.save()
        finally:
            await Org.delete_records(id=org_id)

        with self.assertRaises(DatabaseRecordNotFound):
            await Org.get(org_id)

    @async_test
    async def test_delete_new_record__delete_records_multi_kwargs(self) -> None:
        """Test that we can delete a database record using `delete_records` with multiple kwargs"""
        org_id = str(uuid4())
        serial_id = 104

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )

        try:
            await org.save()
        finally:
            await Org.delete_records(name="fake org104", slug="fake slug104")

        with self.assertRaises(DatabaseRecordNotFound):
            await Org.get(org_id)

    @async_test
    async def test_bad_org_object_creation(self) -> None:
        """Test that we raise an Exception if the object is missing fields"""
        org_id = str(uuid4())

        with self.assertRaises(ValidationError):
            Org(**{"id": org_id})

    @async_test
    async def test_raise_error_get_list_wrong_field(self) -> None:
        """Test that we raise an exception if we query for a wrong field on the object"""
        with self.assertRaises(DatabaseModelMissingField):
            await Org.get_list(table_id="Table1")

    @async_test
    async def test_setting_db_managed_columns(self) -> None:
        """Test that db managed columns get automatically set on save"""
        org = await Org.create(
            id=str(uuid4()), serial_id=105, name="fake_org105", slug="fake_org105"
        )

        try:
            self.assertIsNotNone(org.created_at)
            self.assertIsNotNone(org.updated_at)

            # Test that the updated_at value gets changed when saved again
            orig_updated = org.updated_at
            await sleep(0.01)
            await org.save()
            self.assertNotEqual(orig_updated, org.updated_at)
        finally:
            await org.delete()
