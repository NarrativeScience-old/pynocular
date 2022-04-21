"""Tests for DatabaseModel abstract class"""
from asyncio import gather, sleep
from datetime import datetime
import logging
import os
from typing import Optional
from uuid import uuid4

from databases import Database
from pydantic import Field
from pydantic.error_wrappers import ValidationError
import pytest

from pynocular import (
    backend,
    DatabaseModel,
    MemoryDatabaseModelBackend,
    SQLDatabaseModelBackend,
    UUID_STR,
)
from pynocular.db_util import (
    add_datetime_trigger,
    create_new_database,
    create_table,
    drop_table,
)
from pynocular.exceptions import DatabaseModelMissingField, DatabaseRecordNotFound


@pytest.fixture(scope="module")
async def postgres_backend():
    """Fixture that yields a Postgres backend

    Yields:
        postgres backend

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

    async with Database(db_connection_string) as db:
        await create_table(db, Org.table)
        await create_table(db, Topic.table)
        await add_datetime_trigger(db, "organizations")
        try:
            yield SQLDatabaseModelBackend(db)
        finally:
            await drop_table(db, Topic.table)
            await drop_table(db, Org.table)

    async with Database(maintenance_connection_string) as db:
        try:
            await db.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        except Exception as e:
            logging.info(str(e))


@pytest.fixture()
async def memory_backend():
    """Fixture that yields an in-memory backend

    Returns:
        in-memory backend

    """
    return MemoryDatabaseModelBackend()


class Org(DatabaseModel, table_name="organizations"):
    """A test database model"""

    id: UUID_STR = Field(primary_key=True)
    serial_id: Optional[int]
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)


class Topic(DatabaseModel, table_name="topics"):
    """A test class with a nullable JSONB field"""

    id: UUID_STR = Field(primary_key=True)
    app_id: UUID_STR = Field()
    filter: Optional[dict] = Field()
    filter_hash: str = Field(max_length=45)
    name: str = Field(max_length=45)


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_select(_backend) -> None:
    """Test that we can select the full set of DatabaseModels"""
    with backend(_backend):
        try:
            org = await Org.create(
                id=str(uuid4()),
                name="orgus borgus",
                slug="orgus_borgus",
                serial_id=None,
            )
            all_orgs = await Org.select()
            assert len(all_orgs) > 0
        finally:
            await org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_get_list(_backend) -> None:
    """Test that we can get_list and get a subset of DatabaseModels"""
    with backend(_backend):
        try:
            org1 = await Org.create(
                id=str(uuid4()), name="orgus borgus", slug="orgus_borgus", serial_id=1
            )
            org2 = await Org.create(
                id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus", serial_id=1
            )
            org3 = await Org.create(
                id=str(uuid4()),
                name="nonorgus borgus",
                slug="orgus_borgus",
                serial_id=2,
            )
            all_orgs = await Org.select()
            subset_orgs = await Org.get_list(serial_id=org1.serial_id)
            assert len(subset_orgs) <= len(all_orgs)
        finally:
            await org1.delete()
            await org2.delete()
            await org3.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_get_list__none_filter_value(_backend) -> None:
    """Test that we can get_list based on a None filter value"""
    with backend(_backend):
        try:
            test_org = await Org.create(
                id=uuid4(), name="orgus borgus", slug="orgus_borgus", serial_id=None
            )
            orgs = await Org.get_list(serial_id=None)
            assert orgs == [test_org]
        finally:
            await test_org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_get_list__none_json_value(_backend) -> None:
    """Test that we can get_list for a None value on a JSON field"""
    with backend(_backend):
        # The None value will be persisted as a SQL NULL value rather than a JSON-encoded
        # null value when the Topic is created, so the filter value None will work here
        try:
            base_topic = await Topic.create(
                id=uuid4(),
                app_id=str(uuid4()),
                name="base topic",
                filter_hash="fakehash123",
                filter=None,
            )
            topic = await Topic.get_list(filter=None)
            assert topic == [base_topic]
        finally:
            await base_topic.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_create_new_record(_backend) -> None:
    """Test that we can create a database record"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 100
        try:
            org = await Org.create(
                id=org_id, serial_id=serial_id, name="fake org100", slug="fake slug100"
            )
            db_obj = await Org.get(org_id)
            assert db_obj == org
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_create_list(_backend) -> None:
    """Test that we can create a list of database records"""
    with backend(_backend):
        try:
            initial_orgs = [
                Org(id=str(uuid4()), name="fake org 1", slug="fake-slug-1"),
                Org(id=str(uuid4()), name="fake org 2", slug="fake-slug-2"),
            ]
            created_orgs = await Org.create_list(initial_orgs)
            assert [org.name for org in created_orgs] == [
                org.name for org in initial_orgs
            ]
            assert all(org.id is not None for org in created_orgs)
        finally:
            await gather(*[org.delete() for org in created_orgs])


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_create_list__empty(_backend) -> None:
    """Should return empty list for input of empty list"""
    with backend(_backend):
        created_orgs = await Org.create_list([])
        assert created_orgs == []


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_update_new_record__save(_backend) -> None:
    """Test that we can update a database record using `save`"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 101

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org101", slug="fake slug101"
        )

        try:
            await org.save()
            db_obj = await Org.get(org_id)
            assert db_obj == org, "Object should match the original"
            org.name = "new org name"
            await org.save()
            db_obj = await Org.get(org_id)
            assert db_obj.name == "new org name", "Object should have the new name"
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_update_new_record__update_record(_backend) -> None:
    """Test that we can update a database record using `update_record`"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 100000

        org = Org(id=org_id, serial_id=serial_id, name="fake org1", slug="fake slug1")

        try:
            await org.save()
            org.name = "new org name"
            # call update_record to do the update
            await Org.update_record(id=org_id, name="new org name")
            db_obj = await Org.get(org_id)
            # Confirm the name got updated
            assert db_obj.name == "new org name"
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_delete_new_record__delete(_backend) -> None:
    """Test that we can delete a database record using `delete`"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 102

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org102", slug="fake slug102"
        )

        try:
            await org.save()
            db_obj = await Org.get(org_id)
            assert db_obj == org, "Object should match the original"
        finally:
            await org.delete()

        with pytest.raises(DatabaseRecordNotFound):
            await Org.get(org_id)


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_delete_new_record__delete_records(_backend) -> None:
    """Test that we can delete a database record using `delete_records`"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 103

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org103", slug="fake slug103"
        )

        try:
            await org.save()
        finally:
            await Org.delete_records(id=org_id)

        with pytest.raises(DatabaseRecordNotFound):
            await Org.get(org_id)


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_delete_new_record__delete_records_multi_kwargs(_backend) -> None:
    """Test that we can delete a database record using `delete_records` with multiple kwargs"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 104

        org = Org(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )

        try:
            await org.save()
        finally:
            await Org.delete_records(name="fake org104", slug="fake slug104")

        with pytest.raises(DatabaseRecordNotFound):
            await Org.get(org_id)


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_bad_org_object_creation(_backend) -> None:
    """Test that we raise an Exception if the object is missing fields"""
    with backend(_backend):
        org_id = str(uuid4())

        with pytest.raises(ValidationError):
            Org(**{"id": org_id})


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_raise_error_get_list_wrong_field(_backend) -> None:
    """Test that we raise an exception if we query for a wrong field on the object"""
    with backend(_backend):
        with pytest.raises(DatabaseModelMissingField):
            await Org.get_list(table_id="Table1")


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_setting_db_managed_columns(_backend) -> None:
    """Test that db managed columns get automatically set on save"""
    with backend(_backend):
        org = await Org.create(
            id=str(uuid4()), serial_id=105, name="fake_org105", slug="fake_org105"
        )

        try:
            assert org.created_at is not None
            assert org.updated_at is not None

            # Test that the updated_at value gets changed when saved again
            orig_updated = org.updated_at
            await sleep(0.01)
            await org.save()
            assert orig_updated != org.updated_at
        finally:
            await org.delete()


@pytest.mark.parametrize(
    "_backend",
    [
        pytest.lazy_fixture("postgres_backend"),
        pytest.lazy_fixture("memory_backend"),
    ],
)
@pytest.mark.asyncio
async def test_fetch(_backend) -> None:
    """Test that we can fetch the latest state of a database record"""
    with backend(_backend):
        org_id = str(uuid4())
        serial_id = 100
        try:
            org = await Org.create(
                id=org_id, serial_id=serial_id, name="fake org100", slug="fake slug100"
            )
            # Change the value locally
            org.serial_id = 200
            assert org.serial_id == 200

            # Fetch to change it back
            await org.fetch()
            assert org.serial_id == 100
        finally:
            # Make sure we delete org so we don't leak out of test
            await org.delete()
