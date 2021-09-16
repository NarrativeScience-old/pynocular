"""Tests for DatabaseModel abstract class"""
import asyncio
from datetime import datetime
import os
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field
import pytest

from pynocular.database_model import database_model, foreign_key, UUID_STR
from pynocular.db_util import add_trigger, create_new_database, create_table
from pynocular.engines import DatabaseType, DBEngine, DBInfo

db_user_password = str(os.environ.get("DB_USER_PASSWORD"))
# DB to initially connect to so we can create a new db
existing_connection_string = str(
    os.environ.get(
        "EXISTING_DB_CONNECTION_STRING",
        f"postgresql://postgres:{db_user_password}@localhost:5432/postgres?sslmode=disable",
    )
)

test_db_name = str(os.environ.get("TEST_DB_NAME", "test_db2"))
test_connection_string = str(
    os.environ.get(
        "TEST_DB_CONNECTION_STRING",
        f"postgresql://postgres:{db_user_password}@localhost:5432/{test_db_name}?sslmode=disable",
    )
)
testdb = DBInfo(DatabaseType.aiopg_engine, test_connection_string)


@database_model("organizations", testdb)
class Org(BaseModel):
    """A test class for testing. Linter won't let me not have this useless comment :)"""

    id: UUID_STR = Field(primary_key=True)
    serial_id: Optional[int]
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)


@database_model("apps", testdb)
class App(BaseModel):
    """Model that represents the App table schema"""

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field(max_length=45)
    org: foreign_key(Org, reference_field="organization_id")
    fiscal_year_start_month: Optional[int]
    metadata: Optional[dict]
    slug: str = Field(max_length=45)
    data_cache_schema: str
    data_store_schema: str
    serial_id: Optional[int] = Field(fetch_on_create=True)
    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)
    apple_compliant: Optional[bool] = Field(default=False)
    deleted: Optional[bool] = Field(default=False)


@database_model("topics", testdb)
class Topic(BaseModel):
    """A test class with a nullable JSONB field"""

    id: UUID_STR = Field(primary_key=True)
    app: foreign_key(App, reference_field="app_id")
    filter: Optional[dict] = Field()
    filter_hash: str = Field(max_length=45)
    name: str = Field(max_length=45)


async def setup_db_and_tables():
    """Create the database and tables"""
    await create_new_database(existing_connection_string, test_db_name)

    await create_table(testdb, Org._table)
    await create_table(testdb, Topic._table)
    await create_table(testdb, App._table)
    conn = await (await DBEngine.get_engine(testdb)).acquire()
    await add_trigger(conn, "organizations")


loop = asyncio.get_event_loop()
loop.run_until_complete(setup_db_and_tables())


@pytest.mark.asyncio
async def test_fetch() -> None:
    """Test that we can resolve the reference for a foreign key"""
    org_id = str(uuid4())
    serial_id = 104

    try:
        org = await Org.create(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org,
            slug="app-slug",
            data_cache_schema="app_ds1",
            data_store_schema="app_dc1",
        )

        app_get = await App.get(app.id)
        assert app_get.org.id == org.id
        await app_get.org.fetch()
        assert app_get.org == org
    finally:
        await org.delete()
        await app.delete()


@pytest.mark.asyncio
async def test_swap_foreign_reference() -> None:
    """Test that we can swap foreign key references"""
    org_id = str(uuid4())
    serial_id = 104

    try:
        org1 = await Org.create(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )
        org2 = await Org.create(
            id=str(uuid4()),
            serial_id=serial_id + 1,
            name="fake org105",
            slug="fake slug105",
        )

        # Start with app pointing to the first org
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org1,
            slug="app-slug",
            data_cache_schema="app_ds1",
            data_store_schema="app_dc1",
        )

        # Confirm app is associated with org 1
        app_get = await App.get(app.id)
        assert app_get.org.id == org1.id

        # Move app to org 2
        app_get.org = org2
        await app_get.save()
        app_get = await App.get(app.id)
        assert app_get.org.id == org2.id
        await app_get.org.fetch()
        assert app_get.org == org2
    finally:
        await org1.delete()
        await org2.delete()
        await app.delete()


@pytest.mark.asyncio
async def test_resolve_on_get() -> None:
    """Test that we can resolve foreign keys when we retrieve the record object"""
    org_id = str(uuid4())
    serial_id = 104

    try:
        org = await Org.create(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org,
            slug="app-slug",
            data_cache_schema="app_ds1",
            data_store_schema="app_dc1",
        )

        app_get = await App.get(app.id, resolve_references=True)
        assert app_get.org == org
    finally:
        await org.delete()
        await app.delete()


@pytest.mark.asyncio
async def test_nested_foreign_references() -> None:
    """Test that we can nest foreign key references and resolve them"""
    org_id = str(uuid4())
    serial_id = 104

    try:
        org = await Org.create(
            id=org_id, serial_id=serial_id, name="fake org104", slug="fake slug104"
        )
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org,
            slug="app-slug",
            data_cache_schema="app_ds1",
            data_store_schema="app_dc1",
        )

        topic = await Topic.create(
            id=str(uuid4()), name="topic name", app=app, filter_hash="sdfasdf"
        )

        await topic.app.fetch()
        assert topic.app.id == app.id
        assert topic.app == app
        assert topic.app.org.id == org.id
        assert topic.app.org == org
    finally:
        await org.delete()
        await app.delete()
        await topic.delete()
