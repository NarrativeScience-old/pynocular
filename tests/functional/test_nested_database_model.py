"""Tests for DatabaseModel abstract class"""
import asyncio
from datetime import datetime
import os
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field
import pytest

from pynocular.database_model import database_model, nested_model, UUID_STR
from pynocular.db_util import add_datetime_trigger, create_new_database, create_table
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


@database_model("users", testdb)
class User(BaseModel):
    """Model that represents the `users` table"""

    id: UUID_STR = Field(primary_key=True)
    username: str = Field(max_length=100)


@database_model("organizations", testdb)
class Org(BaseModel):
    """Model that represents the `organizations` table"""

    id: UUID_STR = Field(primary_key=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    tech_owner: Optional[nested_model(User, reference_field="tech_owner_id")]
    business_owner: Optional[nested_model(User, reference_field="business_owner_id")]

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)


@database_model("apps", testdb)
class App(BaseModel):
    """Model that represents the `apps` table"""

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field(max_length=45)
    org: nested_model(Org, reference_field="organization_id")
    slug: str = Field(max_length=45)


@database_model("topics", testdb)
class Topic(BaseModel):
    """Model that represents the `topics` table"""

    id: UUID_STR = Field(primary_key=True)
    app: nested_model(App, reference_field="app_id")
    name: str = Field(max_length=45)


async def setup_db_and_tables():
    """Create the database and tables"""
    await create_new_database(existing_connection_string, test_db_name)

    await create_table(testdb, User._table)
    await create_table(testdb, Org._table)
    await create_table(testdb, Topic._table)
    await create_table(testdb, App._table)
    conn = await (await DBEngine.get_engine(testdb)).acquire()
    await add_datetime_trigger(conn, "organizations")


loop = asyncio.get_event_loop()
loop.run_until_complete(setup_db_and_tables())


@pytest.mark.asyncio
async def test_fetch() -> None:
    """Test that we can resolve the reference for a foreign key"""

    try:
        tech_owner = await User.create(id=str(uuid4()), username="owner1")
        business_owner = await User.create(id=str(uuid4()), username="owner2")
        org = await Org.create(
            id=str(uuid4()),
            name="fake org104",
            slug="fake slug104",
            tech_owner=tech_owner,
            business_owner=business_owner,
        )

        org_get = await Org.get(org.id)
        assert org_get.tech_owner.id == tech_owner.id
        assert org_get.business_owner.id == business_owner.id

        await org_get.tech_owner.fetch()
        await org_get.business_owner.fetch()
        assert org_get.tech_owner == tech_owner
        assert org_get.business_owner == business_owner
    finally:
        await org.delete()
        await tech_owner.delete()
        await business_owner.delete()


@pytest.mark.asyncio
async def test_swap_foreign_reference() -> None:
    """Test that we can swap foreign key references"""
    org_id = str(uuid4())

    try:
        org1 = await Org.create(id=org_id, name="fake org104", slug="fake slug104")
        org2 = await Org.create(
            id=str(uuid4()),
            name="fake org105",
            slug="fake slug105",
        )

        # Start with app pointing to the first org
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org1,
            slug="app-slug",
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
async def test_get_with_refs() -> None:
    """Test that we can resolve foreign keys when we retrieve the record object"""
    org_id = str(uuid4())

    try:
        org = await Org.create(id=org_id, name="fake org104", slug="fake slug104")
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org,
            slug="app-slug",
        )

        app_get = await App.get_with_refs(app.id)
        assert app_get.org == org
    finally:
        await org.delete()
        await app.delete()


@pytest.mark.asyncio
async def test_nested_foreign_references() -> None:
    """Test that we can nest foreign key references and resolve them"""
    org_id = str(uuid4())

    try:
        org = await Org.create(id=org_id, name="fake org104", slug="fake slug104")
        app = await App.create(
            id=str(uuid4()),
            name="app name",
            org=org,
            slug="app-slug",
        )

        topic = await Topic.create(id=str(uuid4()), name="topic name", app=app)

        assert topic.app.id == app.id
        assert topic.app == app
        assert topic.app.org.id == org.id
        assert topic.app.org == org
    finally:
        await org.delete()
        await app.delete()
        await topic.delete()


@pytest.mark.asyncio
async def test_nested_save() -> None:
    """Test that all the objects will persist if the proper flag is provided"""

    try:
        tech_owner = User(id=str(uuid4()), username="owner1")
        business_owner = User(id=str(uuid4()), username="owner2")
        org = Org(
            id=str(uuid4()),
            name="fake org104",
            slug="fake slug104",
            business_owner=business_owner,
        )

        await org.save(include_nested_models=True)

        # Get the org and user that should have persisted
        org_get = await Org.get(org.id)
        user_get = await User.get(business_owner.id)

        assert org_get.business_owner.id == user_get.id

        # Now add the tech owner and save again. This time, org_get.business_owner is
        # not resolved but it should still successfully persist everything
        org_get.tech_owner = tech_owner
        await org_get.save(include_nested_models=True)

        org_get = await Org.get(org_get.id)
        user_get = await User.get(tech_owner.id)

        assert org_get.tech_owner.id == user_get.id
        assert org_get.business_owner.id == business_owner.id
    finally:
        await org.delete()
        await tech_owner.delete()
        await business_owner.delete()
