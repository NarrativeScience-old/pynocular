"""Tests for patch_database_model context manager"""
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field
import pytest

from pynocular.database_model import database_model, nested_model, UUID_STR
from pynocular.engines import DBInfo
from pynocular.patch_models import patch_database_model

# With the `patch_database_model` we don't need a database connection
test_connection_string = "fake connection string"
testdb = DBInfo(test_connection_string)


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


class TestPatchDatabaseModel:
    """Test class for patch_database_model"""

    @pytest.mark.asyncio
    async def test_patch_database_model_without_models(self) -> None:
        """Test that we can use `patch_database_model` without providing models"""
        orgs = [
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus"),
        ]

        with patch_database_model(Org):
            await Org.create_list(orgs)
            # Also create one org through Org.create()
            await Org.create(
                id=str(uuid4()), name="nonorgus borgus", slug="orgus_borgus"
            )
            all_orgs = await Org.select()
            subset_orgs = await Org.get_list(name=orgs[0].name)
            assert len(subset_orgs) <= len(all_orgs)
            assert orgs[0] == subset_orgs[0]

    @pytest.mark.asyncio
    async def test_patch_database_model_with_models(self) -> None:
        """Test that we can use `patch_database_model` with models"""
        orgs = [
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="nonorgus borgus", slug="orgus_borgus"),
        ]

        with patch_database_model(Org, models=orgs):
            org = (await Org.get_list(name=orgs[0].name))[0]
            org.name = "new test name"
            await org.save()
            org_get = await Org.get(org.id)
            assert org_get.name == "new test name"

    @pytest.mark.asyncio
    async def test_patch_database_model_with_nested_models(self) -> None:
        """Test that we can use `patch_database_model` with nested models"""
        users = [
            User(id=str(uuid4()), username="Bob"),
            User(id=str(uuid4()), username="Sally"),
        ]
        orgs = [
            Org(
                id=str(uuid4()),
                name="orgus borgus",
                slug="orgus_borgus",
                tech_owner=users[0],
                business_owner=users[1],
            ),
            Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="nonorgus borgus", slug="orgus_borgus"),
        ]

        with patch_database_model(Org, models=orgs), patch_database_model(
            User, models=users
        ):
            org = (await Org.get_list(name=orgs[0].name))[0]
            org.name = "new test name"
            users[0].username = "bberkley"
            await org.save(include_nested_models=True)
            org_get = await Org.get_with_refs(org.id)
            assert org_get.name == "new test name"
            assert org_get.tech_owner.username == "bberkley"
