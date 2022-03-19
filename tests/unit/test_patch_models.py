"""Tests for patch_database_model context manager"""
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field
import pytest
from sqlalchemy import or_

from pynocular.database_model import (
    database_model,
    DatabaseModel,
    nested_model,
    UUID_STR,
)
from pynocular.engines import DBInfo
from pynocular.patch_models import _evaluate_column_element, patch_database_model

# With the `patch_database_model` we don't need a database connection
test_connection_string = "fake connection string"
testdb = DBInfo(test_connection_string)
name = "boo"


class User(DatabaseModel, table_name="users", database_info=testdb):
    """Model that represents the `users` table"""

    id: UUID_STR = Field(primary_key=True)
    username: str = Field(max_length=100)


@database_model("organizations", testdb)
class Org(BaseModel):
    """Model that represents the `organizations` table"""

    id: UUID_STR = Field(primary_key=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    tech_owner: Optional[
        nested_model(User, reference_field="tech_owner_id")  # noqa F821
    ]
    business_owner: Optional[
        nested_model(User, reference_field="business_owner_id")  # noqa F821
    ]
    flag1: bool = Field(default=True)
    flag2: bool = Field(default=True)
    flag3: bool = Field(default=True)


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

    @pytest.mark.asyncio
    async def test_patch_database_model_with_delete(self) -> None:
        """Test that we can use `delete` on a patched db model"""
        orgs = [
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="nonorgus borgus", slug="orgus_borgus"),
        ]

        with patch_database_model(Org, models=orgs):
            db_orgs = await Org.get_list()
            assert len(db_orgs) == 3
            await orgs[0].delete()
            db_orgs = await Org.get_list()
            assert len(db_orgs) == 2

            # Confirm the correct orgs are left
            sorted_orgs = sorted(orgs[1:3], key=lambda x: x.id)
            sorted_db_orgs = sorted(db_orgs, key=lambda x: x.id)
            assert sorted_orgs == sorted_db_orgs

    @pytest.mark.asyncio
    async def test_patch_database_model_with_delete_records(self) -> None:
        """Test that we can use `delete_records` on a patched db model"""
        orgs = [
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus2"),
            Org(id=str(uuid4()), name="nonorgus borgus", slug="nonorgus_borgus"),
        ]

        with patch_database_model(Org, models=orgs):
            db_orgs = await Org.get_list()
            assert len(db_orgs) == 3
            await Org.delete_records(slug=["orgus_borgus2", "nonorgus_borgus"])
            db_orgs = await Org.get_list()
            assert len(db_orgs) == 1

            # Confirm the correct org is left
            assert orgs[0] == db_orgs[0]

    @pytest.mark.asyncio
    async def test_patch_database_model_with_update(self) -> None:
        """Test that we can use `update` to update multiple models"""
        orgs = [
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
            Org(id=str(uuid4()), name="nonorgus borgus", slug="nonorgus_borgus"),
        ]

        with patch_database_model(Org, models=orgs):
            db_orgs = await Org.get_list()
            assert len(db_orgs) == 3
            updated = await Org.update(
                [Org.columns.name == "orgus borgus"],
                values={"name": "foo", "slug": "bar"},
            )
            assert {org.id for org in updated} == {org.id for org in orgs[:2]}
            assert all(org.name == "foo" and org.slug == "bar" for org in updated)


class TestEvaluateColumnElement:
    """Test class for the _evaluate_column_element function"""

    def test_evaluate_column_element__neq(self) -> None:
        """Should handle the is_not operator"""
        assert not _evaluate_column_element(Org.columns.name != "foo", {"name": "foo"})

    def test_evaluate_column_element__n_ary_or(self) -> None:
        """Should handle an OR with multiple arguments"""
        assert _evaluate_column_element(
            or_(Org.columns.flag1, Org.columns.flag2, Org.columns.flag3),
            {"flag1": False, "flag2": False, "flag3": True},
        )

    def test_evaluate_column_element__not(self) -> None:
        """Should handle a NOT operator"""
        assert not _evaluate_column_element(~Org.columns.flag1, {"flag1": True})
