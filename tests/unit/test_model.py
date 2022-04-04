from typing import List, Optional
from databases import Database
from pydantic import Field
import pytest
from sqlalchemy import asc

from pynocular.backends.sql import SQLDatabaseModelBackend
from pynocular.backends.context import backend
from pynocular.model import DatabaseModel


@pytest.fixture()
async def sql_backend():
    async with Database("postgresql://localhost:5432/jon.drake") as db:
        yield SQLDatabaseModelBackend(db=db)


class Org(DatabaseModel, table_name="organizations"):
    """A test database model"""

    id: Optional[int] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field()


@pytest.mark.asyncio
async def test_org(sql_backend):
    with backend(sql_backend):
        orgs: List[Org] = await Org.select(order_by=[asc(Org.columns.id)])
        org = orgs[0]
        assert org.name == "second"

        org.name = "second"
        await org.save()

        await Org.create_list([Org(name="hello"), Org(name="world")])
        orgs: List[Org] = await Org.select(order_by=[asc(Org.columns.id)])
        assert {o.name for o in orgs} == {"second", "hello", "world"}
