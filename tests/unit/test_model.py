from datetime import datetime
from typing import List, Optional
from databases import Database
from pydantic import Field
import pytest
from pynocular.backends.postgres import PostgresDatabaseModelBackend
from pynocular.backends.util import backend
from pynocular.database_model import UUID_STR
from pynocular.model import DatabaseModel


class Org(DatabaseModel):
    """A test database model"""

    class Config:
        table_name = "organizations"

    id: int = Field(primary_key=True)
    name: str = Field()


@pytest.mark.asyncio
async def test_org():
    async with Database("postgresql://localhost:5432/jon.drake") as db:
        with backend(PostgresDatabaseModelBackend, db=db):
            orgs: List[Org] = await Org.select()
            assert orgs[0].name == "first"
