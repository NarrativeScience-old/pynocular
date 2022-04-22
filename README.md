# Pynocular

[![](https://img.shields.io/pypi/v/pynocular.svg)](https://pypi.org/pypi/pynocular/) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Pynocular is a lightweight ORM that lets you query your database using Pydantic models and asyncio.

With Pynocular, you can annotate your existing Pydantic models to sync them with the corresponding table in your
database, allowing you to persist changes without ever having to think about the database. Transaction management is
automatically handled for you so you can focus on the important parts of your code. This integrates seamlessly with frameworks that use Pydantic models such as FastAPI.

Features:

- Fully supports asyncio to write to SQL databases through the [databases](https://www.encode.io/databases/) library
- Provides simple methods for basic SQLAlchemy support (create, delete, update, read)
- Contains access to more advanced functionality such as custom SQLAlchemy selects
- Contains helper functions for creating new database tables
- Supports automatic and nested transactions

Table of Contents:

- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [Defining models](#defining-models)
  - [Creating a database and setting the backend](#creating-a-database-and-setting-the-backend)
  - [Creating, reading, updating, and deleting database objects](#creating-reading-updating-and-deleting-database-objects)
  - [Serialization](#serialization)
  - [Special type arguments](#special-type-arguments)
- [Advanced Usage](#advanced-usage)
  - [Tables with compound keys](#tables-with-compound-keys)
  - [Batch operations on tables](#batch-operations-on-tables)
  - [Transactions and asyncio.gather](#transactions-and-asynciogather)
  - [Complex queries](#complex-queries)
  - [Creating database and tables](#creating-database-and-tables)
  - [Unit testing with DatabaseModel](#unit-testing-with-databasemodel)
- [Development](#development)

## Installation

Pynocular requires Python 3.9 or above.

```bash
pip install pynocular
# or
poetry add pynocular
```

## Basic Usage

### Defining models

Pynocular works by augmenting Pydantic's `BaseModel` through the `DatabaseModel` class. Once you define a class that extends `DatabaseModel`, you can proceed to use that model to interface with your specified database table.

```python
from pydantic import Field
from pynocular import DatabaseModel, UUID_STR

class Org(DatabaseModel, table_name="organizations"):

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    tag: Optional[str] = Field(max_length=100)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)
```

### Creating a database and setting the backend

The first step is to create a database pool and set the Pynocular backend. This will tell the models how to persist data.

Use the [databases](https://www.encode.io/databases/) library to create a database connection using the dialect of your choice and pass the database object to `SQLDatabaseModelBackend`.

```python
from pynocular import Database, set_backend, SQLDatabaseModelBackend

async def main():
    # Example below shows how to connect to a locally-running Postgres database
    connection_string = f"postgresql://{db_user_name}:{db_user_password}@localhost:5432/{db_name}?sslmode=disable"
    async with Database(connection_string) as db:
        with set_backend(SQLDatabaseModelBackend(db)):
            print(await Org.get_list())
```

### Creating, reading, updating, and deleting database objects

Once you define a database model and set a backend, you are ready to interact with your database!

```python
# Create a new Org via `create`
org = await Org.create(name="new org", slug="new-org")


# Create a new Org via `save`
org2 = Org(name="new org2", slug="new-org2")
await org2.save()


# Update an org
org.name = "renamed org"
await org.save()


# Delete org
await org.delete()


# Get org
org3 = await Org.get(org2.id)
assert org3 == org2

# Get a list of orgs
orgs = await Org.get_list()

# Get a filtered list of orgs
orgs = await Org.get_list(tag="green")

# Get orgs that have several different tags
orgs = await Org.get_list(tag=["green", "blue", "red"])

# Fetch the latest state of a table in the db
org3.name = "fake name"
await org3.fetch()
assert org3.name == "new org2"

```

### Serialization

Database models have their own serialization functions to convert to and from dictionaries.

```python
# Serializing org with `to_dict()`
org = await Org.create(name="org serialize", slug="org-serialize")
org_dict = org.to_dict()
expected_org_dict = {
    "id": "e64f6c7a-1bd1-4169-b482-189bd3598079",
    "name": "org serialize",
    "slug": "org-serialize",
    "created_at": "2018-01-01 7:03:45",
    "updated_at": "2018-01-01 9:24:12"
}
assert org_dict == expected_org_dict

# De-serializing org with `from_dict()`
new_org = Org.from_dict(expected_org_dict)
assert org == new_org
```

### Special type arguments

With Pynocular you can set fields to be optional and rely on the database server to set its value. This is useful
if you want to let the database autogenerate your primary key or `created_at` and `updated_at` fields
on your table. To do this you must:

- Wrap the typehint in `Optional`
- Provide keyword arguments of `fetch_on_create=True` or `fetch_on_update=True` to the `Field` class

## Advanced Usage

For most use cases, the basic usage defined above should suffice. However, there are certain situations
where you don't necessarily want to fetch each object or you need to do more complex queries that
are not exposed by the `DatabaseModel` interface. Below are some examples of how those situations can
be addressed using Pynocular.

### Tables with compound keys

Pynocular supports tables that use multiple fields as its primary key such as join tables.

```python
from pydantic import Field
from pynocular import DatabaseModel, UUID_STR


class UserSubscriptions(DatabaseModel, table_name="user_subscriptions"):

    user_id: UUID_STR = Field(primary_key=True, fetch_on_create=True)
    subscription_id: UUID_STR = Field(primary_key=True, fetch_on_create=True)
    name: str


user_sub = await UserSub.create(
    user_id="4d4254c4-8e99-45f9-8261-82f87991c659",
    subscription_id="3cc5d476-dbe6-4cc1-9390-49ebd7593a3d",
    name="User 1's subscriptions"
)

# Get the users subscription and confirm its the same
user_sub_get = await UserSub.get(
    user_id="4d4254c4-8e99-45f9-8261-82f87991c659",
    subscription_id="3cc5d476-dbe6-4cc1-9390-49ebd7593a3d",
)
assert user_sub_get == user_sub

# Change a property value like any other object
user_sub_get.name = "change name"
await user_sub_get.save()
```

### Batch operations on tables

Sometimes you want to perform a bulk insert of records into a database table.
This can be handled by the `create_list` function.

```python
org_list = [
    Org(name="org1", slug="org-slug1"),
    Org(name="org2", slug="org-slug2"),
    Org(name="org3", slug="org-slug3"),
]
await Org.create_list(org_list)
```

This function will insert all records into your database table in one batch.

If you have a use case that requires deleting a bunch of records based on some field value, you can use `delete_records`:

```python
# Delete all records with the tag "green"
await Org.delete_records(tag="green")

# Delete all records with if their tag has any of the following: "green", "blue", "red"
await Org.delete_records(tag=["green", "blue", "red"])
```

Sometimes you may want to update the value of a record in a database without having to fetch it first. This can be accomplished by using
the `update_record` function:

```python
await Org.update_record(
    id="05c0060c-ceb8-40f0-8faa-dfb91266a6cf",
    tag="blue"
)
org = await Org.get("05c0060c-ceb8-40f0-8faa-dfb91266a6cf")
assert org.tag == "blue"
```

### Transactions and asyncio.gather

You should avoid using `asyncio.gather` within a database transaction. You can use Pynocular's `gather` function instead, which has the same interface but executes queries sequentially:

```python
from pynocular import get_backend
from pynocular.util import gather

async with get_backend().transaction():
    await gather(
        Org.create(id="abc", name="foo"),
        Org.create(id="def", name="bar"),
    )
```

The reason is that concurrent queries can interfere with each other and result in the error:

```txt
asyncpg.exceptions._base.InterfaceError: cannot perform operation: another operation is in progress
```

See: https://github.com/encode/databases/issues/125#issuecomment-511720013

### Complex queries

Sometimes your application will require performing complex queries, such as getting the count of each unique field value for all records in the table.
Because Pynocular is backed by SQLAlchemy, we can access table columns directly to write pure SQLAlchemy queries as well!

```python
from sqlalchemy import func, select
from pynocular import get_backend

async def generate_org_stats():
    query = (
        select([func.count(Org.column.id), Org.column.tag])
        .group_by(Org.column.tag)
        .order_by(func.count().desc())
    )
    # Get the active backend and open a database transaction
    async with get_backend().transaction():
        result = await conn.execute(query)
        return [dict(row) for row in result]
```

### Creating database and tables

With Pynocular you can use simple Python code to create new databases and database tables. All you need is a working connection string to the database host and a properly defined `DatabaseModel` class. When you define a class that extends `DatabaseModel`, Pynocular creates a SQLAlchemy table under the hood. This can be accessed via the `table` property.

```python
from pynocular import Database
from pynocular.util import create_new_database, create_table

from my_package import Org

async def main():
    connection_string = "postgresql://postgres:XXXX@localhost:5432/postgres"
    await create_new_database(connection_string, "my_new_db")

    connection_string = "postgresql://postgres:XXXX@localhost:5432/my_new_db"
    async with Database(connection_string) as db:
        # Creates a new database and "organizations" table in that database
        await create_table(db, Org.table)

```

### Unit testing with DatabaseModel

Pynocular comes with tooling to write unit tests against your database models, giving you
the ability to test your business logic without the extra work and latency involved in
managing a database. All you have to do is set the backend using the `MemoryDatabaseModelBackend` instead of the SQL backend. You don't need to change any of your database model definitions.

```python
from pynocular import MemoryDatabaseModelBackend, set_backend

from my_package import Org, User

async def main():
    orgs = [
        Org(id=str(uuid4()), name="orgus borgus", slug="orgus_borgus"),
        Org(id=str(uuid4()), name="orgus borgus2", slug="orgus_borgus"),
    ]

    with set_backend(MemoryDatabaseModelBackend()):
        await Org.create_list(orgs)
        fetched_orgs = await Org.get_list(name=orgs[0].name)
        assert orgs[0] == fetched_orgs[0]

    users = [
        User(id=str(uuid4()), username="Bob"),
        User(id=str(uuid4()), username="Sally"),
    ]

    # You can also seed the backend with existing records
    with MemoryDatabaseModelBackend(
        records={
            "orgs": [o.to_dict() for o in orgs],
            "users": [u.to_dict() for u in users],
        }
    ):
        org = await Org.get(orgs[0].id)
        org.name = "new test name"
        await org.save()
```

## Development

To develop Pynocular, install dependencies and enable the pre-commit hook. Make sure to install Python 3.9 and activate it in your shell.

```bash
sudo yum install libffi-devel # Needed for ctypes to install poetry
pyenv install 3.9.12
pyenv shell 3.9.12
```

Install dependencies and enable the pre-commit hook.

```bash
pip install pre-commit poetry
poetry install
pre-commit install
```

Run tests to confirm everything is installed correctly.

```bash
poetry run pytest
```
