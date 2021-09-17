# pynocular

[![](https://img.shields.io/pypi/v/pynocular.svg)](https://pypi.org/pypi/pynocular/) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Pynocular is a lightweight ORM that lets you query your database using Pydantic models and asyncio.

With Pynocular you can decorate your existing Pydantic models to sync them with the corresponding table in your
database, allowing you to persist changes without ever having to think about the database. Transaction management is
automatically handled for you so you can focus on the important parts of your code. This integrates seamlessly with frameworks that use Pydantic models such as FastAPI.

Features:

- Fully supports asyncio to write to SQL databases
- Provides simple methods for basic SQLAlchemy support (create, delete, update, read)
- Contains access to more advanced functionality such as custom SQLAlchemy selects
- Contains helper functions for creating new database tables
- Advanced transaction management system allows you to conditionally put requests in transactions

Table of Contents:

- [Installation](#installation)
- [Guide](#guide)
  - [Basic Usage](#basic-usage)
  - [Advanced Usage](#advanced-usage)
  - [Creating database tables](#creating-database-tables)
- [Development](#development)

## Installation

pynocular requires Python 3.6 or above.

```bash
pip install pynocular
# or
poetry add pynocular
```

## Guide

### Basic Usage

Pynocular works by decorating your base Pydantic model with the function `database_model`. Once decorated
with the proper information, you can proceed to use that model to interface with your specified database table.

The first step is to define a `DBInfo` object. This will contain the connection information to your database.

```python
from pynocular.engines import DatabaseType, DBInfo


# Example below shows how to connect to a locally-running Postgres database
connection_string = f"postgresql://{db_user_name}:{db_user_password}@localhost:5432/{db_name}?sslmode=disable"
)
db_info = DBInfo(DatabaseType.aiopg_engine, connection_string)
```

Pynocular supports connecting to your database through two different asyncio engines; aiopg and asyncpgsa.
You can pick which one you want to use by passing the correct `DatabaseType` enum value into `DBInfo`.

#### Object Management

Once you define a `db_info` object, you are ready to decorate your Pydantic models and interact with your database!

```python
from pydantic import BaseModel, Field
from pynocular.database_model import database_model, UUID_STR

from my_package import db_info

@database_model("organizations", db_info)
class Org(BaseModel):

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    tag: Optional[str] = Field(max_length=100)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)

#### Object management

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

#### Serialization

DatabaseModels have their own serialization functions to convert to and from
dictionaries.

```python
# Serializing org with `to_dict()`
org = Org.create(name="org serialize", slug="org-serialize")
org_dict = org.to_dict()
expected_org_dict = {
    "id": "e64f6c7a-1bd1-4169-b482-189bd3598079",
    "name": "org serialize",
    "slug": "org-serialize",
    "created_at": 2018-01-01 7:03:45
    "updated_at": 2018-01-01 9:24:12
}
assert org_dict == expected_org_dict


# De-serializing org with `from_dict()`
new_org = Org.from_dict(expected_org_dict)
assert org == new_org
```

#### Using Nested DatabaseModels

Pynocular also supports basic object relationships. If your database tables have a
foreign key reference you can leverage that in your pydantic models to increase the
accessibility of those related objects.

```python
from pydantic import BaseModel, Field
from pynocular.database_model import database_model, nested_model, UUID_STR

from my_package import db_info

@database_model("users", db_info)
class User(BaseModel):

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    username: str = Field(max_length=100)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)

@database_model("organizations", db_info)
class Org(BaseModel):

    id: Optional[UUID_STR] = Field(primary_key=True, fetch_on_create=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    # `organizations`.`tech_owner_id` is a foreign key to `users`.`id`
    tech_owner: Optional[nested_model(User, reference_field="tech_owner_id")]
    # `organizations`.`business_owner_id` is a foreign key to `users`.`id`
    business_owner: nested_model(User, reference_field="business_owner_id")
    tag: Optional[str] = Field(max_length=100)

    created_at: Optional[datetime] = Field(fetch_on_create=True)
    updated_at: Optional[datetime] = Field(fetch_on_update=True)


tech_owner = await User.create("tech owner")
business_owner = await User.create("business owner")


# Creating org with only business owner set
org = await Org.create(
    name="org name",
    slug="org-slug",
    business_owner=business_owner
)

assert org.business_owner == business_owner

# Add tech owner
org.tech_owner = tech_owner
await org.save()

# Fetch from the db and check ids
org2 = Org.get(org.id)
assert org2.tech_owner.id == tech_owner.id
assert org2.business_owner.id == business_owner.id

# Swap user roles
org2.tech_owner = business_owner
org2.business_owner = tech_owner
await org2.save()
org3 = await Org.get(org2.id)
assert org3.tech_owner.id == business_owner.id
assert org3.business_owner.id == tech_owner.id


# Serialize org
org_dict = org3.to_dict()
expected_org_dict = {
    "id": org3.id,
    "name": "org name",
    "slug": "org-slug",
    "business_owner_id": tech_owner.id,
    "tech_owner_id": business_owner.id,
    "tag": None,
    "created_at": org3.created_at,
    "updated_at": org3.updated_at
}

assert org_dict == expected_org_dict

```

When using `DatabaseModel.get(..)`, any foreign references will need to be resolved before any properties besides the primary ID can be accessed. If you try to access a property before calling `fetch()` on the nested model, a `NestedDatabaseModelNotResolved` error will be thrown.

```python
org_get = await Org.get(org3.id)
org_get.tech_owner.id # Does not raise `NestedDatabaseModelNotResolved`
org_get.tech_owner.username # Raises `NestedDatabaseModelNotResolved`

org_get = await Org.get(org3.id)
await org_get.tech_owner.fetch()
org_get.tech_owner.username # Does not raise `NestedDatabaseModelNotResolved`
```

Alternatively, calling `DatabaseModel.get_with_refs()` instead of `DatabaseModel.get()` will
automatically fetch the referenced records and fully resolve those objects for you.

```python
org_get_with_refs = await Org.get_with_refs(org3.id)
org_get_with_refs.tech_owner.username # Does not raise `NestedDatabaseModelNotResolved`
```

There are some situations where none of the objects have been persisted to the
database yet. In this situation, you can call `Database.save(include_nested_models=True)`
on the object with the references and it will persist all of them in a transaction.

```python
# We create the objects but dont persist them
tech_owner = User("tech owner")
business_owner = User("business owner")

org = Org(
    name="org name",
    slug="org-slug",
    business_owner=business_owner
)

await org.save(include_nested_models=True)
```

#### Special Type arguments

With Pynocular you can set fields to be optional and set by the database. This is useful
if you want to let the database autogenerate your primary key or `created_at` and `updated_at` fields
on your table. To do this you must:

- Wrap the typehint in `Optional`
- Provide keyword arguments of `fetch_on_create=True` or `fetch_on_update=True` to the `Field` class

### Advanced Usage

For most use cases, the basic usage defined above should suffice. However, there are certain situations
where you don't necessarily want to fetch each object or you need to do more complex queries that
are not exposed by the `DatabaseModel` interface. Below are some examples of how those situations can
be addressed using Pynocular.

#### Tables with compound keys

Pynocular supports tables that use multiple fields as its primary key such as join tables.

```python
from pydantic import BaseModel, Field
from pynocular.database_model import database_model, nested_model, UUID_STR

from my_package import db_info

@database_model("user_subscriptions", db_info)
class UserSubscriptions(BaseModel):

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

#### Batch operations on tables

Sometimes you want to insert a bunch of records into a database and you don't want to do an insert for each one.
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

#### Complex queries

Sometimes your application will require performing complex queries, such as getting the count of each unique field value for all records in the table.
Because Pynocular is backed by SQLAlchemy, we can access table columns directly to write pure SQLAlchemy queries as well!

```python
from sqlalchemy import func, select
from pynocular.engines import DBEngine
async def generate_org_stats():
    query = (
        select([func.count(Org.column.id), Org.column.tag])
        .group_by(Org.column.tag)
        .order_by(func.count().desc())
    )
    async with await DBEngine.transaction(Org._database_info, is_conditional=False) as conn:
        result = await conn.execute(query)
        return [dict(row) async for row in result]
```

NOTE: `DBengine.transaction` is used to create a connection to the database using the credentials passed in.
If `is_conditional` is `False`, then it will add the query to any transaction that is opened in the call chain. This allows us to make database calls
in different functions but still have them all be under the same database transaction. If there is no transaction opened in the call chain it will open
a new one and any subsequent calls underneath that context manager will be added to the new transaction.

If `is_conditional` is `True` and there is no transaction in the call chain, then the connection will not create a new transaction. Instead, the query will be performed without a transaction.

### Creating database and tables

With Pynocular you can use simple python code to create new databases and database tables. All you need is a working connection string to the database host, a DatabaseInfo that contains the information of the database you want to create and a properly decorated pydantic model. When you decorate a Pydantic model with Pynocular, it creates a SQLAlchemy table as a private variable. This can be accessed via the `_table` property
(although accessing private variables is not recommended).

```python
from pynocular.db_util import create_new_database, create_table

from my_package import Org, db_info

connection_string = "postgresql://postgres:XXXX@localhost:5432/postgres?sslmode=disable"

# Creates a new database and "organizations" table in that database
await create_new_database(connection_string, db_info)
await create_table(db_info, Org._table)

```

## Development

To develop pynocular, install dependencies and enable the pre-commit hook:

```bash
pip install pre-commit poetry
poetry install
pre-commit install
```

To run tests:

```bash
poetry run pytest
```
