"""Base Model class that implements CRUD methods for database entities based on Pydantic dataclasses"""
import asyncio
from datetime import datetime
from enum import Enum, EnumMeta
import inspect
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Set, Union
from uuid import UUID as stdlib_uuid

from aenum import Enum as AEnum, EnumMeta as AEnumMeta
from pydantic import BaseModel, PositiveFloat, PositiveInt
from pydantic.types import UUID4
from sqlalchemy import (
    and_,
    Boolean,
    Column,
    Enum as SQLEnum,
    Float,
    Integer,
    MetaData,
    Table,
    TIMESTAMP,
    VARCHAR,
)
from sqlalchemy.dialects.postgresql import insert, JSONB, UUID as sqlalchemy_uuid
from sqlalchemy.schema import FetchedValue
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.engines import DBEngine, DBInfo
from pynocular.exceptions import (
    DatabaseModelMisconfigured,
    DatabaseModelMissingField,
    DatabaseRecordNotFound,
    InvalidFieldValue,
    InvalidMethodParameterization,
    InvalidTextRepresentation,
)
from pynocular.nested_database_model import NestedDatabaseModel


def is_valid_uuid(string: str) -> bool:
    """Check if a string is a valid UUID

    Args:
        string: the string to check

    Returns:
        Whether or not the string is a well-formed UUIDv4

    """
    try:
        stdlib_uuid(string, version=4)
        return True
    except (TypeError, AttributeError, ValueError):
        return False


class UUID_STR(str):
    """A string that represents a UUID4 value"""

    @classmethod
    def __get_validators__(cls) -> Generator:
        """Get the validators for the given class"""
        yield cls.validate

    @classmethod
    def validate(cls, v: Any) -> str:
        """Function to validate the value

        Args:
            v: The value to validate

        """
        if isinstance(v, stdlib_uuid) or (isinstance(v, str) and is_valid_uuid(v)):
            return str(v)
        else:
            raise ValueError("invalid UUID string")


def nested_model(
    db_model_class: "DatabaseModel", reference_field: str = None
) -> Callable:
    """Generate a NestedModel class with dynamic model references"""

    class NestedModel:
        """NestedModel type for NestedDatabaseModels"""

        reference_field_name = reference_field

        @classmethod
        def __get_validators__(cls) -> Generator:
            """Get the validators for the given class"""
            yield cls.validate

        @classmethod
        def validate(cls, v: Union[UUID_STR, "DatabaseModel"]) -> NestedDatabaseModel:
            """Validate value and generate a nested database model"""
            # If value is a uuid then create a NestedDatabaseModel, otherwise just
            # Set the DatabaseModel as the value
            if is_valid_uuid(v):
                return NestedDatabaseModel(db_model_class, v)
            else:
                return NestedDatabaseModel(db_model_class, v.get_primary_id(), v)

    return NestedModel


def database_model(table_name: str, database_info: DBInfo) -> "DatabaseModel":
    """Decorator that adds SQL functionality to Pydantic BaseModel objects

    Args:
        table_name: Name of the table this model represents in the database
        database_info: Database connection info for the database to connect to

    Raises:
        DatabaseModelMisconfigured: Raised when class with this decorator is not a pydantic.BaseModel
            subclass. We depend on the class implementing a some specific things and currently don't
            support any other type of dataclass.

    """

    def wrapped(cls):
        if BaseModel not in inspect.getmro(cls):
            raise DatabaseModelMisconfigured(
                "Model is not subclass of pydantic.BaseModel"
            )

        cls.__bases__ += (DatabaseModel,)
        cls.initialize_table(table_name, database_info)

        return cls

    return wrapped


class DatabaseModel:
    """Adds database functionality to a Pydantic BaseModel

    A DatabaseModel is a Pydantic based model along with a SQLAlchemy
    table object. This allows us to use the same object for both
    database queries and HTTP requests

    """

    # Define metadata for the database connection on the class level so we don't
    # have to recaluclate the table for each database call
    _table: Table = None
    _database_info: DBInfo = None

    # We may have times where we need a compound primary key.
    # We store each one into this list and have our query functions
    # handle using it
    _primary_keys: List[Column] = None

    # Some fields are exclusively produced by the database server
    # For all save operations, we need to get those values from the database
    # These are the server_default and server_onupdate functions in SQLAlchemy
    _db_managed_fields: List[str] = None

    # The following tables track which attributes on the model are nested model
    # references
    # Some nested model attributes may have different names than their actual db table;
    # For example; on an App we may have an `org` attribute but the db field is
    # `organzation_id`

    # In order to manage this we also need maps from attribute name to table_field_name
    # and back
    _nested_model_attributes: Set[str] = None
    _nested_attr_table_field_map: Dict[str, str] = None
    _nested_table_field_attr_map: Dict[str, str] = None

    # This can be used to access the table when defining where expressions
    columns: ImmutableColumnCollection = None

    @classmethod
    def initialize_table(cls, table_name: str, database_info: DBInfo) -> None:
        """Returns a SQLAlchemy table definition to expose SQLAlchemy functions

        This method should cache the Table on the __table__ class property.
        We don't want to have to recaluclate the table for every SQL call,
        so it's desirable to cache this at the class level.

        Returns:
            A Table object based on the Field properties defined from the Pydantic model

        Raises:
            DatabaseModelMisconfigured: When the class does not defined certain properties;
                or cannot be converted to a Table

        """
        cls._primary_keys = []
        cls._database_info = database_info
        cls._db_managed_fields = []
        cls._nested_attr_table_field_map = {}
        cls._nested_table_field_attr_map = {}
        cls._nested_model_attributes = set()

        columns = []
        for field in cls.__fields__.values():
            name = field.name
            is_nullable = not field.required
            is_primary_key = field.field_info.extra.get("primary_key", False)
            fetch_on_create = field.field_info.extra.get("fetch_on_create", False)
            fetch_on_update = field.field_info.extra.get("fetch_on_update", False)

            if field.type_ is str:
                type = VARCHAR
            elif field.type_.__name__ == "ConstrainedStrValue":
                # This is because pydantic is doing some kind of dynamic type construction.
                # See: https://github.com/samuelcolvin/pydantic/blob/e985857e5a9ede8d346b010a5a039aa84a089826/pydantic/types.py#L245-L263
                length = field.field_info.max_length
                type = VARCHAR(length)
            elif (
                field.type_ in (int, PositiveInt)
                or field.type_.__name__ == "ConstrainedIntValue"
            ):
                type = Integer
            elif (
                field.type_ in (float, PositiveFloat)
                or field.type_.__name__ == "ConstrainedFloatValue"
            ):
                type = Float
            elif field.type_.__class__ in (AEnumMeta, EnumMeta):
                type = SQLEnum(field.type_)
            elif field.type_ is bool:
                type = Boolean
            elif field.type_ in (dict, Dict):
                type = JSONB(none_as_null=True)
            elif field.type_ in (UUID4, stdlib_uuid, UUID_STR):
                type = sqlalchemy_uuid()
            elif field.type_ is datetime:
                type = TIMESTAMP(timezone=True)
            elif field.type_.__name__ == "NestedModel":
                cls._nested_model_attributes.add(name)
                # If the field name on the NestedModel type is not None, use that for the
                # column name
                if field.type_.reference_field_name is not None:
                    cls._nested_attr_table_field_map[
                        name
                    ] = field.type_.reference_field_name
                    cls._nested_table_field_attr_map[
                        field.type_.reference_field_name
                    ] = name
                    name = field.type_.reference_field_name

                # Assume all IDs are UUIDs for now
                type = sqlalchemy_uuid()
            # TODO - how are people using this today? Is there a class we need to make or can we reuse one
            # elif field.type_ is bit:
            #     type = Bit
            else:
                raise DatabaseModelMisconfigured(f"Unsupported type {field.type_}")

            column = Column(
                name, type, primary_key=is_primary_key, nullable=is_nullable
            )

            if fetch_on_create:
                column.server_default = FetchedValue()
                cls._db_managed_fields.append(name)

            if fetch_on_update:
                column.server_onupdate = FetchedValue()
                cls._db_managed_fields.append(name)

            if is_primary_key:
                cls._primary_keys.append(column)

            columns.append(column)

        cls._table = Table(table_name, MetaData(), *columns)
        cls.columns = cls._table.c

    @classmethod
    async def get_with_refs(cls, *args: Any, **kwargs: Any) -> "DatabaseModel":
        """Gets the DatabaseModel associated with any nested key references resolved

        Args:
            args: The column id for the object's primary key
            kwargs: The columns and ids that make up the object's composite primary key

        Returns:
            A DatabaseModel object representing the record in the db if one exists

        """
        obj = await cls.get(*args, **kwargs)
        gatherables = [
            (getattr(obj, prop_name)).fetch()
            for prop_name in cls._nested_model_attributes
        ]
        await asyncio.gather(*gatherables)

        return obj

    @classmethod
    async def get(cls, *args: Any, **kwargs: Any) -> "DatabaseModel":
        """Gets the DatabaseModel for the given primary key value(s)

        Args:
            args: The column id for the object's primary key
            kwargs: The columns and ids that make up the object's composite primary key

        Returns:
            A DatabaseModel object representing the record in the db if one exists

        Raises:
            InvalidMethodParameterization: An invalid parameter configuration was passed in.
                This method should only receive one arg or >= one kwargs. Any other
                combination of parameters is invalid.

        """
        if (
            (len(args) > 1)
            or (len(args) == 1 and len(kwargs) > 0)
            or (len(args) == 1 and len(cls._primary_keys) > 1)
            or (len(args) == 0 and len(kwargs) == 0)
        ):
            raise InvalidMethodParameterization("get", args=args, kwargs=kwargs)

        if len(args) == 1:
            primary_key_dict = {cls._primary_keys[0].name: args[0]}
        else:
            primary_key_dict = kwargs

        original_primary_key_dict = primary_key_dict.copy()
        where_expressions = []
        for primary_key in cls._primary_keys:
            primary_key_value = primary_key_dict.pop(primary_key.name)
            where_expressions.append(primary_key == primary_key_value)

        records = await cls.select(where_expressions=where_expressions, limit=1)
        if len(records) == 0:
            raise DatabaseRecordNotFound(cls._table.name, **original_primary_key_dict)

        return records[0]

    @classmethod
    async def get_list(cls, **kwargs: Any) -> List["DatabaseModel"]:
        """Fetches the DatabaseModel for based on the provided kwargs

        Args:
            kwargs: The filterable key/value pairs for the where clause. These will be
                `and`ed together

        Returns:
            List of DatabaseModel objects

        Raises:
            DatabaseModelMisconfigured: The class is missing a database table
            DatabaseModelMissingField: One of the fields provided in the query does not
                exist on the database table

        """
        where_clause_list = []
        for field_name, db_field_value in kwargs.items():
            db_field_name = cls._nested_attr_table_field_map.get(field_name, field_name)

            try:
                db_field = getattr(cls._table.c, db_field_name)
            except AttributeError:
                raise DatabaseModelMissingField(cls.__name__, db_field_name)

            if isinstance(db_field_value, list):
                exp = db_field.in_(db_field_value)
            else:
                exp = db_field == db_field_value

            where_clause_list.append(exp)

        return await cls.select(where_expressions=where_clause_list)

    @classmethod
    async def select(
        cls,
        where_expressions: Optional[List[BinaryExpression]] = None,
        order_by: Optional[List[UnaryExpression]] = None,
        limit: Optional[int] = None,
    ) -> List["DatabaseModel"]:
        """Execute a SELECT on the DatabaseModel table with the given parameters

        Args:
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the SELECT
            order_by: A list of criteria for the order_by clause
            limit: The number of instances to return

        Returns:
            A list of DatabaseModel instances

        Raises:
            DatabaseModelMisconfigured: The class is missing a database table

        """
        async with (
            await DBEngine.transaction(cls._database_info, is_conditional=True)
        ) as conn:
            query = cls._table.select()
            if where_expressions is not None and len(where_expressions) > 0:
                query = query.where(and_(*where_expressions))
            if order_by is not None and len(order_by) > 0:
                query = query.order_by(*order_by)
            if limit is not None and limit > 0:
                query = query.limit(limit)

            try:
                result = await conn.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)
            records = await result.fetchall()

            return [cls.from_dict(dict(record)) for record in records]

    @classmethod
    async def create(cls, **data) -> "DatabaseModel":
        """Create a new instance of the this DatabaseModel and save it

        Args:
            kwargs: The parameters for the instance

        Returns:
            The new DatabaseModel instance

        """
        new = cls(**data)
        await new.save()

        return new

    @classmethod
    async def create_list(cls, models: List["DatabaseModel"]) -> List["DatabaseModel"]:
        """Create new batch of records in one query

        This will mutate the provided models to include db managed column values.

        Args:
            models: List of database models to persist

        Returns:
            list of new database models that have been saved

        """
        if not models:
            return []

        values = []
        for model in models:
            dict_obj = model.to_dict()
            for field in cls._db_managed_fields:
                # Remove any fields that the database calculates
                del dict_obj[field]
            values.append(dict_obj)

        async with (
            await DBEngine.transaction(cls._database_info, is_conditional=False)
        ) as conn:
            result = await conn.execute(
                insert(cls._table).values(values).returning(cls._table)
            )
            # Set db managed column information on the object
            rows = await result.fetchall()
            for row, model in zip(rows, models):
                record_dict = dict(row)
                for column in cls._db_managed_fields:
                    col_val = record_dict.get(column)
                    if col_val is not None:
                        setattr(model, column, col_val)

        return models

    @classmethod
    async def delete_records(cls, **kwargs: Any) -> None:
        """Execute a DELETE on a DatabaseModel with the provided kwargs

        Args:
            kwargs: The filterable key/value pairs for the where clause. These will be
                `and`ed together

        Raises:
            DatabaseModelMisconfigured: The class is missing a database table
            DatabaseModelMissingField: One of the fields provided in the query does not
                exist on the database table

        """
        where_clause_list = []
        for field_name, db_field_value in kwargs.items():
            db_field_name = cls._nested_attr_table_field_map.get(field_name, field_name)

            try:
                db_field = getattr(cls._table.c, db_field_name)
            except AttributeError:
                raise DatabaseModelMissingField(cls.__name__, db_field_name)

            if isinstance(db_field_value, list):
                exp = db_field.in_(db_field_value)
            else:
                exp = db_field == db_field_value

            where_clause_list.append(exp)

        async with (
            await DBEngine.transaction(cls._database_info, is_conditional=False)
        ) as conn:
            query = cls._table.delete().where(and_(*where_clause_list))
            try:
                await conn.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

    @classmethod
    async def update_record(cls, **kwargs: Any) -> "DatabaseModel":
        """Update a record associated with this DatabaseModel

        Notes:
            the primary key must be in the kwargs

        Args:
            kwargs: The values to update.

        Returns:
            The updated database object

        """
        where_expressions = []
        primary_key_dict = {}
        for primary_key in cls._primary_keys:
            primary_key_value = kwargs.pop(primary_key.name)
            where_expressions.append(primary_key == primary_key_value)
            primary_key_dict[primary_key.name] = primary_key_value

        modified_kwargs = {}
        for field_name, value in kwargs.items():
            db_field_name = cls._nested_attr_table_field_map.get(field_name, field_name)
            modified_kwargs[db_field_name] = value

        updated_records = await cls.update(where_expressions, modified_kwargs)
        if len(updated_records) == 0:
            raise DatabaseRecordNotFound(cls._table.name, **primary_key_dict)
        return updated_records[0]

    @classmethod
    async def update(
        cls, where_expressions: Optional[List[BinaryExpression]], values: Dict[str, Any]
    ) -> List["DatabaseModel"]:
        """Execute an UPDATE on a DatabaseModel table with the given parameters

        Args:
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the UPDATE
            values: The field and values to update all records to that match the
                where_expressions

        Returns:
            The updated DatabaseModels

        Raises:
            DatabaseModelMisconfigured: The class is missing a database table

        """
        async with (
            await DBEngine.transaction(cls._database_info, is_conditional=False)
        ) as conn:
            query = (
                cls._table.update()
                .where(and_(*where_expressions))
                .values(**values)
                .returning(cls._table)
            )
            try:
                results = await conn.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

            return [cls.from_dict(dict(record)) for record in await results.fetchall()]

    async def save(self) -> None:
        """Update the database record this object represents with its current state"""
        dict_self = self.to_dict()

        primary_key_names = [primary_key.name for primary_key in self._primary_keys]

        for field in self._db_managed_fields:
            if field in primary_key_names and dict_self[field] is not None:
                continue

            # Remove any fields that the database calculates
            del dict_self[field]

        async with (
            await DBEngine.transaction(self._database_info, is_conditional=False)
        ) as conn:
            record = await conn.execute(
                insert(self._table)
                .values(dict_self)
                .on_conflict_do_update(index_elements=primary_key_names, set_=dict_self)
                .returning(self._table)
            )

            row = await record.fetchone()

            for field in self._db_managed_fields:
                setattr(self, field, row[field])

    def get_primary_id(self) -> Any:
        """Standard interface for returning the id of a field

        This assumes that there is a single primary id, otherwise this returns `None`

        Returns:
            The ID value for this DatabaseModel instance

        """
        if len(self._primary_keys) > 1:
            return None

        return getattr(self, self._primary_keys[0].name)

    async def fetch(self, resolve_references: bool = False) -> None:
        """Gets the latest of the object from the database and updates itself

        Args:
            resolve_references: If True, resolve any nested key references

        """
        # Get the latest version of self
        get_params = {
            primary_key.name: getattr(self, primary_key.name)
            for primary_key in self._primary_keys
        }
        if resolve_references:
            new_self = await self.get_with_refs(**get_params)
        else:
            new_self = await self.get(**get_params)

        for attr_name, new_attr_val in new_self.dict().items():
            setattr(self, attr_name, new_attr_val)

    async def delete(self) -> None:
        """Delete this record from the database"""

        async with (
            await DBEngine.transaction(self._database_info, is_conditional=False)
        ) as conn:
            where_expressions = [
                getattr(self._table.c, pkey.name) == getattr(self, pkey.name)
                for pkey in self._primary_keys
            ]
            query = self._table.delete().where(and_(*where_expressions))
            try:
                await conn.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

    @classmethod
    def from_dict(cls, _dict: Dict[str, Any]) -> "DatabaseModel":
        """Instantiate a DatabaseModel object from a dict record

        Note:
            This is the base implementation and is set up so classes that subclass this
            one don't have to make this boilerplate if they don't need to

        Args:
            _dict: The dictionary form of the DatabaseModel

        Returns:
            The DatabaseModel object

        """
        modified_dict = {}
        for key, value in _dict.items():
            modified_key = cls._nested_table_field_attr_map.get(key, key)
            modified_dict[modified_key] = value
        return cls(**modified_dict)

    def to_dict(
        self, serialize: bool = False, include_keys: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Create a dict from the DatabaseModel object

        Note:
            This implementation is only valid if __base_props__ is set for the instance

        Args:
            serialize: A flag determining whether or not to serialize enum types into
                strings
            include_keys: Set of keys that should be included in the results. If not
                provided or empty, all keys will be included.

        Returns:
            A dict of the DatabaseObject object

        Raises:
            NotImplementedError: This function implementation is being used without
                __base_props__ being set

        """
        _dict = {}
        for prop_name, prop_value in self.dict().items():
            if serialize:
                if isinstance(prop_value, Enum):
                    prop_value = prop_value.name
                elif isinstance(prop_value, AEnum):
                    prop_value = prop_value.value

            if prop_name in self._nested_model_attributes:
                # self.dict() will serialize any BaseModels into a dict so fetch the
                # actual object from self
                temp_prop_value = getattr(self, prop_name)
                prop_name = self._nested_attr_table_field_map.get(prop_name, prop_name)
                # temp_prop_value can be `None` if the nested key is optional
                if temp_prop_value is not None:
                    prop_value = temp_prop_value.get_primary_id()

            if not include_keys or prop_name in include_keys:
                _dict[prop_name] = prop_value

        return _dict
