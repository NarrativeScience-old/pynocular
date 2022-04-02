import asyncio
from contextlib import contextmanager
from datetime import datetime
from enum import Enum, EnumMeta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Type
from uuid import UUID as stdlib_uuid

from pydantic import UUID4, BaseModel, PositiveFloat, PositiveInt
from sqlalchemy import (
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
from sqlalchemy.dialects.postgresql import JSONB, UUID as sqlalchemy_uuid
from sqlalchemy.schema import FetchedValue
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression
from sqlalchemy.sql.base import ImmutableColumnCollection
from pynocular.backends.base import DatabaseModelBackend
from pynocular.backends.util import get_backend
from pynocular.database_model import UUID_STR

from pynocular.exceptions import (
    DatabaseModelMisconfigured,
    DatabaseModelMissingField,
    DatabaseRecordNotFound,
    InvalidMethodParameterization,
)


class DatabaseModel(BaseModel):

    if TYPE_CHECKING:
        # Populated by _initialize_table. Defined here to help IDEs only
        _primary_keys: List[Column]
        _db_managed_fields: List[str]
        _nested_model_attributes: Set[str]
        _nested_attr_table_field_map: Dict[str, str]
        _nested_table_field_attr_map: Dict[str, str]
        _table: Table
        columns: ImmutableColumnCollection

        # Set by backend method
        _backend: DatabaseModelBackend

    @staticmethod
    def _initialize_table(cls) -> "DatabaseModel":
        """Returns a SQLAlchemy table definition to expose SQLAlchemy functions

        This method should cache the Table on the __table__ class property.
        We don't want to have to recalculate the table for every SQL call,
        so it's desirable to cache this at the class level.

        Returns:
            A Table object based on the Field properties defined from the Pydantic model

        Raises:
            DatabaseModelMisconfigured: When the class does not defined certain properties;
                or cannot be converted to a Table

        """
        # We may have times where we need a compound primary key.
        # We store each one into this list and have our query functions
        # handle using it
        _primary_keys: List[Column] = []

        # Some fields are exclusively produced by the database server
        # For all save operations, we need to get those values from the database
        # These are the server_default and server_onupdate functions in SQLAlchemy
        _db_managed_fields: List[str] = []

        # The following tables track which attributes on the model are nested model
        # references
        # Some nested model attributes may have different names than their actual db table;
        # For example; on an App we may have an `org` attribute but the db field is
        # `organzation_id`

        # In order to manage this we also need maps from attribute name to table_field_name
        # and back
        _nested_model_attributes: Set[str] = set()
        _nested_attr_table_field_map: Dict[str, str] = {}
        _nested_table_field_attr_map: Dict[str, str] = {}

        columns: List[Column] = []
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
            elif field.type_.__class__ == EnumMeta:
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
                _nested_model_attributes.add(name)
                # If the field name on the NestedModel type is not None, use that for the
                # column name
                if field.type_.reference_field_name is not None:
                    _nested_attr_table_field_map[
                        name
                    ] = field.type_.reference_field_name
                    _nested_table_field_attr_map[
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
                _db_managed_fields.append(name)

            if fetch_on_update:
                column.server_onupdate = FetchedValue()
                _db_managed_fields.append(name)

            if is_primary_key:
                _primary_keys.append(column)

            columns.append(column)

        # Define metadata for the database connection on the class level so we don't
        # have to recalculate the table for each database call
        _table = Table(cls.Config.table_name, MetaData(), *columns)

        # _database_info: DBInfo = None

        cls._db_managed_fields = _db_managed_fields
        cls._nested_table_field_attr_map = _nested_table_field_attr_map
        cls._primary_keys = _primary_keys
        cls._table = _table
        cls.columns = _table.c

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        DatabaseModel._initialize_table(cls)
        return cls

    @classmethod
    def _backend(cls) -> None:
        cls._backend = backend_cls(cls, **kwargs)
        try:
            yield
        finally:
            cls._backend = None

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
        return await get_backend(cls).select(
            where_expressions=where_expressions, order_by=order_by, limit=limit
        )

    @classmethod
    async def create_list(cls, models: List["DatabaseModel"]) -> List["DatabaseModel"]:
        """Create new batch of records in one query

        This will mutate the provided models to include db managed column values.

        Args:
            models: List of database models to persist

        Returns:
            list of new database models that have been saved

        """
        pass

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
        pass

    @classmethod
    async def update_record(cls, **kwargs: Any) -> "DatabaseModel":
        """Update a record associated with this DatabaseModel

        Notes:
            the primary key must be in the kwargs

        Args:
            kwargs: The values to update.

        Returns:
            The updated DatabaseModel

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
        pass

    async def save(self, include_nested_models=False) -> None:
        """Update the database record this object represents with its current state

        Args:
            include_nested_models: If True, any nested models should get saved before
                this object gets saved

        """
        pass

    async def delete(self) -> None:
        """Delete this record from the database"""
        pass
