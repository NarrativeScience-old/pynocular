"""Contains DatabaseModel class"""

from datetime import datetime
from enum import Enum, EnumMeta
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from uuid import UUID as stdlib_uuid

from pydantic import BaseModel, PositiveFloat, PositiveInt, UUID4
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
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.backends.base import DatabaseModelConfig
from pynocular.backends.context import get_backend
from pynocular.exceptions import (
    DatabaseModelMisconfigured,
    DatabaseModelMissingField,
    DatabaseRecordNotFound,
    InvalidMethodParameterization,
)
from pynocular.util import UUID_STR


class DatabaseModel(BaseModel):
    """DatabaseModel defines a Pydantic model that abstracts away backend storage

    This allows us to use the same object for both database queries and HTTP requests.
    Methods on the DatabaseModel call through to the active backend implementation. The
    backend handle queries and storage.
    """

    if TYPE_CHECKING:
        # Set by _process_config
        _config: DatabaseModelConfig

    @staticmethod
    def _process_config(cls, table_name: str) -> DatabaseModelConfig:
        """Process configuration passed into the DatabaseModel subclass signature

        The primary job of this method is to generate a DatabaseModelConfig instance,
        specifically a SQLAlchemy table definition for backend implementations to
        leverage.

        Returns:
            DatabaseModelConfig instance

        Raises:
            DatabaseModelMisconfigured: When the class does not define certain
                properties or cannot be converted to a SQLAlchemy Table

        """
        # We may have times where we need a compound primary key.
        # We store each one into this list and have our query functions
        # handle using it
        primary_keys: List[Column] = []

        # Some fields are exclusively produced by the database server
        # For all save operations, we need to get those values from the database
        # These are the server_default and server_onupdate functions in SQLAlchemy
        db_managed_fields: List[str] = []

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
                db_managed_fields.append(name)

            if fetch_on_update:
                column.server_onupdate = FetchedValue()
                db_managed_fields.append(name)

            if is_primary_key:
                primary_keys.append(column)

            columns.append(column)

        # Define metadata for the database connection on the class level so we don't
        # have to recalculate the table for each database call
        table = Table(table_name, MetaData(), *columns)

        return DatabaseModelConfig(
            fields={**cls.__fields__},
            db_managed_fields=db_managed_fields,
            primary_keys=primary_keys,
            table=table,
        )

    def __init_subclass__(cls, table_name: str, **kwargs) -> None:
        """Hook for processing class configuration when DatabaseModel is subclassed

        Args:
            table_name: Name of the database table

        """
        super().__init_subclass__(**kwargs)
        cls._config = DatabaseModel._process_config(cls, table_name)

    @classmethod
    @property
    def table(cls) -> Table:
        """Returns SQLAlchemy table object for the model"""
        return cls._config.table

    @classmethod
    @property
    def columns(cls) -> ImmutableColumnCollection:
        """Reference to the model's table's column collection"""
        return cls.table.c

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
        return cls(**_dict)

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

            if not include_keys or prop_name in include_keys:
                _dict[prop_name] = prop_value

        return _dict

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
            or (len(args) == 1 and len(cls._config.primary_keys) > 1)
            or (len(args) == 0 and len(kwargs) == 0)
        ):
            raise InvalidMethodParameterization("get", args=args, kwargs=kwargs)

        if len(args) == 1:
            primary_key_dict = {cls._config.primary_keys[0].name: args[0]}
        else:
            primary_key_dict = kwargs

        original_primary_key_dict = primary_key_dict.copy()
        where_expressions = []
        for primary_key in cls._config.primary_keys:
            primary_key_value = primary_key_dict.pop(primary_key.name)
            where_expressions.append(primary_key == primary_key_value)

        records = await cls.select(where_expressions=where_expressions, limit=1)
        if len(records) == 0:
            raise DatabaseRecordNotFound(
                cls._config.table.name, **original_primary_key_dict
            )

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
        if len(self._config.primary_keys) > 1:
            return None

        return getattr(self, self._config.primary_keys[0].name)

    async def fetch(self) -> None:
        """Gets the latest of the object from the database and updates itself"""
        get_params = {
            primary_key.name: getattr(self, primary_key.name)
            for primary_key in self._config.primary_keys
        }
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
        where_expressions = []
        for field_name, db_field_value in kwargs.items():
            try:
                db_field = getattr(cls._config.table.c, field_name)
            except AttributeError:
                raise DatabaseModelMissingField(cls.__name__, field_name)

            if isinstance(db_field_value, list):
                exp = db_field.in_(db_field_value)
            else:
                exp = db_field == db_field_value

            where_expressions.append(exp)

        return await cls.select(where_expressions=where_expressions)

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
        records = await get_backend().select(
            cls._config,
            where_expressions=where_expressions,
            order_by=order_by,
            limit=limit,
        )
        return [cls.from_dict(record) for record in records]

    @classmethod
    async def create_list(cls, models: List["DatabaseModel"]) -> List["DatabaseModel"]:
        """Create new batch of records in one query

        This will mutate the provided models to include db managed column values.

        Args:
            models: List of database models to persist

        Returns:
            list of new database models that have been saved

        """
        values = []
        for model in models:
            dict_obj = model.to_dict()

            # Remove any fields that the database calculates
            for field in cls._config.db_managed_fields:
                del dict_obj[field]

            # Remove keys for primary keys that don't have a value. This indicates that
            # the backend will generate new values.
            for field in cls._config.primary_keys:
                if dict_obj.get(field.name) is None:
                    del dict_obj[field.name]

            values.append(dict_obj)

        records = await get_backend().create_records(cls._config, values)

        # Set db managed column information on the object
        for record, model in zip(records, models):
            for column in cls._config.db_managed_fields:
                col_val = record.get(column)
                if col_val is not None:
                    setattr(model, column, col_val)

            for field in cls._config.primary_keys:
                value = record.get(field.name)
                if value is not None:
                    setattr(model, field.name, value)

        return models

    @classmethod
    async def delete_records(cls, **kwargs: Any) -> Optional[int]:
        """Execute a DELETE on a DatabaseModel with the provided kwargs

        Args:
            kwargs: The filterable key/value pairs for the where clause. These will be
                `and`ed together

        Returns:
            number of records deleted (or None if the backend does not support)

        Raises:
            DatabaseModelMisconfigured: The class is missing a database table
            DatabaseModelMissingField: One of the fields provided in the query does not
                exist on the database table

        """
        where_expressions = []
        for field_name, db_field_value in kwargs.items():
            try:
                db_field = getattr(cls._config.table.c, field_name)
            except AttributeError:
                raise DatabaseModelMissingField(cls.__name__, field_name)

            if isinstance(db_field_value, list):
                exp = db_field.in_(db_field_value)
            else:
                exp = db_field == db_field_value

            where_expressions.append(exp)

        return await get_backend().delete_records(cls._config, where_expressions)

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
        for primary_key in cls._config.primary_keys:
            primary_key_value = kwargs.pop(primary_key.name)
            where_expressions.append(primary_key == primary_key_value)
            primary_key_dict[primary_key.name] = primary_key_value

        updated_records = await cls.update(where_expressions, kwargs)
        if len(updated_records) == 0:
            raise DatabaseRecordNotFound(cls._config.table.name, **primary_key_dict)
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
        return [
            cls.from_dict(record)
            for record in await get_backend().update_records(
                cls._config, where_expressions=where_expressions, values=values
            )
        ]

    async def save(self) -> None:
        """Update the database record this object represents with its current state"""
        dict_self = self.to_dict()
        for field in self._config.db_managed_fields:
            if field in self._config.primary_key_names and dict_self[field] is not None:
                continue

            # Remove any fields that the database calculates
            del dict_self[field]

        record = await get_backend().upsert(
            self._config,
            dict_self,
        )
        for field in self._config.db_managed_fields:
            setattr(self, field, record[field])

    async def delete(self) -> None:
        """Delete this record from the database"""
        where_expressions = [
            getattr(self._config.table.c, pkey.name) == getattr(self, pkey.name)
            for pkey in self._config.primary_keys
        ]
        return await get_backend().delete_records(self._config, where_expressions)
