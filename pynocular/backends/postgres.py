from operator import mod
import pdb
from sqlite3 import DatabaseError
from typing import Any, Dict, List, Optional, Type
from databases import Database

from sqlalchemy import (
    and_,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.engines import DBEngine
from pynocular.exceptions import (
    DatabaseModelMissingField,
    InvalidFieldValue,
    InvalidTextRepresentation,
    NestedDatabaseModelNotResolved,
)
from pynocular.backends.base import DatabaseModelBackend


class PostgresDatabaseModelBackend(DatabaseModelBackend):
    """Postgres backend"""

    def __init__(self, model_cls: Any, db: Database):
        self.model_cls = model_cls
        self.db = db

    async def select(
        self,
        where_expressions: Optional[List[BinaryExpression]] = None,
        order_by: Optional[List[UnaryExpression]] = None,
        limit: Optional[int] = None,
    ) -> List["PostgresDatabaseModelBackend"]:
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
        async with self.db.transaction():
            query = self.model_cls._table.select()
            if where_expressions is not None and len(where_expressions) > 0:
                query = query.where(and_(*where_expressions))
            if order_by is not None and len(order_by) > 0:
                query = query.order_by(*order_by)
            if limit is not None and limit > 0:
                query = query.limit(limit)

            try:
                records = await self.db.fetch_all(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

            return [self.model_cls.from_dict(dict(record)) for record in records]

    @classmethod
    async def create_list(
        cls, models: List["PostgresDatabaseModelBackend"]
    ) -> List["PostgresDatabaseModelBackend"]:
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
    async def update(
        cls, where_expressions: Optional[List[BinaryExpression]], values: Dict[str, Any]
    ) -> List["PostgresDatabaseModelBackend"]:
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

    async def save(self, include_nested_models=False) -> None:
        """Update the database record this object represents with its current state

        Args:
            include_nested_models: If True, any nested models should get saved before
                this object gets saved

        """

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
            # If flag is set, first try to persist any nested models. This needs to
            # happen inside of the transaction so if something fails everything gets
            # rolled back
            if include_nested_models:
                for attr_name in self._nested_model_attributes:
                    try:
                        obj = getattr(self, attr_name)
                        if obj is not None:
                            await obj.save()
                    except NestedDatabaseModelNotResolved:
                        # If the object was never resolved than it already exists in the
                        # DB and the DB has the latest state
                        continue

            record = await conn.execute(
                insert(self._table)
                .values(dict_self)
                .on_conflict_do_update(index_elements=primary_key_names, set_=dict_self)
                .returning(self._table)
            )

            row = await record.fetchone()

            for field in self._db_managed_fields:
                setattr(self, field, row[field])
