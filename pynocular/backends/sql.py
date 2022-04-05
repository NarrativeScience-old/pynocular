"""Contains the SQLDatabaseModelBackend class"""

from typing import Any, Dict, List, Optional

from databases import Database
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.backends.base import DatabaseModelBackend, DatabaseModelConfig
from pynocular.exceptions import InvalidFieldValue, InvalidTextRepresentation


class SQLDatabaseModelBackend(DatabaseModelBackend):
    """SQL database model backend

    This backend works with SQL dialects supported by https://www.encode.io/databases/. except sqlite*

    * sqlalchemy does not support the `RETURNING` clause. See https://github.com/sqlalchemy/sqlalchemy/issues/6195
    """

    def __init__(self, db: Database):
        """Initialize a SQLDatabaseModelBackend

        Args:
            db: Database object that has already established a connection

        """
        self.db = db

    async def select(
        self,
        config: DatabaseModelConfig,
        where_expressions: Optional[List[BinaryExpression]] = None,
        order_by: Optional[List[UnaryExpression]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Select a group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the backend query
            order_by: A list of criteria for the order_by clause
            limit: The number of records to return

        Returns:
            list of records

        Raises:
            InvalidFieldValue: The class is missing a database table

        """
        async with self.db.transaction():
            query = config.table.select()
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

            return [dict(record) for record in records]

    async def create_records(
        self, config: DatabaseModelConfig, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Create new group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            records: List of records to persist

        Returns:
            list of newly created records

        """
        if not records:
            return []

        async with self.db.transaction():
            result = await self.db.fetch_all(
                insert(config.table).values(records).returning(config.table)
            )

        return [dict(row) for row in result]

    async def delete_records(
        self, config: DatabaseModelConfig, where_expressions: List[BinaryExpression]
    ) -> None:
        """Delete a group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the backend query

        """
        async with self.db.transaction():
            query = config.table.delete().where(and_(*where_expressions))
            try:
                await self.db.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

    async def update_records(
        self,
        config: DatabaseModelConfig,
        where_expressions: Optional[List[BinaryExpression]],
        values: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Update a group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the backend query
            values: The map of key-values to update all records to that match the
                where_expressions

        Returns:
            the updated database records

        """
        async with self.db.transaction():
            query = (
                config.table.update()
                .where(and_(*where_expressions))
                .values(values)
                .returning(config.table)
            )
            try:
                results = await self.db.execute(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

            return [dict(record) for record in await results]

    async def upsert(
        self,
        config: DatabaseModelConfig,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upsert a single database record

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            record: The record to update

        Returns:
            the updated record

        """
        async with self.db.transaction():
            query = (
                insert(config.table)
                .values(record)
                .on_conflict_do_update(
                    index_elements=config.primary_key_names, set_=record
                )
                .returning(config.table)
            )
            updated_record = await self.db.fetch_one(query)
            return dict(updated_record)
