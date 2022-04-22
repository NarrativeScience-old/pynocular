"""Contains the SQLDatabaseModelBackend class"""

import logging
from typing import Any, Dict, List, Optional

from databases import Database
from databases.core import Transaction
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.backends.base import DatabaseModelBackend, DatabaseModelConfig
from pynocular.exceptions import InvalidFieldValue, InvalidTextRepresentation

logger = logging.getLogger("pynocular")


class SQLDatabaseModelBackend(DatabaseModelBackend):
    """SQL database model backend

    This backend works with SQL dialects supported by https://www.encode.io/databases/. except sqlite*

    * sqlalchemy does not support the `RETURNING` clause. See https://github.com/sqlalchemy/sqlalchemy/issues/6195
    """

    def __init__(self, db: Database):
        """Initialize a SQLDatabaseModelBackend

        Args:
            db: Database object that has already established a connection pool

        """
        self.db = db

    def transaction(self) -> Transaction:
        """Create a new transaction

        Returns:
            new transaction to be used as a context manager

        """
        return self.db.transaction()

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
        query = config.table.select()
        if where_expressions is not None and len(where_expressions) > 0:
            query = query.where(and_(*where_expressions))
        if order_by is not None and len(order_by) > 0:
            query = query.order_by(*order_by)
        if limit is not None and limit > 0:
            query = query.limit(limit)

        try:
            result = await self.db.fetch_all(query)
        # The value was the wrong type. This usually happens with UUIDs.
        except InvalidTextRepresentation as e:
            raise InvalidFieldValue(message=e.diag.message_primary)

        return [dict(record) for record in result]

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

        async with self.transaction():
            result = await self.db.fetch_all(
                insert(config.table).values(records).returning(config.table)
            )

            return [dict(record) for record in result]

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
        async with self.transaction():
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
        async with self.transaction():
            query = (
                config.table.update()
                .where(and_(*where_expressions))
                .values(values)
                .returning(config.table)
            )
            try:
                result = await self.db.fetch_all(query)
            # The value was the wrong type. This usually happens with UUIDs.
            except InvalidTextRepresentation as e:
                raise InvalidFieldValue(message=e.diag.message_primary)

            return [dict(record) for record in result]

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
        async with self.transaction():
            logger.debug("Upsert starting")
            query = (
                insert(config.table)
                .values(record)
                .on_conflict_do_update(
                    index_elements=config.primary_key_names, set_=record
                )
                .returning(config.table)
            )
            updated_record = await self.db.fetch_one(query)
            logger.debug("Upsert complete")
            return dict(updated_record)
