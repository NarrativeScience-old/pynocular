"""Contains the MemoryDatabaseModelBackend class"""

from collections import defaultdict
import itertools
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import Integer
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression
from sqlalchemy.sql.operators import desc_op

from pynocular.backends.base import DatabaseModelBackend, DatabaseModelConfig
from pynocular.patch_models import _evaluate_column_element


class MemoryDatabaseModelBackend(DatabaseModelBackend):
    """In-memory database model backend

    This backend stores records in memory. It translates SQLAlchemy expressions into
    Python operations. It should only be used in tests.
    """

    def __init__(self, records: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        """Initialize a SQLDatabaseModelBackend

        Args:
            records: Optional map of table name to list of records to bootstrap the
                in-memory database

        """
        self.records = records or defaultdict(list)
        # Serial primary key generator
        self._pk_generator = itertools.count(start=1)

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
        records = self.records[config.table.name]

        if where_expressions:
            records = [
                record
                for record in records
                if all(
                    _evaluate_column_element(expr, record) for expr in where_expressions
                )
            ]

        if order_by:
            for expr in order_by:
                if isinstance(expr, UnaryExpression):
                    column = expr.element
                    reverse = expr.modifier == desc_op
                else:
                    # Assume a column was provided with no explicit sorting modifier
                    column = expr
                    reverse = False

                records = sorted(
                    records, key=lambda r: r.get(column.name), reverse=reverse
                )

        if limit is None:
            records[:limit]

        return records

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
        for record in records:
            for primary_key in config.primary_keys:
                value = (
                    next(self._pk_generator)
                    if isinstance(primary_key.type, Integer)
                    else str(uuid4())
                )
                record.setdefault(primary_key.name, value)

        self.records[config.table.name].extend(records)

        return self.records[config.table.name]

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
        self.records[config.table.name][:] = [
            record
            for record in self.records[config.table.name]
            if not all(
                _evaluate_column_element(expr, record) for expr in where_expressions
            )
        ]

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
        records = await self.select(config, where_expressions=where_expressions)
        for record in records:
            record.update(values)

        return records

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
        if all(
            record.get(primary_key.name) is not None
            for primary_key in config.primary_keys
        ):
            # All primary keys are already set so update
            where_expressions = [
                primary_key == record.get(primary_key.name)
                for primary_key in config.primary_keys
            ]
            records = await self.update_records(config, where_expressions, record)
            return records[0]
        else:
            # Primary keys have not been set so this is a new record
            for primary_key in config.primary_keys:
                value = (
                    next(self._pk_generator)
                    if isinstance(primary_key.type, Integer)
                    else str(uuid4())
                )
                record.setdefault(primary_key.name, value)

            self.records[config.table.name].append(record)
            return record
