"""Contains the MemoryDatabaseModelBackend class"""

import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import functools
import itertools
from types import TracebackType
from typing import Any, Callable, Dict, Generator, List, Optional, Type
from uuid import uuid4

from sqlalchemy import Integer
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression
from sqlalchemy.sql.operators import desc_op

from pynocular.backends.base import DatabaseModelBackend, DatabaseModelConfig
from pynocular.evaluate_column_element import evaluate_column_element
from pynocular.util import UUID_STR


class MemoryConnection:
    """In-memory connection

    This mirrors the databases library implementation.
    """

    def __init__(
        self, records: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> None:
        """In-memory connection

        Args:
            records: Optional map of table name to list of records to bootstrap the
                in-memory database

        """
        self.records = records or defaultdict(list)
        self._tmp_records = None
        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: list[MemoryTransaction] = []

    def backup_records(self) -> None:
        """Backup the records in the connection to a temporary variable"""
        self._tmp_records = deepcopy(self.records)

    def clear_backup(self) -> None:
        """Clear the backup"""
        self._tmp_records = None

    def restore_records(self) -> None:
        """Restore the original copy of records"""
        self.records = deepcopy(self._tmp_records)


class MemoryTransaction:
    """In-memory transaction

    This mirrors the databases library implementation.
    """

    def __init__(self, connection: MemoryConnection) -> None:
        """In-memory transaction

        Args:
            connection: Connection instance containing records

        """
        self._connection = connection

    async def __aenter__(self) -> "MemoryTransaction":
        """Called when entering `async with database.transaction()`"""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        """Called when exiting `async with database.transaction()`"""
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> Generator:
        """Called if using the low-level `transaction = await database.transaction()`"""
        return self.start().__await__()

    def __call__(self, func: Callable) -> Callable:
        """Called if using `@database.transaction()` as a decorator."""

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper

    async def start(self) -> "MemoryTransaction":
        """Start a transaction"""
        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            if is_root:
                self._connection.backup_records()
            self._connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        """Commit the transaction on success"""
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            is_root = not self._connection._transaction_stack
            if is_root:
                self._connection.clear_backup()

    async def rollback(self) -> None:
        """Rollback the transaction in case of failure"""
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            is_root = not self._connection._transaction_stack
            if is_root:
                self._connection.restore_records()


class MemoryDatabaseModelBackend(DatabaseModelBackend):
    """In-memory database model backend

    This backend stores records in memory. It translates SQLAlchemy expressions into
    Python operations. It should only be used in tests.
    """

    def __init__(self, records: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        """Initialize a MemoryDatabaseModelBackend

        Args:
            records: Optional map of table name to list of records to bootstrap the
                in-memory database

        """
        super().__init__()
        # Create a "connection" to hold records and interface with transactions
        self._connection = MemoryConnection(records)
        # Serial primary key generator
        self._pk_generator = itertools.count(start=1)

    @property
    def records(self) -> Dict[str, List[Dict[str, Any]]]:
        """Map of table name to list of records"""
        return self._connection.records

    def _set_primary_key_values(
        self,
        config: DatabaseModelConfig,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Set default values on a record for the primary keys

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            record: The record to update

        Returns:
            updated record

        """
        for primary_key in config.primary_keys:
            value = (
                next(self._pk_generator)
                if isinstance(primary_key.type, Integer)
                else str(uuid4())
            )
            record.setdefault(primary_key.name, value)

        return record

    @staticmethod
    def _update_db_managed_fields(
        config: DatabaseModelConfig,
        record: Dict[str, Any],
        fetch_on_create: bool = False,
        fetch_on_update: bool = False,
    ) -> Dict[str, Any]:
        """Update record values for db managed fields

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            record: The record to update
            fetch_on_create: Flag that controls whether the db managed field will be
                updated if it has the option `fetch_on_create=True`. Defaults to False.
            fetch_on_update: Flag that controls whether the db managed field will be
                updated if it has the option `fetch_on_update=True`. Defaults to False.

        Raises:
            NotImplementedError: if a field sets fetch_on_create or fetch_on_update to
                true but its type is not supported

        Returns:
            updated record

        """
        for name in config.db_managed_fields:
            field = config.fields[name]
            if (
                fetch_on_create
                and field.field_info.extra.get("fetch_on_create")
                and record.get(name) is None
            ) or (fetch_on_update and field.field_info.extra.get("fetch_on_update")):
                if field.type_ == datetime:
                    record[name] = datetime.utcnow()
                elif field.type_ == UUID_STR:
                    record[name] = str(uuid4())
                else:
                    raise NotImplementedError(field.type_)

        return record

    def transaction(self) -> MemoryTransaction:
        """Create a new transaction"""
        return MemoryTransaction(self._connection)

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
                    evaluate_column_element(expr, record) for expr in where_expressions
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
            self._set_primary_key_values(config, record)
            self._update_db_managed_fields(
                config, record, fetch_on_create=True, fetch_on_update=True
            )

        self.records[config.table.name].extend(records)

        return self.records[config.table.name]

    async def delete_records(
        self, config: DatabaseModelConfig, where_expressions: List[BinaryExpression]
    ) -> int:
        """Delete a group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the backend query

        Returns:
            number of records deleted

        """
        start_count = len(self.records[config.table.name])
        self.records[config.table.name][:] = [
            record
            for record in self.records[config.table.name]
            if not all(
                evaluate_column_element(expr, record) for expr in where_expressions
            )
        ]
        return start_count - len(self.records[config.table.name])

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
            self._update_db_managed_fields(config, record, fetch_on_update=True)

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
        where_expressions = [
            primary_key == record.get(primary_key.name)
            for primary_key in config.primary_keys
        ]
        existing_records = await self.select(
            config, where_expressions=where_expressions, limit=1
        )
        if (
            all(
                record.get(primary_key.name) is not None
                for primary_key in config.primary_keys
            )
            and existing_records
        ):
            # All primary keys are already set and a record was found so update
            self._update_db_managed_fields(config, record, fetch_on_update=True)
            records = await self.update_records(config, where_expressions, record)
            return records[0]
        else:
            # Primary keys have not been set or there were no records found, so this is
            # a new record
            self._set_primary_key_values(config, record)
            self._update_db_managed_fields(
                config, record, fetch_on_create=True, fetch_on_update=True
            )
            self.records[config.table.name].append(record)
            return record
