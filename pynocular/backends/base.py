"""Contains base classes for defining database backends"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from pydantic import Field
from sqlalchemy import Column, Table
from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression


@dataclass
class DatabaseModelConfig:
    """Data class that holds parsed configuration for a database model.

    This class will be instantiated by a database model class at import time.
    """

    fields: Dict[str, Field]
    primary_keys: List[Column]
    db_managed_fields: List[str]
    table: Table

    @property
    def primary_key_names(self) -> Set[str]:
        """Set of primary key names"""
        return {primary_key.name for primary_key in self.primary_keys}


class DatabaseModelBackend(ABC):
    """Defines abstract base class that database backends must implement

    The backend is agnostic to the DatabaseModel. This means that the concept of a
    DatabaseModel should not show up in any of the backend method implementations.

    * Methods should accept and return raw dictionaries.
    * Each method should accept a DatabaseModelConfig instance, which contains references
      to a table and columns that can be used to build queries suited to the backend.

    """

    @abstractmethod
    def transaction(self) -> Any:
        """Create a new transaction

        Not all backends will be able to implement this method.
        """
        pass

    @abstractmethod
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
            A list of record dicts

        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def delete_records(
        self, config: DatabaseModelConfig, where_expressions: List[BinaryExpression]
    ) -> Optional[int]:
        """Delete a group of records

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the backend query

        Returns:
            number of records deleted (or None if the backend does not support)

        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def upsert(
        self, config: DatabaseModelConfig, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Upsert a single database record

        Args:
            config: DatabaseModelConfig instance that contains references to a table and
                columns that can be used to build queries suited to the backend.
            record: The record to update

        Returns:
            the updated record

        """
        pass
