from abc import ABC, abstractmethod, abstractmethod
from typing import Any, Dict, List, Optional

from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression


class DatabaseModelBackend(ABC):
    @abstractmethod
    async def select(
        self,
        where_expressions: Optional[List[BinaryExpression]] = None,
        order_by: Optional[List[UnaryExpression]] = None,
        limit: Optional[int] = None,
    ) -> List["DatabaseModelBackend"]:
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
        pass

    @abstractmethod
    async def create_list(
        self, models: List["DatabaseModelBackend"]
    ) -> List["DatabaseModelBackend"]:
        """Create new batch of records in one query

        This will mutate the provided models to include db managed column values.

        Args:
            models: List of database models to persist

        Returns:
            list of new database models that have been saved

        """
        pass

    @abstractmethod
    async def delete_records(self, **kwargs: Any) -> None:
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

    @abstractmethod
    async def update(
        self,
        where_expressions: Optional[List[BinaryExpression]],
        values: Dict[str, Any],
    ) -> List["DatabaseModelBackend"]:
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

    @abstractmethod
    async def save(self, include_nested_models=False) -> None:
        """Update the database record this object represents with its current state

        Args:
            include_nested_models: If True, any nested models should get saved before
                this object gets saved

        """
        pass

    @abstractmethod
    async def delete(self) -> None:
        """Delete this record from the database"""
        pass
