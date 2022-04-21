"""Context manager for mocking db calls for DatabaseModels during tests"""
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.sql.elements import BinaryExpression, UnaryExpression

from pynocular.database_model import DatabaseModel
from pynocular.evaluate_column_element import evaluate_column_element


@contextmanager
def patch_database_model(
    model_cls: DatabaseModel,
    models: Optional[List[DatabaseModel]] = None,
) -> None:
    """Patch a DatabaseModel class, seeding with a set of values

    Example:
        with patch_database_model(Org, [Org(id="1", name="org 1"), ...]:
            await Org.get_list(...)

    Args:
        model_cls: A subclass of DatabaseModel that should be patched.
        models: models that should be in the patched DB table.

    """
    models = list(models) if models is not None else []

    def match(model: DatabaseModel, expression: BinaryExpression) -> bool:
        """Function to match the value with the expected one in the expression

        Args:
            model: The db model that represents a model in the "db".
            expression: The expression object to compare to.

        Returns:
            True if the expression operator is True.

        """
        return expression.operator(
            model.get(expression.left.name), expression.right.value
        )

    async def select(
        where_expressions: Optional[List[BinaryExpression]] = None,
        order_by: Optional[List[UnaryExpression]] = None,
        limit: Optional[int] = None,
    ) -> List[DatabaseModel]:
        """Mock select function for DatabaseModel

        Args:
            where_expressions: The BinaryExpressions to use in the select where clause.
            order_by: The order by expressions to be included in the select. This are
            not supported for mocking at this time.
            limit: The maximum number of objects to return.

        Returns:
            List of DatabaseModels that match the parameters.

        """
        # This function currently does not support `order_by` parameter.
        if where_expressions is None:
            return models

        matched_models = [
            model
            for model in models
            if all(
                evaluate_column_element(expr, model.to_dict())
                for expr in where_expressions
            )
        ]

        if limit is None:
            matched_models[:limit]

        return matched_models

    async def create_list(models) -> List[DatabaseModel]:
        """Mock `create_list` function for DatabaseModel

        Args:
            models: List of DatabaseModels to persist.

        Returns:
            The list of new DatabaseModels that have been saved.

        """
        # Iterate through the list of orm objs and call save().
        for obj in models:
            await obj.save()

        return models

    async def save(model, include_nested_models=False) -> None:
        """Mock `save` function for DatabaseModel

        Args:
            model: The model to save.
            include_nested_models: If True, any nested models should get saved before
                this object gets saved.

        """
        # If include_nested_models is True, call save on all nested model attributes.
        # This requires that the nested models are also patched.
        if include_nested_models:
            for attr_name in model._nested_model_attributes:
                obj = getattr(model, attr_name)
                if obj is not None:
                    await obj.save()

        primary_keys = model_cls._primary_keys
        # Put uuids into any primary key that isn't set yet.
        for primary_key in primary_keys:
            val = getattr(model, primary_key.name)
            if val is None:
                setattr(model, primary_key.name, str(uuid4()))

        # Pull the primary keys out of the class and the values out of the provided
        # database model. Then build a where_expression list to get the model matching those
        # primary keys.
        where_expressions = [
            primary_key == getattr(model, primary_key.name)
            for primary_key in primary_keys
        ]
        selected_models = [
            model
            for model in models
            if all(
                evaluate_column_element(expr, model.to_dict())
                for expr in where_expressions
            )
        ]

        if len(selected_models) == 0:
            # Add a new model to the models since this model didn't exist before.
            models.append(model)
        else:
            # Update the matching model. Since these are primary keys there should only
            # ever be one model matching the given where_expressions.
            matched_model = selected_models[0]
            for attr, val in model.dict().items():
                setattr(matched_model, attr, val)

    async def update_record(**kwargs: Any) -> DatabaseModel:
        """Mock `update_record` function for DatabaseModel

        Args:
            kwargs: The values to update.

        Returns:
            The updated DatabaseModel.

        """
        primary_keys = model_cls._primary_keys

        # Pull the primary keys out of the class and the values out of the provided
        # kwargs. Then build a where_expression list to get the model matching those
        # primary keys.
        where_expressions = [
            primary_key == kwargs[primary_key.name] for primary_key in primary_keys
        ]
        selected_models = [
            model
            for model in models
            if all(
                evaluate_column_element(expr, model.to_dict())
                for expr in where_expressions
            )
        ]

        # Update the matching model. Since these are primary keys there should only
        # ever be one model matching the given where_expressions.
        model = selected_models[0]
        for attr, val in kwargs.items():
            setattr(model, attr, val)
        return model

    async def update(
        where_expressions: Optional[List[BinaryExpression]], values: Dict[str, Any]
    ) -> List[DatabaseModel]:
        """Mock `update_record` function for DatabaseModel

        Args:
            where_expressions: A list of BinaryExpressions for the table that will be
                `and`ed together for the where clause of the UPDATE
            values: The field and values to update all records to that match the
                where_expressions

        Returns:
            The updated DatabaseModels.

        """
        models = await select(where_expressions)
        for model in models:
            for attr, val in values.items():
                setattr(model, attr, val)
        return models

    async def delete(model) -> None:
        """Mock `delete` function for DatabaseModel"""
        primary_keys = model_cls._primary_keys

        # Pull the primary keys out of the class and the values out of the provided
        # database model. Then build a where_expression list to get the model matching those
        # primary keys.
        where_expressions = [
            primary_key == getattr(model, primary_key.name)
            for primary_key in primary_keys
        ]

        # Remove any models that match the given where_expression
        models[:] = [
            model
            for model in models
            if not all(
                evaluate_column_element(expr, model.to_dict())
                for expr in where_expressions
            )
        ]

    async def delete_records(**kwargs: Any) -> None:
        """Mock `delete_records` function for DatabaseModel

        Args:
            kwargs: The values used to find the records that should be deleted

        """
        where_exp = []
        for key, value in kwargs.items():
            col = getattr(model_cls.columns, key)
            if isinstance(value, list):
                where_exp.append(col.in_(value))
            else:
                where_exp.append(col == value)

        # Remove any models that match the given where_expression
        models[:] = [
            model
            for model in models
            if not all(
                evaluate_column_element(expr, model.to_dict()) for expr in where_exp
            )
        ]

    # Add the patches. Note that create functionality is patched indirectly though
    # 'save' already, but add a spy on it anyway so we can test calls against it.
    with patch.object(model_cls, "select", select), patch.object(
        model_cls, "save", save
    ), patch.object(model_cls, "update_record", update_record), patch.object(
        model_cls, "update", update
    ), patch.object(
        model_cls, "create_list", create_list
    ), patch.object(
        model_cls, "create", wraps=model_cls.create
    ), patch.object(
        model_cls, "delete", delete
    ), patch.object(
        model_cls, "delete_records", delete_records
    ):
        yield
