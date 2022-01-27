"""Context manager for mocking db calls for DatabaseModels during tests"""
from contextlib import contextmanager
import functools
from typing import Any, Dict, List, Optional
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy.sql.elements import (
    AsBoolean,
    BinaryExpression,
    BindParameter,
    BooleanClauseList,
    ClauseList,
    ColumnElement,
    False_,
    Grouping,
    Null,
    True_,
    UnaryExpression,
)
from sqlalchemy.sql.operators import in_op, is_, is_false, is_not

from pynocular.database_model import DatabaseModel


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
                _evaluate_column_element(expr, model.to_dict())
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
                _evaluate_column_element(expr, model.to_dict())
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
                _evaluate_column_element(expr, model.to_dict())
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
                _evaluate_column_element(expr, model.to_dict())
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
                _evaluate_column_element(expr, model.to_dict()) for expr in where_exp
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


@functools.singledispatch
def _evaluate_column_element(
    column_element: ColumnElement, model: Dict[str, Any]
) -> Any:
    """Evaluate a ColumnElement on a dictionary representing a database model

    This function can be overridden based on the type of ColumnElement to return
    an element from the model, a static value, or the result of some operation (e.g.
    addition).

    Args:
        column_element: The element to evaluate.
        model: The model to evaluate the column element on. Represented as a dictionary
            where the keys are column names.

    """
    raise Exception(f"Cannot evaluate a {column_element} object.")


@_evaluate_column_element.register(BooleanClauseList)
def _evaluate_boolean_clause_list(
    column_element: ClauseList, model: Dict[str, Any]
) -> Any:
    """Evaluates a boolean clause list and breaks it down into its sub column elements

    Args:
        column_element: The BooleanClauseList in question.
        model: The model of data this clause should be evaluated for.

    Returns:
        The result of the evaluation.

    """
    operator = column_element.operator

    return functools.reduce(
        operator,
        [
            _evaluate_column_element(sub_element, model)
            for sub_element in column_element.get_children()
        ],
    )


@_evaluate_column_element.register(ClauseList)
def _evaluate_clause_list(column_element: ClauseList, model: Dict[str, Any]) -> Any:
    """Evaluates a clause list and breaks it down into its sub column elements

    Args:
        column_element: The ClauseList in question.
        model: The model of data this clause should be evaluated for.

    Returns:
        The result of the evaluation.

    """
    operator = column_element.operator

    return operator(
        *[
            _evaluate_column_element(sub_element, model)
            for sub_element in column_element.get_children()
        ]
    )


@_evaluate_column_element.register(BinaryExpression)
def _evaluate_binary_expression(
    column_element: BinaryExpression, model: Dict[str, Any]
) -> Any:
    """Evaluates the binary expression

    Args:
        column_element: The binary expression to evaluate.
        model: The model to evaluate the expression on.

    Returns:
        The evaluation response dictated by the operator of the expression.

    """
    operator = column_element.operator

    # The sqlalchemy `in` operator does not work on evaluated columns, so we replace
    # it with the standard `in` operator.
    if operator == in_op:
        operator = lambda x, y: x in y

    # The sqlalchemy `is` operator does not work on evaluated columns, so we replace it
    # with the standard `is` operator.
    if operator == is_:
        operator = lambda x, y: x is y

    # The sqlalchemy `is_not` operator does not work on evaluated columns, so we replace
    # it with the standard `!=` operator.
    if operator == is_not:
        operator = lambda x, y: x != y

    return operator(
        _evaluate_column_element(column_element.left, model),
        _evaluate_column_element(column_element.right, model),
    )


@_evaluate_column_element.register(AsBoolean)
def _evaluate_as_boolean(column_element: AsBoolean, model: Dict[str, Any]) -> Any:
    """Evaluates a boolean

    Args:
        column_element: The boolean to evaluate.
        model: The model to evaluate the expression on.

    Returns:
        The evaluation response dictated by the operator of the expression.

    """
    result = bool(_evaluate_column_element(column_element.element, model))
    if column_element.operator == is_false:
        return not result
    return result


@_evaluate_column_element.register(Column)
def _evaluate_column(column_element: Column, model: Dict[str, Any]) -> Any:
    """Evaluate the column based on the column element name

    Args:
        column_element: The column to evaluate.
        model: The model dictionary.

    Returns:
        The value from the model of attribute referenced by column_element.

    """
    return model.get(column_element.name)


@_evaluate_column_element.register(BindParameter)
def _evaluate_bind_parameter(
    column_element: BindParameter, model: Dict[str, Any]
) -> Any:
    """Evaluate the column_elements value

    Args:
        column_element: The column's bind parameter.
        model: The model dictionary.

    Returns:
        The value of the column_element

    """
    return column_element.value


@_evaluate_column_element.register(True_)
def _evaluate_true(column_element: True_, model: Dict[str, Any]) -> bool:
    """Wrapper around evaluating True

    Args:
        column_element: The column to evaluate. This is just True
        model: The model dictionary.

    Returns:
        True

    """
    # The boolean value True is its own SQLAlchemy element
    return True


@_evaluate_column_element.register(False_)
def _evaluate_false(column_element: False_, model: Dict[str, Any]) -> bool:
    """Wrapper around evaluating False

    Args:
        column_element: The column to evaluate. This just returns False
        model: The model dictionary.

    Returns:
        False

    """
    # The boolean value False is its own SQLAlchemy element
    return False


@_evaluate_column_element.register(Grouping)
def _evaluate_grouping(column_element: Grouping, model: Dict[str, Any]) -> List[Any]:
    """Wrapper around evaluating a grouping

    Args:
        column_element: The grouping to evaluate.
        model: The model dictionary.

    Returns:
        A list of of values that are the resulting of evaluating each element in the
        group.

    """
    return [
        _evaluate_column_element(clause, model)
        for clause in column_element.element.clauses
    ]


@_evaluate_column_element.register(Null)
def _evaluate_null(column_element: Null, model: Dict[str, Any]) -> Any:
    """Wrapper around evaluating null

    Args:
        column_element: The column element to evaluate. This is null
        model: The model dictionary.

    Returns:
        None

    """
    # The Null value is its own SQLAlchemy element
    return None
