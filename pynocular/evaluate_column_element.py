"""Contains evaluate_column_element function for evaluating filter expressions"""

import functools
from typing import Any, Dict, List

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
)
from sqlalchemy.sql.operators import in_op, is_, is_false, is_not


@functools.singledispatch
def evaluate_column_element(
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


@evaluate_column_element.register(BooleanClauseList)
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
            evaluate_column_element(sub_element, model)
            for sub_element in column_element.get_children()
        ],
    )


@evaluate_column_element.register(ClauseList)
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
            evaluate_column_element(sub_element, model)
            for sub_element in column_element.get_children()
        ]
    )


@evaluate_column_element.register(BinaryExpression)
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
        evaluate_column_element(column_element.left, model),
        evaluate_column_element(column_element.right, model),
    )


@evaluate_column_element.register(AsBoolean)
def _evaluate_as_boolean(column_element: AsBoolean, model: Dict[str, Any]) -> Any:
    """Evaluates a boolean

    Args:
        column_element: The boolean to evaluate.
        model: The model to evaluate the expression on.

    Returns:
        The evaluation response dictated by the operator of the expression.

    """
    result = bool(evaluate_column_element(column_element.element, model))
    if column_element.operator == is_false:
        return not result
    return result


@evaluate_column_element.register(Column)
def _evaluate_column(column_element: Column, model: Dict[str, Any]) -> Any:
    """Evaluate the column based on the column element name

    Args:
        column_element: The column to evaluate.
        model: The model dictionary.

    Returns:
        The value from the model of attribute referenced by column_element.

    """
    return model.get(column_element.name)


@evaluate_column_element.register(BindParameter)
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


@evaluate_column_element.register(True_)
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


@evaluate_column_element.register(False_)
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


@evaluate_column_element.register(Grouping)
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
        evaluate_column_element(clause, model)
        for clause in column_element.element.clauses
    ]


@evaluate_column_element.register(Null)
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
