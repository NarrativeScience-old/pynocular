"""Contains unit tests for the evaluate_column_element module"""

from pydantic import Field
from sqlalchemy import or_

from pynocular.database_model import DatabaseModel
from pynocular.evaluate_column_element import evaluate_column_element
from pynocular.util import UUID_STR


class Org(DatabaseModel, table_name="organizations"):
    """Model that represents the `organizations` table"""

    id: UUID_STR = Field(primary_key=True)
    name: str = Field(max_length=45)
    slug: str = Field(max_length=45)
    flag1: bool = Field(default=True)
    flag2: bool = Field(default=True)
    flag3: bool = Field(default=True)


def test_evaluate_column_element__neq():
    """Should handle the is_not operator"""
    assert not evaluate_column_element(Org.columns.name != "foo", {"name": "foo"})


def test_evaluate_column_element__n_ary_or():
    """Should handle an OR with multiple arguments"""
    assert evaluate_column_element(
        or_(Org.columns.flag1, Org.columns.flag2, Org.columns.flag3),
        {"flag1": False, "flag2": False, "flag3": True},
    )


def test_evaluate_column_element__not():
    """Should handle a NOT operator"""
    assert not evaluate_column_element(~Org.columns.flag1, {"flag1": True})
