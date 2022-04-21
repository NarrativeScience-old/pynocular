"""Contains util functions"""

from typing import Any, Generator
from uuid import UUID as stdlib_uuid


def is_valid_uuid(string: str) -> bool:
    """Check if a string is a valid UUID

    Args:
        string: the string to check

    Returns:
        Whether or not the string is a well-formed UUIDv4

    """
    try:
        stdlib_uuid(string, version=4)
        return True
    except (TypeError, AttributeError, ValueError):
        return False


class UUID_STR(str):
    """A string that represents a UUID4 value"""

    @classmethod
    def __get_validators__(cls) -> Generator:
        """Get the validators for the given class"""
        yield cls.validate

    @classmethod
    def validate(cls, v: Any) -> str:
        """Function to validate the value

        Args:
            v: The value to validate

        """
        if isinstance(v, stdlib_uuid) or (isinstance(v, str) and is_valid_uuid(v)):
            return str(v)
        else:
            raise ValueError("invalid UUID string")
