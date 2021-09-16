"""Exceptions module for ORM package"""
import traceback
from typing import Any, Dict, Iterable, Optional

# Reexport
from psycopg2.errors import InvalidTextRepresentation  # noqa: F401

# This is copied from ns_python_core for convenience but definitely belongs
# somewhere else.
ERROR_INFO_KEYS = [
    "internal_status_msg",
    "external_status_msg",
    "exception_type",
    "stack_trace",
]


class ErrorInfo(dict):
    """Error information that is passed between services.

    This class verifies that parameters are named correctly and can be
    serialized by the default JSON encoder to the correct format.
    """

    def _validate_keys(self, keys: Iterable[str]) -> None:
        """Validate that the given key is one of the expected keys.

        Will raise an exception if an unexpected key is encountered.
        """
        for key in keys:
            if key not in ERROR_INFO_KEYS:
                raise ValueError("Invalid key: '%s'" % key)

    def _ensure_external_status_message(self) -> None:
        """Make sure there is an external status message.

        Set a generic external status message if an internal status message
        is specified and no external status message is specified.
        """
        if (
            self["internal_status_msg"] is not None
            and self["external_status_msg"] is None
        ):
            self[
                "external_status_msg"
            ] = "An error occurred, please contact Narrative Science."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new instance of the ErrorInfo class

        This is initialized the same way a dict is.
        Keys that are not specified will receive a value of None.
        """
        for key in ERROR_INFO_KEYS:
            if key not in self:
                self[key] = None
        self.update(*args, **kwargs)

    def __setitem__(self, key: str, value: Any) -> None:
        """Update or set the value of an item.

        This will raise an exception if an unexpected key is passed in.
        """
        self._validate_keys([key])
        super(ErrorInfo, self).__setitem__(key, value)
        self._ensure_external_status_message()

    def update(self, *args: Any, **kwargs: Any) -> None:
        """Update the values in the dict.

        This will raise an exception if an unexpected value is passed in.
        """
        self._validate_keys(kwargs.keys())

        updated_args = []
        for arg in args:
            self._validate_keys(arg.keys())
            updated_args.append(arg)
        super(ErrorInfo, self).update(*updated_args, **kwargs)
        self._ensure_external_status_message()


class BaseException(Exception):
    """Base class for exceptions.

    Its only responsibility is to capture internal and external status
    information as well as a stack trace.
    """

    # This property denotes the component that owns will raise this exception. This
    # property can be used for component level monitoring
    COMPONENT = None

    def __init__(
        self,
        internal_msg: str = None,
        external_msg: str = None,
        error_info: ErrorInfo = None,
    ) -> None:
        """Initialize the exception.

        The error_info parameter is expected to be an ErrorInfo object or a
        dictionary with the same keys as an ErrorInfo object.
        If an error_info object is passed in the content of that object will
        override anything else in this class.
        """
        Exception.__init__(self, internal_msg)
        # Set internal message to '' by default to avoid TypeErrors
        self.internal_msg = internal_msg
        if internal_msg is None:
            self.internal_msg = ""
        self.error_info = error_info
        self.external_msg = external_msg

        # Attempt to capture a stack trace.  If an exception has occurred we'll
        # just grab that stack trace.  If one has not occurred we'll grab the
        # current stack minus the last frame which is just the constructor of
        # the exception class.
        self.stack_trace = None
        try:
            self.stack_trace = traceback.format_exc()
            if self.stack_trace == "None\n":
                self.stack_trace = "".join(traceback.format_stack()[:-1])
        except AttributeError:
            # Not in an exception context
            pass

    def get_error_info(self) -> ErrorInfo:
        """Build and return an ErrorInfo object

        The object should contain the error information collected by the
        exception class.  If an ErrorInfo object was passed in to the
        constructor, then return that instead.
        """
        if self.error_info is not None:
            return self.error_info
        else:
            return ErrorInfo(
                internal_status_msg=self.internal_msg,
                external_status_msg=self.external_msg,
                exception_type=self.__class__.__name__,
                stack_trace=self.stack_trace,
            )

    def get_metadata(self) -> Dict[str, str]:
        """Return the standard metadata for this error

        Returns:
            A dictionary of property keys to their stringified values

        """
        return {"component": self.COMPONENT.name}

    def __str__(self) -> str:
        """Returns the message describing the exception"""
        return self.internal_msg


class DatabaseRecordNotFound(BaseException):
    """Exception thrown when trying to fetch a database record that doesn't exist"""

    def __init__(self, table_name: str, **kwargs: Any) -> None:
        """Initialize DatabaseRecordNotFound

        Args:
            table_name: The name of the table that the record would be from
            kwargs: The field names and field values of the record that's not found

        """
        msg = (
            f"No record was found with the following fields: {kwargs} "
            f"on table '{table_name}'"
        )

        super().__init__(msg, msg)


class DatabaseObjectMisconfigured(BaseException):
    """Exception thrown when using a misconfigured DatabaseObject object"""

    def __init__(self, class_name: str) -> None:
        """Initialize DatabaseObjectMisconfigured

        Args:
            class_name: The name of the DatabaseObject class

        """
        msg = f"DatabaseObject class '{class_name}' is misconfigured"
        super().__init__(msg, msg)


class DatabaseModelMisconfigured(BaseException):
    """Exception thrown when using a misconfigured DatabaseModel object"""

    def __init__(self, class_name: str) -> None:
        """Initialize DatabaseModelMisconfigured

        Args:
            class_name: The name of the DatabaseModel class

        """
        msg = f"DatabaseModel '{class_name}' is misconfigured"
        super().__init__(msg, msg)


class DatabaseObjectMissingField(BaseException):
    """Exception thrown when the field provided for querying doesn't exist on the table"""

    def __init__(self, class_name: str, field_name: str) -> None:
        """Initialize DatabaseObjectMissingField

        Args:
            class_name: The name of the DatabaseObject class
            field_name: The name of the field used for querying this class

        """
        msg = f"DatabaseObject class '{class_name}' does not have field '{field_name}'"
        super().__init__(msg, msg)


class DatabaseModelMissingField(BaseException):
    """Exception thrown when the field provided for querying doesn't exist on the table"""

    def __init__(self, class_name: str, field_name: str) -> None:
        """Initialize DatabaseModelMissingField

        Args:
            class_name: The name of the DatabaseModel class
            field_name: The name of the field used for querying this class

        """
        msg = f"DatabaseModel class '{class_name}' does not have field '{field_name}'"
        super().__init__(msg, msg)


class InvalidMethodParameterization(BaseException):
    """Exception thrown when passing invalid parameters into a method"""

    def __init__(self, method_name: str, **kwargs: Any) -> None:
        """Initialize InvalidMethodParameterization

        Args:
            method_name: The name of the method
            kwargs: The parameters that were passed into the method

        """
        msg = (
            f"An invalid parameter configuration: {kwargs} "
            f"was passed in for the method '{method_name}'"
        )
        super().__init__(msg, msg)


class InvalidFieldValue(BaseException):
    """Exception thrown when a field value is invalid"""

    def __init__(
        self,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        message: Optional[str] = None,
    ) -> None:
        """Initialize exception.

        Can create with the invalid field and field value or a prewritten message
        returned from postgres.

        Args:
            field_name: The name of the field with the invalid value
            field_value: The value that was invalid
            message: A pre-created message explaining the issue

        """
        if message:
            msg = message
        else:
            self.field_name = field_name
            self.field_value = field_value

            value_type = type(field_value)
            try:
                string_value = str(field_value)
            except Exception:
                string_value = "<couldn't convert>"

            msg = (
                f"An invalid value was used for field {field_name}: {string_value} "
                f"of type {value_type}."
            )

        super().__init__(msg, msg)


class InvalidSqlIdentifierErr(Exception):
    """Indicates a sql identifier is invalid"""

    def __init__(self, identifier: str) -> None:
        """Initialize InvalidSqlIndentifierErr

        Args:
            identifier: The invalid identifier

        """
        self.identifier = identifier

    def __str__(self) -> str:
        """Returns the message describing the exception"""
        return f"Invalid identifier {self.identifier}"


class ForeignReferenceNotResolved(BaseException):
    """Indicates a property was accessed before the reference was resolved"""

    def __init__(self, model_cls: str, foreign_key_value: Any) -> None:
        """Initialize ForeignReferenceNotResolved

        Args:
            model_cls: The class name of the model that was being referenecd
            foreign_key_value: The foreign_key value

        """
        msg = (
            f"Object {model_cls} with id {foreign_key_value} was not resolved."
            f"Please call `fetch()` before trying to access properties of {model_cls}"
        )

        super.__init__(msg, msg)
