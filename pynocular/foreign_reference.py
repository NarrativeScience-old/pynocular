"""Class that wraps Foreign Key models"""
from typing import Any, Callable


class ForeignReferenceModel:
    """Class that holds a foreign key reference to another model"""

    def __init__(
        self,
        model_cls: Callable,
        _id: Any,
        model: "DatabaseModel" = None,
    ) -> None:
        """Init for ForeignReferenceModel

        Args:
            model_cls: The class that the id relates to
            _id: The id of the references
            model: The model object if it is already loaded

        """
        self._model_cls = model_cls
        self._model = model
        # Foreign Key references must only have one primary key
        self._primary_key_name = model_cls._primary_keys[0].name
        setattr(self, self._primary_key_name, _id)

    def get_primary_id(self) -> Any:
        """Standard interface for returning the id of a field

        Returns:
            The ID value for the proxied DatabaseModel

        """
        return getattr(self, self._primary_key_name)

    async def fetch(self) -> None:
        """Resolves the reference via the id set"""
        if self._model is None:
            self._model = await self._model_cls.get(
                getattr(self, self._primary_key_name)
            )

    def __getattr__(self, attr_name: str) -> Any:
        """Wrapper around getattr

        Args:
            attr_name: The name of the attribute

        Returns:
            The value of the attribute on the object

        """
        if attr_name == "resolve_ref" or attr_name == self._primary_key_name:
            return getattr(self, attr_name)
        elif self._model is None:
            raise AttributeError
        else:
            return getattr(self._model, attr_name)

    def __eq__(self, other: Any) -> bool:
        """Equality function

        Args:
            other: The object to compare to

        Returns:
            If the object is equal to this one

        """

        if self._model is None:
            return False
        else:
            return self._model == other
