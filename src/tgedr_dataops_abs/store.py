"""Store abstractions for CRUD (Create, Read, Update, Delete) operations.

Provides abstract base classes and interfaces for implementing stores that handle
full data persistence operations including get, save, update, and delete.
"""

import abc
from typing import Any


class StoreException(Exception):
    """Exception raised for store-related errors."""


class NoStoreException(StoreException):
    """Exception raised when a requested store item is not found."""


class StoreInterface(metaclass=abc.ABCMeta):
    """Interface for store implementations.

    Defines the contract for classes that implement CRUD operations.
    """

    @classmethod
    def __subclasshook__(cls, subclass):  # noqa: ANN001, ANN206
        """Check if a class implements the store interface."""
        return (
            hasattr(subclass, "get")
            and callable(subclass.get)
            and hasattr(subclass, "delete")
            and callable(subclass.delete)
            and hasattr(subclass, "save")
            and callable(subclass.save)
            and hasattr(subclass, "update")
            and callable(subclass.update)
        ) or NotImplemented


@StoreInterface.register
class Store(abc.ABC):
    """Abstract class used to manage persistence, defining CRUD-like (CreateReadUpdateDelete) methods."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize store with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the store.
        """
        self._config = config

    @abc.abstractmethod
    def get(self, key: str, **kwargs) -> Any:  # noqa: ANN003
        """Get data from the store by key.

        Parameters
        ----------
        key : str
            The key identifying the data to retrieve.
        **kwargs
            Additional store-specific parameters.

        Returns
        -------
        Any
            Retrieved data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, key: str, **kwargs) -> None:  # noqa: ANN003
        """Delete data from the store by key.

        Parameters
        ----------
        key : str
            The key identifying the data to delete.
        **kwargs
            Additional store-specific parameters.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def save(self, df: Any, key: str, **kwargs) -> Any:  # noqa: ANN003
        """Save data to the store.

        Parameters
        ----------
        df : Any
            The data to save.
        key : str
            The key to associate with the data.
        **kwargs
            Additional store-specific parameters.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, df: Any, key: str, **kwargs) -> Any:  # noqa: ANN003
        """Update existing data in the store.

        Parameters
        ----------
        df : Any
            The updated data.
        key : str
            The key identifying the data to update.
        **kwargs
            Additional store-specific parameters.
        """
        raise NotImplementedError
