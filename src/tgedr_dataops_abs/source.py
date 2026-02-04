"""Source abstractions for read-only data retrieval operations.

Provides abstract base classes and interfaces for implementing sources that handle
data input operations (get, list) without write capabilities.
"""

import abc
from typing import Any

from tgedr_dataops_abs.chain import Chain


class SourceException(Exception):
    """Exception raised for source-related errors."""


class NoSourceException(SourceException):
    """Exception raised when a requested source is not found."""


class SourceInterface(metaclass=abc.ABCMeta):
    """Interface for source implementations.

    Defines the contract for classes that implement get and list operations.
    """

    @classmethod
    def __subclasshook__(cls, subclass):  # noqa: ANN001, ANN206
        """Check if a class implements the source interface."""
        return (
            hasattr(subclass, "get")
            and callable(subclass.get)
            and hasattr(subclass, "list")
            and callable(subclass.list)
        ) or NotImplemented


@SourceInterface.register
class Source(abc.ABC):
    """Abstract class defining methods ('list' and 'get') to manage retrieval of data from somewhere as defined by implementing classes."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize source with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the source.
        """
        self._config = config

    @abc.abstractmethod
    def get(self, context: dict[str, Any] | None = None) -> Any:
        """Get data from the source.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context containing parameters for the get operation.

        Returns
        -------
        Any
            Retrieved data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def list(self, context: dict[str, Any] | None = None) -> Any:
        """List available items in the source.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context containing parameters for the list operation.

        Returns
        -------
        Any
            List of available items.
        """
        raise NotImplementedError


@SourceInterface.register
class SourceChain(Chain, abc.ABC):
    """Abstract source that can be chained with other operations.

    Combines Chain and Source capabilities for building processing pipelines.
    """

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Execute the source operation by calling get.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context to pass to the get operation.

        Returns
        -------
        Any
            Result of the get operation.
        """
        return self.get(context=context)
