"""Sink abstractions for write-only data persistence operations.

Provides abstract base classes and interfaces for implementing sinks that handle
data output operations (put, delete) without read capabilities.
"""

import abc
from typing import Any

from tgedr_dataops_abs.chain import Chain


class SinkException(Exception):
    """Exception raised for sink-related errors."""


class SinkInterface(metaclass=abc.ABCMeta):
    """Interface for sink implementations.

    Defines the contract for classes that implement put and delete operations.
    """

    @classmethod
    def __subclasshook__(cls, subclass):  # noqa: ANN001, ANN206
        """Check if a class implements the sink interface."""
        return (
            hasattr(subclass, "put")
            and callable(subclass.put)
            and hasattr(subclass, "delete")
            and callable(subclass.delete)
        ) or NotImplemented


@SinkInterface.register
class Sink(abc.ABC):
    """Abstract class defining methods ('put' and 'delete') to manage persistence of data somewhere as defined by implementing classes."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize sink with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the sink.
        """
        self._config = config

    @abc.abstractmethod
    def put(self, context: dict[str, Any] | None = None) -> Any:
        """Put data to the sink.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context containing data and metadata for the put operation.

        Returns
        -------
        Any
            Result of the put operation.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, context: dict[str, Any] | None = None) -> None:
        """Delete data from the sink.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context containing information about what to delete.
        """
        raise NotImplementedError


@SinkInterface.register
class SinkChain(Chain, abc.ABC):
    """Abstract sink that can be chained with other operations.

    Combines Chain and Sink capabilities for building processing pipelines.
    """

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Execute the sink operation by calling put.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context to pass to the put operation.

        Returns
        -------
        Any
            Result of the put operation.
        """
        return self.put(context=context)
