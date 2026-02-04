"""Processor abstractions for data transformation operations.

Provides abstract base classes and interfaces for implementing processors that
transform data based on context.
"""

import abc
from typing import Any


class ProcessorException(Exception):
    """Exception raised for processor-related errors."""


class ProcessorInterface(metaclass=abc.ABCMeta):
    """Interface for processor implementations.

    Defines the contract for classes that implement process operations.
    """

    @classmethod
    def __subclasshook__(cls, subclass):  # noqa: ANN001, ANN206
        """Check if a class implements the processor interface."""
        return (hasattr(subclass, "process") and callable(subclass.process)) or NotImplemented


@ProcessorInterface.register
class Processor(abc.ABC):
    """Abstract base class for processors that transform data."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize processor with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the processor.
        """
        self._config = config

    @abc.abstractmethod
    def process(self, context: dict[str, Any] | None = None) -> Any:
        """Process data based on the provided context.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context containing data and parameters for processing.

        Returns
        -------
        Any
            Processed result.
        """
        raise NotImplementedError
