"""Chain abstractions for building processing pipelines.

Provides abstract base classes and interfaces for chaining operations together
in a chain of responsibility pattern, allowing sequential processing steps.
"""

import abc
from typing import Any

from tgedr_dataops_abs.processor import Processor


class ChainException(Exception):
    """Exception raised for chain-related errors."""


class ChainInterface(metaclass=abc.ABCMeta):
    """Interface for chain implementations.

    Defines the contract for classes that implement next and execute operations.
    """

    @classmethod
    def __subclasshook__(cls, subclass):  # noqa: ANN001, ANN206
        """Check if a class implements the chain interface."""
        return (
            hasattr(subclass, "next")
            and callable(subclass.next)
            and hasattr(subclass, "execute")
            and callable(subclass.execute)
        ) or NotImplemented


class ChainMixin(abc.ABC):
    """Mixin providing chain functionality for sequential execution.

    Implements the chain of responsibility pattern for processing operations.
    """

    def next(self, handler: "ChainMixin") -> "ChainMixin":
        """Add the next handler in the chain.

        Parameters
        ----------
        handler : ChainMixin
            The next handler to add to the chain.

        Returns
        -------
        ChainMixin
            The current chain instance for method chaining.
        """
        if "_next" not in self.__dict__ or self._next is None:
            self._next: "ChainMixin" = handler  # noqa: UP037
        else:
            self._next.next(handler)
        return self

    @abc.abstractmethod
    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Execute the operation in the chain.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context to pass through the chain.

        Returns
        -------
        Any
            Result of the execution.
        """
        raise NotImplementedError


class ProcessorChainMixin(ChainMixin):
    """Mixin that combines processor and chain capabilities.

    Executes processor logic and passes control to the next handler in the chain.
    """

    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Execute processor and continue to next handler.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context to process and pass to next handler.

        Returns
        -------
        Any
            Result from processing.
        """
        self.process(context=context)
        if "_next" in self.__dict__ and self._next is not None:
            self._next.execute(context=context)


@ChainInterface.register
class ProcessorChain(ProcessorChainMixin, Processor):
    """Concrete processor that can be chained with other processors.

    Combines ProcessorChainMixin and Processor for chainable processing.
    """


@ChainInterface.register
class Chain(ChainMixin, abc.ABC):
    """Abstract base class for chainable operations.

    Extends ChainMixin to provide a base for custom chainable components.
    """

    @abc.abstractmethod
    def execute(self, context: dict[str, Any] | None = None) -> Any:
        """Execute the chain operation.

        Parameters
        ----------
        context : dict[str, Any] | None
            Context to pass through execution.

        Returns
        -------
        Any
            Result of the execution.
        """
        raise NotImplementedError
