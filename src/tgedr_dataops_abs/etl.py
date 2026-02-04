"""ETL (Extract, Transform, Load) abstractions for data processing workflows.

Provides abstract base classes for implementing ETL pipelines with configuration
injection, validation hooks, and structured execution flow.

Example:
-------
```python
class MyEtl(Etl):
    @Etl.inject_configuration
    def extract(self, MY_PARAM) -> None:
        # "MY_PARAM" should be supplied in 'configuration' dict
        # otherwise an exception will be raised
        pass

    @Etl.inject_configuration
    def load(self, NOT_IN_CONFIG=123) -> None:
        # If you try to inject a configuration key that is NOT in the
        # configuration dictionary supplied to the constructor, it will
        # not throw an error as long as you set a default value
        assert NOT_IN_CONFIG == 123, "This will be ok"
```
"""

from abc import ABC, abstractmethod
import inspect
import logging
from typing import Any


logger = logging.getLogger(__name__)


class EtlException(Exception):
    """Exception raised for ETL-related errors."""


class Etl(ABC):
    """Abstract base class for ETL (Extract, Transform, Load) operations.

    Provides a template method pattern for ETL workflows with configuration
    injection and optional validation hooks.
    """

    def __init__(self, configuration: dict[str, Any] | None = None) -> None:
        """Initialize a new instance of ETL.

        Parameters
        ----------
        configuration : dict[str, Any]
            source for configuration injection
        """
        self._configuration = configuration

    @abstractmethod
    def extract(self) -> Any:
        """Extract data from source.

        Returns
        -------
        Any
            Extracted data.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self) -> Any:
        """Transform extracted data.

        Returns
        -------
        Any
            Transformed data.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self) -> Any:
        """Load transformed data to destination.

        Returns
        -------
        Any
            Result of load operation.
        """
        raise NotImplementedError

    def validate_extract(self) -> None:  # noqa: B027
        """Optional extra checks for extract step."""

    def validate_transform(self) -> None:  # noqa: B027
        """Optional extra checks for transform step."""

    def run(self) -> Any:
        """Execute the complete ETL workflow.

        Runs extract, validate_extract, transform, validate_transform, and load
        in sequence with structured logging.

        Returns
        -------
        Any
            Result from the load operation.
        """
        logger.info("[run|in]")

        self.extract()
        self.validate_extract()

        self.transform()
        self.validate_transform()

        result: Any = self.load()

        logger.info("[run|out] => %s", result)
        return result

    @staticmethod
    def inject_configuration(f):  # noqa: ANN001, ANN205, D102
        def decorator(self):  # noqa: ANN001, ANN202
            signature = inspect.signature(f)

            missing_params = []
            params = {}
            for param in [parameter for parameter in signature.parameters if parameter != "self"]:
                if signature.parameters[param].default != inspect._empty:  # noqa: SLF001
                    params[param] = signature.parameters[param].default
                else:
                    params[param] = None
                    if self._configuration is None or param not in self._configuration:
                        missing_params.append(param)

                if self._configuration is not None and param in self._configuration:
                    params[param] = self._configuration[param]

            if 0 < len(missing_params):
                msg = f"missing required configuration parameters: {missing_params}"
                raise EtlException(msg)

            return f(
                self,
                *[params[argument] for argument in params],
            )

        return decorator
