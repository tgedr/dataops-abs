"""Great Expectations validation module for data quality checks.

This module provides:
- ValidationError: exception for validation errors in data validation operations;
- GreatExpectationsValidation: abstract base class for Great Expectations validation implementations.
"""

from abc import ABC, abstractmethod
import logging
from typing import Any

import great_expectations as gx
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Exception raised for validation errors in data validation operations."""


class GreatExpectationsValidation(ABC):
    """Abstract base class for Great Expectations validation implementations.

    This class provides a common interface for validating dataframes using Great Expectations.
    Subclasses must implement the `_get_execution_engine` method to provide the appropriate
    execution engine for their specific data processing framework.

    Methods
    -------
    validate(df: Any, expectations: dict) -> dict
        Validate a dataframe against Great Expectations suite.
    _get_execution_engine(batch_data_dict: dict) -> ExecutionEngine
        Get the execution engine used by the validation implementation (abstract).
    """

    def validate(self, df: Any, expectations: dict) -> dict:
        """Validate a dataframe against Great Expectations suite.

        Parameters
        ----------
        df : Any
            The dataframe to validate.
        expectations : dict
            Dictionary containing expectation configurations.
            Expected format:
            {
                "expectation_suite_name": "suite_name",
                "expectations": [
                    {
                        "expectation_type": "expect_column_to_exist",
                        "kwargs": {"column": "col_name"}
                    },
                    ...
                ]
            }

        Returns
        -------
        dict
            Validation results as a dictionary with "success" and "results" keys.

        Raises
        ------
        ValidationError
            If validation fails or encounters an error.
        """
        logger.info("[validate|in] (%s, %s)", df, expectations)

        try:
            # Get batch from dataframe
            batch = Batch(data=df)

            # Create expectation suite from dict
            suite_name = expectations.get("expectation_suite_name", "validation_suite")
            suite = gx.ExpectationSuite(name=suite_name)

            # Add expectations from dict
            for exp_config in expectations.get("expectations", []):
                exp_type = exp_config.get("expectation_type")
                exp_kwargs = exp_config.get("kwargs", {})

                # Create ExpectationConfiguration with 'type' parameter (not 'expectation_type')
                expectation = ExpectationConfiguration(type=exp_type, kwargs=exp_kwargs)
                suite.add_expectation_configuration(expectation)

            # Create validator directly from batch and suite
            # This avoids the FluentBatch requirement in context.get_validator for GE 1.9.1
            context = gx.get_context(mode="ephemeral")
            execution_engine = self._get_execution_engine(batch_data_dict={batch.id: batch.data})
            validator = Validator(
                execution_engine=execution_engine, batches=[batch], expectation_suite=suite, data_context=context
            )

            # Run validation
            validation_result = validator.validate(only_return_failures=True)

            result = validation_result.to_json_dict()

        except Exception as x:
            msg = f"[validate] failed data expectations: {x}"
            raise ValidationError(msg) from x

        logger.info("[validate|out] => %s", result.get("success"))
        return result

    @abstractmethod
    def _get_execution_engine(self, batch_data_dict: dict) -> ExecutionEngine:
        """Get the execution engine used by the validation implementation.

        Returns
        -------
        Any
            The execution engine instance.
        """
        raise NotImplementedError
