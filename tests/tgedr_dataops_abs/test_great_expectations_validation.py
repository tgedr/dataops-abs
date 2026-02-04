"""Unit tests for Great Expectations validation module."""

import pytest
from unittest.mock import Mock, MagicMock, patch

import great_expectations as gx
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.validator.validator import Validator

from tgedr_dataops_abs.great_expectations_validation import (
    ValidationError,
    GreatExpectationsValidation,
)


class ConcreteGreatExpectationsValidation(GreatExpectationsValidation):
    """Concrete implementation for testing purposes."""

    def __init__(self, execution_engine=None):
        """Initialize with optional execution engine."""
        self._execution_engine = execution_engine

    def _get_execution_engine(self, batch_data_dict: dict) -> ExecutionEngine:
        """Return a mock execution engine."""
        if self._execution_engine:
            return self._execution_engine
        # Return a mock engine
        engine = Mock(spec=ExecutionEngine)
        engine.batch_manager = Mock()
        return engine


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_validation_error_is_exception(self):
        """Test that ValidationError is an Exception subclass."""
        assert issubclass(ValidationError, Exception)

    def test_validation_error_can_be_raised(self):
        """Test that ValidationError can be raised with a message."""
        with pytest.raises(ValidationError, match="Test error message"):
            raise ValidationError("Test error message")

    def test_validation_error_from_exception(self):
        """Test that ValidationError can wrap another exception."""
        original_error = ValueError("Original error")
        with pytest.raises(ValidationError) as exc_info:
            try:
                raise original_error
            except ValueError as e:
                raise ValidationError("Validation failed") from e

        assert exc_info.value.__cause__ is original_error


class TestGreatExpectationsValidation:
    """Tests for GreatExpectationsValidation abstract class."""

    def test_is_abstract_class(self):
        """Test that GreatExpectationsValidation is abstract."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            GreatExpectationsValidation()

    def test_concrete_implementation_can_be_instantiated(self):
        """Test that a concrete implementation can be instantiated."""
        impl = ConcreteGreatExpectationsValidation()
        assert isinstance(impl, GreatExpectationsValidation)

    def test_has_validate_method(self):
        """Test that the class has a validate method."""
        impl = ConcreteGreatExpectationsValidation()
        assert hasattr(impl, "validate")
        assert callable(impl.validate)

    def test_has_get_execution_engine_method(self):
        """Test that the class has _get_execution_engine method."""
        impl = ConcreteGreatExpectationsValidation()
        assert hasattr(impl, "_get_execution_engine")
        assert callable(impl._get_execution_engine)


class TestValidateMethod:
    """Tests for the validate method."""

    @pytest.fixture
    def mock_validator_result(self):
        """Create a mock validation result."""
        result = Mock()
        result.success = True
        result.to_json_dict.return_value = {
            "success": True,
            "results": [],
            "statistics": {
                "evaluated_expectations": 2,
                "successful_expectations": 2,
                "unsuccessful_expectations": 0,
            },
        }
        return result

    @pytest.fixture
    def mock_execution_engine(self):
        """Create a mock execution engine."""
        engine = Mock(spec=ExecutionEngine)
        engine.batch_manager = Mock()
        return engine

    def test_validate_creates_batch(self, mock_validator_result, mock_execution_engine):
        """Test that validate creates a Batch object from the dataframe."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"},
                }
            ],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch") as mock_batch_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_batch = Mock(spec=Batch)
            mock_batch.id = "batch_id"
            mock_batch.data = df
            mock_batch_class.return_value = mock_batch

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            result = impl.validate(df, expectations)

            # Verify Batch was created with the dataframe
            mock_batch_class.assert_called_once_with(data=df)
            assert result["success"] is True

    def test_validate_creates_expectation_suite(self, mock_validator_result, mock_execution_engine):
        """Test that validate creates an ExpectationSuite with correct name."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        suite_name = "custom_suite_name"
        expectations = {
            "expectation_suite_name": suite_name,
            "expectations": [],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify ExpectationSuite was created with correct name
            mock_suite_class.assert_called_once_with(name=suite_name)

    def test_validate_uses_default_suite_name(self, mock_validator_result, mock_execution_engine):
        """Test that validate uses default suite name when not provided."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {"expectations": []}  # No suite name provided

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify default suite name was used
            mock_suite_class.assert_called_once_with(name="validation_suite")

    def test_validate_adds_expectations_to_suite(self, mock_validator_result, mock_execution_engine):
        """Test that validate adds expectations to the suite."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "name"},
                },
            ],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.ExpectationConfiguration") as mock_exp_config_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_exp_config = Mock()
            mock_exp_config_class.return_value = mock_exp_config

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify expectations were added to the suite
            assert mock_suite.add_expectation_configuration.call_count == 2
            assert mock_exp_config_class.call_count == 2

            # Verify first expectation
            first_call = mock_exp_config_class.call_args_list[0]
            assert first_call[1]["type"] == "expect_column_to_exist"
            assert first_call[1]["kwargs"] == {"column": "id"}

            # Verify second expectation
            second_call = mock_exp_config_class.call_args_list[1]
            assert second_call[1]["type"] == "expect_column_values_to_not_be_null"
            assert second_call[1]["kwargs"] == {"column": "name"}

    def test_validate_creates_validator_with_correct_parameters(self, mock_validator_result, mock_execution_engine):
        """Test that validate creates a Validator with correct parameters."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"},
                }
            ],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch") as mock_batch_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_batch = Mock(spec=Batch)
            mock_batch.id = "batch_id"
            mock_batch.data = df
            mock_batch_class.return_value = mock_batch

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify Validator was created with correct parameters
            mock_validator_class.assert_called_once()
            call_kwargs = mock_validator_class.call_args[1]
            assert call_kwargs["execution_engine"] == mock_execution_engine
            assert call_kwargs["batches"] == [mock_batch]
            assert call_kwargs["expectation_suite"] == mock_suite
            assert call_kwargs["data_context"] == mock_ctx

    def test_validate_calls_get_execution_engine(self, mock_validator_result):
        """Test that validate calls _get_execution_engine with batch data."""
        mock_engine = Mock(spec=ExecutionEngine)
        mock_engine.batch_manager = Mock()
        impl = ConcreteGreatExpectationsValidation(mock_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch") as mock_batch_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite"), \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_batch = Mock(spec=Batch)
            mock_batch.id = "test_batch_id"
            mock_batch.data = df
            mock_batch_class.return_value = mock_batch

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            with patch.object(impl, "_get_execution_engine", wraps=impl._get_execution_engine) as mock_get_engine:
                impl.validate(df, expectations)

                # Verify _get_execution_engine was called with correct batch data dict
                mock_get_engine.assert_called_once()
                call_kwargs = mock_get_engine.call_args[1]
                assert "batch_data_dict" in call_kwargs
                assert "test_batch_id" in call_kwargs["batch_data_dict"]
                assert call_kwargs["batch_data_dict"]["test_batch_id"] == df

    def test_validate_runs_validation(self, mock_validator_result, mock_execution_engine):
        """Test that validate runs the validation."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite"), \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify validate was called with only_return_failures=True
            mock_validator.validate.assert_called_once_with(only_return_failures=True)

    def test_validate_returns_json_dict_result(self, mock_validator_result, mock_execution_engine):
        """Test that validate returns the JSON dict from validation result."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite"), \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            result = impl.validate(df, expectations)

            # Verify to_json_dict was called
            mock_validator_result.to_json_dict.assert_called_once()

            # Verify result is the JSON dict
            expected_result = mock_validator_result.to_json_dict.return_value
            assert result == expected_result
            assert result["success"] is True

    def test_validate_raises_validation_error_on_exception(self, mock_execution_engine):
        """Test that validate raises ValidationError when an exception occurs."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {"expectations": []}

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch", side_effect=RuntimeError("Test error")):
            with pytest.raises(ValidationError, match="failed data expectations"):
                impl.validate(df, expectations)

    def test_validate_wraps_original_exception(self, mock_execution_engine):
        """Test that validate wraps the original exception in ValidationError."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {"expectations": []}

        original_error = RuntimeError("Original test error")
        with patch("tgedr_dataops_abs.great_expectations_validation.Batch", side_effect=original_error):
            with pytest.raises(ValidationError) as exc_info:
                impl.validate(df, expectations)

            # Verify original exception is wrapped
            assert exc_info.value.__cause__ is original_error

    def test_validate_handles_empty_expectations_list(self, mock_validator_result, mock_execution_engine):
        """Test that validate handles empty expectations list."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "empty_suite",
            "expectations": [],  # Empty list
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            result = impl.validate(df, expectations)

            # Verify no expectations were added
            mock_suite.add_expectation_configuration.assert_not_called()
            assert result["success"] is True

    def test_validate_handles_missing_expectations_key(self, mock_validator_result, mock_execution_engine):
        """Test that validate handles missing 'expectations' key."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "suite_without_expectations"
            # No 'expectations' key
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            result = impl.validate(df, expectations)

            # Verify no expectations were added
            mock_suite.add_expectation_configuration.assert_not_called()
            assert result["success"] is True

    def test_validate_handles_expectation_without_kwargs(self, mock_validator_result, mock_execution_engine):
        """Test that validate handles expectations without 'kwargs' key."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between"
                    # No 'kwargs' key
                }
            ],
        }

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite") as mock_suite_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.ExpectationConfiguration") as mock_exp_config_class, \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_suite = Mock()
            mock_suite_class.return_value = mock_suite

            mock_exp_config = Mock()
            mock_exp_config_class.return_value = mock_exp_config

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify expectation was created with empty kwargs
            mock_exp_config_class.assert_called_once()
            call_kwargs = mock_exp_config_class.call_args[1]
            assert call_kwargs["kwargs"] == {}

    def test_validate_creates_ephemeral_context(self, mock_validator_result, mock_execution_engine):
        """Test that validate creates an ephemeral context."""
        impl = ConcreteGreatExpectationsValidation(mock_execution_engine)
        df = Mock()
        expectations = {"expectations": []}

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_get_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite"), \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_ctx = Mock()
            mock_get_context.return_value = mock_ctx

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_validator_result
            mock_validator_class.return_value = mock_validator

            impl.validate(df, expectations)

            # Verify get_context was called with mode="ephemeral"
            mock_get_context.assert_called_once_with(mode="ephemeral")


class TestGetExecutionEngine:
    """Tests for _get_execution_engine abstract method."""

    def test_concrete_implementation_must_implement_get_execution_engine(self):
        """Test that concrete implementations must implement _get_execution_engine."""

        class IncompleteImplementation(GreatExpectationsValidation):
            """Implementation without _get_execution_engine."""
            pass

        # Should not be able to instantiate without implementing abstract method
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteImplementation()

    def test_concrete_implementation_get_execution_engine_is_called(self):
        """Test that the concrete _get_execution_engine is actually called."""
        mock_engine = Mock(spec=ExecutionEngine)
        mock_engine.batch_manager = Mock()

        class TestImplementation(GreatExpectationsValidation):
            def __init__(self):
                self.get_engine_called = False
                self.mock_engine = mock_engine

            def _get_execution_engine(self, batch_data_dict: dict) -> ExecutionEngine:
                self.get_engine_called = True
                return self.mock_engine

        impl = TestImplementation()
        df = Mock()
        expectations = {"expectations": []}

        mock_result = Mock()
        mock_result.to_json_dict.return_value = {"success": True}

        with patch("tgedr_dataops_abs.great_expectations_validation.Batch"), \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.get_context") as mock_context, \
             patch("tgedr_dataops_abs.great_expectations_validation.gx.ExpectationSuite"), \
             patch("tgedr_dataops_abs.great_expectations_validation.Validator") as mock_validator_class:

            mock_validator = Mock(spec=Validator)
            mock_validator.validate.return_value = mock_result
            mock_validator_class.return_value = mock_validator

            mock_ctx = Mock()
            mock_context.return_value = mock_ctx

            impl.validate(df, expectations)

            # Verify _get_execution_engine was called
            assert impl.get_engine_called is True
