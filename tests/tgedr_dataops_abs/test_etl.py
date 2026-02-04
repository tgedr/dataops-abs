import pytest

from tgedr_dataops_abs.etl import Etl, EtlException


class MyEtl(Etl):
    @Etl.inject_configuration
    def extract(self, input) -> None:
        return input

    def transform(self, input_2: str = 6) -> None:
        return input_2

    def load(self) -> None:
        self._configuration["output"] = 9


class MyEtlWithValidation(Etl):
    def __init__(self, configuration=None):
        super().__init__(configuration)
        self.extract_validated = False
        self.transform_validated = False

    @Etl.inject_configuration
    def extract(self, source: str) -> None:
        self._data = f"data from {source}"

    def transform(self) -> None:
        self._result = self._data.upper()

    def load(self) -> str:
        return self._result

    def validate_extract(self):
        """Test custom validation."""
        self.extract_validated = True

    def validate_transform(self):
        """Test custom validation."""
        self.transform_validated = True


class MyEtlWithMissingParam(Etl):
    @Etl.inject_configuration
    def extract(self, required_param) -> None:
        return required_param

    def transform(self) -> None:
        pass

    def load(self) -> None:
        pass


class MyEtlWithMultipleParams(Etl):
    @Etl.inject_configuration
    def extract(self, param1, param2="default2", param3=None) -> dict:
        return {"param1": param1, "param2": param2, "param3": param3}

    def transform(self) -> None:
        pass

    def load(self) -> None:
        pass


def test_config():
    etl = MyEtl({"input": 3})
    assert 3 == etl.extract()


def test_default_param():
    etl = MyEtl()
    assert 6 == etl.transform()


def test_static_run():
    config = {"input": 3}
    MyEtl(config).run()
    assert 9 == config["output"]


def test_etl_exception():
    """Test EtlException can be raised and caught."""
    with pytest.raises(EtlException):
        raise EtlException("test error")


def test_inject_configuration_missing_required_param():
    """Test that inject_configuration raises exception for missing required parameters."""
    etl = MyEtlWithMissingParam()  # No config provided

    with pytest.raises(EtlException) as exc_info:
        etl.extract()

    assert "missing required configuration parameters" in str(exc_info.value)
    assert "required_param" in str(exc_info.value)


def test_inject_configuration_with_none_config():
    """Test inject_configuration when _configuration is None."""
    etl = MyEtlWithMissingParam(None)

    with pytest.raises(EtlException):
        etl.extract()


def test_inject_configuration_param_not_in_config():
    """Test inject_configuration when param exists but is not in config dict."""
    etl = MyEtlWithMissingParam({"other_param": "value"})

    with pytest.raises(EtlException) as exc_info:
        etl.extract()

    assert "required_param" in str(exc_info.value)


def test_inject_configuration_with_default_value():
    """Test inject_configuration uses default value when param not in config."""
    etl = MyEtlWithMultipleParams({"param1": "value1"})

    result = etl.extract()

    assert result["param1"] == "value1"
    assert result["param2"] == "default2"
    assert result["param3"] is None


def test_inject_configuration_overrides_default():
    """Test inject_configuration overrides default value with config value."""
    etl = MyEtlWithMultipleParams({"param1": "value1", "param2": "custom2", "param3": "custom3"})

    result = etl.extract()

    assert result["param1"] == "value1"
    assert result["param2"] == "custom2"
    assert result["param3"] == "custom3"


def test_inject_configuration_partial_override():
    """Test inject_configuration with partial config override."""
    etl = MyEtlWithMultipleParams({"param1": "value1", "param3": "custom3"})

    result = etl.extract()

    assert result["param1"] == "value1"
    assert result["param2"] == "default2"
    assert result["param3"] == "custom3"


def test_etl_run_with_validation():
    """Test ETL run method calls validation hooks."""
    config = {"source": "database"}
    etl = MyEtlWithValidation(config)

    result = etl.run()

    assert etl.extract_validated
    assert etl.transform_validated
    assert result == "DATA FROM DATABASE"


def test_etl_cannot_instantiate_directly():
    """Test that Etl abstract class cannot be instantiated directly."""
    with pytest.raises(TypeError):
        Etl()
