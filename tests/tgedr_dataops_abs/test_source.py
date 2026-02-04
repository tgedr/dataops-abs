from typing import Any, Dict, Optional

import pytest

from tgedr_dataops_abs.source import (
    NoSourceException,
    Source,
    SourceChain,
    SourceException,
    SourceInterface,
)


class ConcreteSource(Source):
    """Concrete implementation of Source for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.get_called = False
        self.list_called = False
        self.last_context = None

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.get_called = True
        self.last_context = context
        if context and context.get("raise_no_source"):
            raise NoSourceException("Source not found")
        return {"data": "retrieved", "context": context}

    def list(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.list_called = True
        self.last_context = context
        return ["item1", "item2", "item3"]


class ConcreteSourceChain(SourceChain):
    """Concrete implementation of SourceChain for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config
        self.get_called = False

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.get_called = True
        return {"chain_data": "success"}

    def list(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return []


class NotASource:
    """Class that doesn't implement Source interface."""

    pass


class PartialSource:
    """Class that partially implements Source interface (missing list)."""

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return "data"


def test_source_exception():
    """Test SourceException can be raised and caught."""
    with pytest.raises(SourceException):
        raise SourceException("test error")


def test_no_source_exception():
    """Test NoSourceException can be raised and caught."""
    with pytest.raises(NoSourceException):
        raise NoSourceException("source not found")


def test_no_source_exception_is_source_exception():
    """Test NoSourceException is a subclass of SourceException."""
    with pytest.raises(SourceException):
        raise NoSourceException("source not found")


def test_source_interface_subclass_hook_valid():
    """Test that a valid Source implementation is recognized by the interface."""
    assert issubclass(ConcreteSource, SourceInterface)


def test_source_interface_subclass_hook_invalid():
    """Test that an invalid class is not recognized by the interface."""
    assert not issubclass(NotASource, SourceInterface)


def test_source_interface_subclass_hook_partial():
    """Test that a partial implementation is not recognized by the interface."""
    assert not issubclass(PartialSource, SourceInterface)


def test_source_init_with_config():
    """Test Source initialization with configuration."""
    config = {"endpoint": "http://api.example.com", "api_key": "secret"}
    source = ConcreteSource(config)
    assert source._config == config


def test_source_init_without_config():
    """Test Source initialization without configuration."""
    source = ConcreteSource()
    assert source._config is None


def test_source_get_with_context():
    """Test Source get method with context."""
    config = {"endpoint": "http://api.example.com"}
    source = ConcreteSource(config)
    context = {"id": "123", "filter": "active"}

    result = source.get(context)

    assert source.get_called
    assert source.last_context == context
    assert result["data"] == "retrieved"
    assert result["context"] == context


def test_source_get_without_context():
    """Test Source get method without context."""
    source = ConcreteSource()

    result = source.get()

    assert source.get_called
    assert source.last_context is None
    assert result["data"] == "retrieved"
    assert result["context"] is None


def test_source_get_raises_no_source_exception():
    """Test Source get method can raise NoSourceException."""
    source = ConcreteSource()
    context = {"raise_no_source": True}

    with pytest.raises(NoSourceException):
        source.get(context)


def test_source_list_with_context():
    """Test Source list method with context."""
    source = ConcreteSource()
    context = {"prefix": "data/"}

    result = source.list(context)

    assert source.list_called
    assert source.last_context == context
    assert result == ["item1", "item2", "item3"]


def test_source_list_without_context():
    """Test Source list method without context."""
    source = ConcreteSource()

    result = source.list()

    assert source.list_called
    assert source.last_context is None
    assert len(result) == 3


def test_source_abstract_methods():
    """Test that Source cannot be instantiated directly due to abstract methods."""
    with pytest.raises(TypeError):
        Source()


def test_source_chain_init_with_config():
    """Test SourceChain initialization with configuration."""
    config = {"input": "location"}
    chain = ConcreteSourceChain(config)
    assert chain._config == config


def test_source_chain_init_without_config():
    """Test SourceChain initialization without configuration."""
    chain = ConcreteSourceChain()
    assert chain._config is None


def test_source_chain_execute():
    """Test SourceChain execute method calls get."""
    chain = ConcreteSourceChain()
    context = {"filter": "test"}

    result = chain.execute(context)

    assert chain.get_called
    assert result["chain_data"] == "success"


def test_source_chain_execute_without_context():
    """Test SourceChain execute method without context."""
    chain = ConcreteSourceChain()

    result = chain.execute()

    assert chain.get_called
    assert result["chain_data"] == "success"


def test_source_chain_is_chain():
    """Test that SourceChain is recognized as a Chain."""
    from tgedr_dataops_abs.chain import Chain

    chain = ConcreteSourceChain()
    assert isinstance(chain, Chain)


def test_source_chain_is_source_interface():
    """Test that SourceChain is recognized by SourceInterface."""
    assert issubclass(ConcreteSourceChain, SourceInterface)
