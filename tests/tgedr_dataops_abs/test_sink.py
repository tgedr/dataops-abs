from typing import Any, Dict, Optional

import pytest

from tgedr_dataops_abs.sink import Sink, SinkChain, SinkException, SinkInterface


class ConcreteSink(Sink):
    """Concrete implementation of Sink for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.put_called = False
        self.delete_called = False
        self.last_context = None

    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.put_called = True
        self.last_context = context
        return {"status": "put_success", "context": context}

    def delete(self, context: Optional[Dict[str, Any]] = None):
        self.delete_called = True
        self.last_context = context


class ConcreteSinkChain(SinkChain):
    """Concrete implementation of SinkChain for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config
        self.put_called = False

    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.put_called = True
        return {"chain_result": "success"}

    def delete(self, context: Optional[Dict[str, Any]] = None):
        pass


class NotASink:
    """Class that doesn't implement Sink interface."""

    pass


class PartialSink:
    """Class that partially implements Sink interface (missing delete)."""

    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return "put"


def test_sink_exception():
    """Test SinkException can be raised and caught."""
    with pytest.raises(SinkException):
        raise SinkException("test error")


def test_sink_interface_subclass_hook_valid():
    """Test that a valid Sink implementation is recognized by the interface."""
    assert issubclass(ConcreteSink, SinkInterface)


def test_sink_interface_subclass_hook_invalid():
    """Test that an invalid class is not recognized by the interface."""
    assert not issubclass(NotASink, SinkInterface)


def test_sink_interface_subclass_hook_partial():
    """Test that a partial implementation is not recognized by the interface."""
    assert not issubclass(PartialSink, SinkInterface)


def test_sink_init_with_config():
    """Test Sink initialization with configuration."""
    config = {"bucket": "test-bucket", "prefix": "data/"}
    sink = ConcreteSink(config)
    assert sink._config == config


def test_sink_init_without_config():
    """Test Sink initialization without configuration."""
    sink = ConcreteSink()
    assert sink._config is None


def test_sink_put_with_context():
    """Test Sink put method with context."""
    config = {"bucket": "test-bucket"}
    sink = ConcreteSink(config)
    context = {"key": "value", "data": [1, 2, 3]}

    result = sink.put(context)

    assert sink.put_called
    assert sink.last_context == context
    assert result["status"] == "put_success"
    assert result["context"] == context


def test_sink_put_without_context():
    """Test Sink put method without context."""
    sink = ConcreteSink()

    result = sink.put()

    assert sink.put_called
    assert sink.last_context is None
    assert result["status"] == "put_success"
    assert result["context"] is None


def test_sink_delete_with_context():
    """Test Sink delete method with context."""
    sink = ConcreteSink()
    context = {"key": "item-to-delete"}

    sink.delete(context)

    assert sink.delete_called
    assert sink.last_context == context


def test_sink_delete_without_context():
    """Test Sink delete method without context."""
    sink = ConcreteSink()

    sink.delete()

    assert sink.delete_called
    assert sink.last_context is None


def test_sink_abstract_methods():
    """Test that Sink cannot be instantiated directly due to abstract methods."""
    with pytest.raises(TypeError):
        Sink()


def test_sink_chain_init_with_config():
    """Test SinkChain initialization with configuration."""
    config = {"output": "destination"}
    chain = ConcreteSinkChain(config)
    assert chain._config == config


def test_sink_chain_init_without_config():
    """Test SinkChain initialization without configuration."""
    chain = ConcreteSinkChain()
    assert chain._config is None


def test_sink_chain_execute():
    """Test SinkChain execute method calls put."""
    chain = ConcreteSinkChain()
    context = {"data": "test"}

    result = chain.execute(context)

    assert chain.put_called
    assert result["chain_result"] == "success"


def test_sink_chain_execute_without_context():
    """Test SinkChain execute method without context."""
    chain = ConcreteSinkChain()

    result = chain.execute()

    assert chain.put_called
    assert result["chain_result"] == "success"


def test_sink_chain_is_chain():
    """Test that SinkChain is recognized as a Chain."""
    from tgedr_dataops_abs.chain import Chain

    chain = ConcreteSinkChain()
    assert isinstance(chain, Chain)


def test_sink_chain_is_sink_interface():
    """Test that SinkChain is recognized by SinkInterface."""
    assert issubclass(ConcreteSinkChain, SinkInterface)
