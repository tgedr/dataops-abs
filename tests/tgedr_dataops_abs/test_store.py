from typing import Any, Dict, Optional

import pytest

from tgedr_dataops_abs.store import NoStoreException, Store, StoreException, StoreInterface


class ConcreteStore(Store):
    """Concrete implementation of Store for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.storage = {}
        self.get_called = False
        self.delete_called = False
        self.save_called = False
        self.update_called = False

    def get(self, key: str, **kwargs) -> Any:
        self.get_called = True
        if key not in self.storage:
            raise NoStoreException(f"Key '{key}' not found")
        return self.storage[key]

    def delete(self, key: str, **kwargs) -> None:
        self.delete_called = True
        if key in self.storage:
            del self.storage[key]

    def save(self, df: Any, key: str, **kwargs):
        self.save_called = True
        self.storage[key] = df
        return key

    def update(self, df: Any, key: str, **kwargs):
        self.update_called = True
        if key not in self.storage:
            raise NoStoreException(f"Key '{key}' not found for update")
        self.storage[key] = df
        return key


class NotAStore:
    """Class that doesn't implement Store interface."""

    pass


class PartialStore:
    """Class that partially implements Store interface (missing update)."""

    def get(self, key: str, **kwargs) -> Any:
        return "data"

    def delete(self, key: str, **kwargs) -> None:
        pass

    def save(self, df: Any, key: str, **kwargs):
        pass


def test_store_exception():
    """Test StoreException can be raised and caught."""
    with pytest.raises(StoreException):
        raise StoreException("test error")


def test_no_store_exception():
    """Test NoStoreException can be raised and caught."""
    with pytest.raises(NoStoreException):
        raise NoStoreException("store not found")


def test_no_store_exception_is_store_exception():
    """Test NoStoreException is a subclass of StoreException."""
    with pytest.raises(StoreException):
        raise NoStoreException("store not found")


def test_store_interface_subclass_hook_valid():
    """Test that a valid Store implementation is recognized by the interface."""
    assert issubclass(ConcreteStore, StoreInterface)


def test_store_interface_subclass_hook_invalid():
    """Test that an invalid class is not recognized by the interface."""
    assert not issubclass(NotAStore, StoreInterface)


def test_store_interface_subclass_hook_partial():
    """Test that a partial implementation is not recognized by the interface."""
    assert not issubclass(PartialStore, StoreInterface)


def test_store_init_with_config():
    """Test Store initialization with configuration."""
    config = {"connection": "postgresql://localhost", "table": "data"}
    store = ConcreteStore(config)
    assert store._config == config


def test_store_init_without_config():
    """Test Store initialization without configuration."""
    store = ConcreteStore()
    assert store._config is None


def test_store_save():
    """Test Store save method."""
    store = ConcreteStore()
    data = {"value": 42, "name": "test"}

    result = store.save(data, "key1")

    assert store.save_called
    assert result == "key1"
    assert store.storage["key1"] == data


def test_store_save_with_kwargs():
    """Test Store save method with additional kwargs."""
    config = {"path": "/data"}
    store = ConcreteStore(config)
    data = [1, 2, 3, 4, 5]

    result = store.save(data, "key2", overwrite=True, compress=True)

    assert store.save_called
    assert result == "key2"
    assert store.storage["key2"] == data


def test_store_get():
    """Test Store get method."""
    store = ConcreteStore()
    data = {"value": 100}
    store.storage["key3"] = data

    result = store.get("key3")

    assert store.get_called
    assert result == data


def test_store_get_with_kwargs():
    """Test Store get method with additional kwargs."""
    store = ConcreteStore()
    data = "important_data"
    store.storage["key4"] = data

    result = store.get("key4", version=2, decrypt=True)

    assert store.get_called
    assert result == data


def test_store_get_raises_no_store_exception():
    """Test Store get method raises NoStoreException for missing key."""
    store = ConcreteStore()

    with pytest.raises(NoStoreException) as exc_info:
        store.get("nonexistent_key")

    assert "nonexistent_key" in str(exc_info.value)


def test_store_update():
    """Test Store update method."""
    store = ConcreteStore()
    original_data = {"value": 10}
    updated_data = {"value": 20}
    store.storage["key5"] = original_data

    result = store.update(updated_data, "key5")

    assert store.update_called
    assert result == "key5"
    assert store.storage["key5"] == updated_data


def test_store_update_with_kwargs():
    """Test Store update method with additional kwargs."""
    store = ConcreteStore()
    store.storage["key6"] = "old_data"
    new_data = "new_data"

    result = store.update(new_data, "key6", merge=True, validate=True)

    assert store.update_called
    assert result == "key6"
    assert store.storage["key6"] == new_data


def test_store_update_raises_no_store_exception():
    """Test Store update method raises NoStoreException for missing key."""
    store = ConcreteStore()
    data = {"value": 30}

    with pytest.raises(NoStoreException) as exc_info:
        store.update(data, "nonexistent_key")

    assert "nonexistent_key" in str(exc_info.value)


def test_store_delete():
    """Test Store delete method."""
    store = ConcreteStore()
    store.storage["key7"] = "data_to_delete"

    store.delete("key7")

    assert store.delete_called
    assert "key7" not in store.storage


def test_store_delete_with_kwargs():
    """Test Store delete method with additional kwargs."""
    store = ConcreteStore()
    store.storage["key8"] = "data"

    store.delete("key8", force=True, recursive=True)

    assert store.delete_called
    assert "key8" not in store.storage


def test_store_delete_nonexistent_key():
    """Test Store delete method with nonexistent key (no error)."""
    store = ConcreteStore()

    store.delete("nonexistent_key")

    assert store.delete_called


def test_store_abstract_methods():
    """Test that Store cannot be instantiated directly due to abstract methods."""
    with pytest.raises(TypeError):
        Store()


def test_store_crud_workflow():
    """Test complete CRUD workflow."""
    store = ConcreteStore({"location": "memory"})

    # Create
    data = {"id": 1, "name": "test_item"}
    store.save(data, "item1")
    assert "item1" in store.storage

    # Read
    retrieved = store.get("item1")
    assert retrieved == data

    # Update
    updated_data = {"id": 1, "name": "updated_item"}
    store.update(updated_data, "item1")
    assert store.get("item1") == updated_data

    # Delete
    store.delete("item1")
    with pytest.raises(NoStoreException):
        store.get("item1")
