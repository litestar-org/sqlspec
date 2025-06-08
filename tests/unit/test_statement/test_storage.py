from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.storage.protocol import ObjectStoreProtocol
from sqlspec.storage.registry import StorageRegistry


@pytest.fixture
def registry() -> "StorageRegistry":
    return StorageRegistry()


def test_register_and_get_backend_with_uri(registry: "StorageRegistry") -> None:
    # Test registering with URI
    uri = "s3://test-bucket"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register("foo", uri=uri)
        assert registry.get("foo") is backend_instance
        assert registry.is_registered("foo")
        assert "foo" in registry.list_keys()


def test_register_and_get_backend_with_backend_instance(registry: "StorageRegistry") -> None:
    # Test registering with backend class
    backend_cls = MagicMock()
    backend_instance = MagicMock(spec=ObjectStoreProtocol)
    backend_cls.return_value = backend_instance

    registry.register("foo", backend=backend_cls)  # type: ignore[arg-type]
    assert registry.get("foo") is backend_instance
    assert registry.is_registered("foo")
    assert "foo" in registry.list_keys()


def test_get_unregistered_backend_raises(registry: "StorageRegistry") -> None:
    with pytest.raises(KeyError):
        registry.get("missing")


def test_invalid_backend_raises(registry: "StorageRegistry") -> None:
    # Test with None backend and no URI (should raise ValueError)
    with pytest.raises(ValueError, match="Either backend or uri must be provided"):
        registry.register("foo")


def test_register_from_uri_obstore(registry: "StorageRegistry") -> None:
    # Test with S3 URI (maps to obstore)
    uri = "s3://bkt/data"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register("ob", uri=uri)
        backend = registry.get("ob")
        assert backend is backend_instance


def test_register_from_uri_fsspec(registry: "StorageRegistry") -> None:
    # Test with file URI (maps to fsspec)
    uri = "file:///tmp/data"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register("fs", uri=uri)
        backend = registry.get("fs")
        assert backend is backend_instance


def test_get_from_unknown_uri_raises(registry: "StorageRegistry") -> None:
    # Test with unknown URI scheme
    uri = "unknown://test"
    with patch("sqlspec.storage.registry.StorageRegistry._create_backend") as mock_create:
        mock_create.side_effect = Exception("Unknown scheme")
        with pytest.raises(KeyError):
            registry.get(uri)


def test_scheme_registration() -> None:
    registry = StorageRegistry()

    # Test default scheme mappings
    schemes = registry.list_schemes()
    assert schemes["s3"] == "obstore"
    assert schemes["file"] == "fsspec"

    # Test custom scheme registration
    registry.register_scheme("custom", "fsspec")
    updated_schemes = registry.list_schemes()
    assert updated_schemes["custom"] == "fsspec"


def test_cache_clearing() -> None:
    registry = StorageRegistry()

    # Register and get a backend
    call_count = 0

    def mock_backend_factory(**kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.call_id = call_count  # Add unique identifier
        return instance

    backend_cls = MagicMock(side_effect=mock_backend_factory)

    registry.register("test", backend=backend_cls)  # type: ignore[arg-type]
    first_get = registry.get("test")
    second_get = registry.get("test")

    # Should return the same cached instance
    assert first_get is second_get
    assert first_get.call_id == 1  # type: ignore[attr-defined]

    # Clear cache and get again
    registry.clear_cache("test")
    third_get = registry.get("test")

    # Should create a new instance
    assert third_get is not first_get
    assert third_get.call_id == 2  # type: ignore[attr-defined]


@pytest.mark.skipif(True, reason="Storage backends require optional dependencies")  # type: ignore
def test_backend_creation_with_obstore() -> None:
    """Test actual backend creation (requires obstore dependency)."""
    registry = StorageRegistry()

    # This would require actual obstore installation
    uri = "memory://test"
    backend = registry.get(uri)
    assert backend.backend_type == "obstore"  # type: ignore[attr-defined]


@pytest.mark.skipif(True, reason="Storage backends require optional dependencies")  # type: ignore
def test_backend_creation_with_fsspec() -> None:
    """Test actual backend creation (requires fsspec dependency)."""
    registry = StorageRegistry()

    # This would require actual fsspec installation
    uri = "file:///tmp"
    backend = registry.get(uri)
    assert backend.backend_type == "fsspec"  # type: ignore[attr-defined]


def test_uri_resolution_with_path() -> None:
    """Test Path object handling."""
    registry = StorageRegistry()
    test_path = Path("/tmp/test")

    with patch("sqlspec.storage.registry.StorageRegistry._resolve_from_uri") as mock_resolve:
        mock_backend = MagicMock(spec=ObjectStoreProtocol)
        mock_resolve.return_value = mock_backend

        result = registry.get(test_path)
        assert result is mock_backend
        mock_resolve.assert_called_once_with(f"file://{test_path.resolve()}")


def test_duplicate_registration() -> None:
    """Test that duplicate keys are allowed (overwrites previous)."""
    registry = StorageRegistry()

    # Create distinct mock factories
    def make_backend1(**kwargs: Any) -> MagicMock:
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.backend_id = "backend1"
        return instance

    def make_backend2(**kwargs: Any) -> MagicMock:
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.backend_id = "backend2"
        return instance

    backend1_cls = MagicMock(side_effect=make_backend1)
    backend2_cls = MagicMock(side_effect=make_backend2)

    # Register first backend
    registry.register("dup", backend=backend1_cls)  # type: ignore[arg-type]
    first_result = registry.get("dup")
    assert first_result.backend_id == "backend1"  # type: ignore[attr-defined]

    # Clear cache before re-registering
    registry.clear_cache("dup")

    # Register second backend with same key
    registry.register("dup", backend=backend2_cls)  # type: ignore[arg-type]
    second_result = registry.get("dup")
    assert second_result.backend_id == "backend2"  # type: ignore[attr-defined]

    # Should have overwritten the first one
    assert second_result is not first_result


def test_empty_key_allowed() -> None:
    """Test that empty string keys are allowed."""
    registry = StorageRegistry()

    backend_cls = MagicMock()
    backend_instance = MagicMock(spec=ObjectStoreProtocol)
    backend_cls.return_value = backend_instance

    # Empty key should be allowed
    registry.register("", backend=backend_cls)  # type: ignore[arg-type]
    result = registry.get("")
    assert result is backend_instance
    assert registry.is_registered("")
    assert "" in registry.list_keys()
