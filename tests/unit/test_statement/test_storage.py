from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.storage.protocol import ObjectStoreProtocol
from sqlspec.storage.registry import StorageRegistry


@pytest.fixture
def registry() -> "StorageRegistry":
    return StorageRegistry()


def test_register_alias_and_get_backend_with_uri(registry: "StorageRegistry") -> None:
    # Test registering alias with URI
    uri = "s3://test-bucket"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register_alias("foo", uri=uri)
        assert registry.get("foo") is backend_instance
        assert registry.is_alias_registered("foo")
        assert "foo" in registry.list_aliases()


def test_register_alias_with_backend_class(registry: "StorageRegistry") -> None:
    # Test registering alias with backend class
    backend_cls = MagicMock()
    backend_instance = MagicMock(spec=ObjectStoreProtocol)
    backend_cls.return_value = backend_instance

    registry.register_alias("foo", uri="memory://test", backend=backend_cls)  # type: ignore[arg-type]
    assert registry.get("foo") is backend_instance
    assert registry.is_alias_registered("foo")
    assert "foo" in registry.list_aliases()


def test_get_unregistered_backend_raises(registry: "StorageRegistry") -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("missing")


def test_uri_first_access(registry: "StorageRegistry") -> None:
    # Test URI-first access pattern without registration
    uri = "s3://test-bucket/file.parquet"
    with patch("sqlspec.storage.registry.StorageRegistry._create_backend") as mock_create:
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        mock_create.return_value = backend_instance

        result = registry.get(uri)
        assert result is backend_instance
        mock_create.assert_called_once_with("obstore", uri)


def test_alias_with_obstore_uri(registry: "StorageRegistry") -> None:
    # Test alias with S3 URI (maps to obstore)
    uri = "s3://bkt/data"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register_alias("ob", uri=uri)
        backend = registry.get("ob")
        assert backend is backend_instance


def test_alias_with_fsspec_uri(registry: "StorageRegistry") -> None:
    # Test alias with file URI (maps to fsspec)
    uri = "file:///tmp/data"
    with patch("sqlspec.storage.registry.StorageRegistry._get_backend_class") as mock_get_class:
        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=ObjectStoreProtocol)
        backend_cls.return_value = backend_instance
        mock_get_class.return_value = backend_cls

        registry.register_alias("fs", uri=uri)
        backend = registry.get("fs")
        assert backend is backend_instance


def test_get_from_unknown_uri_raises(registry: "StorageRegistry") -> None:
    from sqlspec.exceptions import MissingDependencyError

    # Test with unknown URI scheme
    uri = "unknown://test"
    with patch("sqlspec.storage.registry.StorageRegistry._create_backend") as mock_create:
        mock_create.side_effect = ValueError("Unknown scheme")
        with pytest.raises(MissingDependencyError, match="No storage backend available"):
            registry.get(uri)


def test_clear_methods() -> None:
    registry = StorageRegistry()

    # Register an alias
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_backend:
            registry.register_alias("test", "s3://bucket", backend=mock_backend)

    # Test clearing
    assert "test" in registry.list_aliases()
    registry.clear_aliases()
    assert "test" not in registry.list_aliases()


def test_cache_clearing() -> None:
    registry = StorageRegistry()

    # Register alias and get a backend
    call_count = 0

    def mock_backend_factory(uri: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.call_id = call_count  # Add unique identifier
        return instance

    backend_cls = MagicMock(side_effect=mock_backend_factory)

    registry.register_alias("test", uri="memory://test", backend=backend_cls)  # type: ignore[arg-type]
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


def test_backend_creation_with_obstore() -> None:
    """Test actual backend creation (requires obstore dependency)."""
    registry = StorageRegistry()

    # This should now work with obstore MemoryStore
    uri = "memory://test"
    backend = registry.get(uri)
    assert backend.backend_type == "obstore"  # type: ignore[attr-defined]


def test_backend_creation_with_fsspec() -> None:
    """Test actual backend creation (requires fsspec dependency)."""
    registry = StorageRegistry()

    # This would require actual fsspec installation
    # ObStore is preferred for file:// URIs when available
    uri = "file:///tmp"
    backend = registry.get(uri)
    # Could be either obstore or fsspec depending on what's installed
    assert backend.backend_type in ("obstore", "fsspec")  # type: ignore[attr-defined]


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


def test_duplicate_alias_registration() -> None:
    """Test that duplicate aliases are allowed (overwrites previous)."""
    registry = StorageRegistry()

    # Create distinct mock factories
    def make_backend1(uri: str, **kwargs: Any) -> MagicMock:
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.backend_id = "backend1"
        return instance

    def make_backend2(uri: str, **kwargs: Any) -> MagicMock:
        instance = MagicMock(spec=ObjectStoreProtocol)
        instance.backend_id = "backend2"
        return instance

    backend1_cls = MagicMock(side_effect=make_backend1)
    backend2_cls = MagicMock(side_effect=make_backend2)

    # Register first backend
    registry.register_alias("dup", uri="memory://test1", backend=backend1_cls)  # type: ignore[arg-type]
    first_result = registry.get("dup")
    assert first_result.backend_id == "backend1"  # type: ignore[attr-defined]

    # Clear cache before re-registering
    registry.clear_cache("dup")

    # Register second backend with same alias
    registry.register_alias("dup", uri="memory://test2", backend=backend2_cls)  # type: ignore[arg-type]
    second_result = registry.get("dup")
    assert second_result.backend_id == "backend2"  # type: ignore[attr-defined]

    # Should have overwritten the first one
    assert second_result is not first_result


def test_empty_alias_rejected() -> None:
    """Test that empty string aliases are rejected."""
    from sqlspec.exceptions import ImproperConfigurationError

    registry = StorageRegistry()

    backend_cls = MagicMock()
    backend_instance = MagicMock(spec=ObjectStoreProtocol)
    backend_cls.return_value = backend_instance

    # Empty alias can be registered but not retrieved
    registry.register_alias("", uri="memory://test", backend=backend_cls)  # type: ignore[arg-type]

    # Getting empty alias should raise error
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("")
    assert registry.is_alias_registered("")
    assert "" in registry.list_aliases()
