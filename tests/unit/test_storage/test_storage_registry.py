"""Unit tests for StorageRegistry."""

import tempfile
from pathlib import Path

import pytest

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.storage.registry import StorageRegistry, _is_local_uri
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED


def test_is_local_uri() -> None:
    """Test _is_local_uri type guard function."""
    # Absolute paths
    assert _is_local_uri("/absolute/path")
    assert _is_local_uri("C:\\Windows\\path")

    # Relative paths
    assert _is_local_uri("./relative/path")
    assert _is_local_uri("../parent/path")
    assert _is_local_uri("~/home/path")
    assert _is_local_uri("relative/path")

    # URIs should return False
    assert not _is_local_uri("s3://bucket/key")
    assert not _is_local_uri("https://example.com")
    assert not _is_local_uri("gs://bucket")


def test_registry_init() -> None:
    """Test registry initialization."""
    registry = StorageRegistry()
    assert len(registry.list_aliases()) == 0


def test_register_alias() -> None:
    """Test alias registration."""
    registry = StorageRegistry()

    registry.register_alias("test_store", "file:///tmp/test")
    assert registry.is_alias_registered("test_store")
    assert "test_store" in registry.list_aliases()


def test_get_local_backend() -> None:
    """Test getting local backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        # Test direct path
        backend = registry.get(temp_dir)
        assert backend.backend_type == "local"

        # Test file:// URI
        backend = registry.get(f"file://{temp_dir}")
        assert backend.backend_type == "local"


def test_get_alias() -> None:
    """Test getting backend by alias."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()
        registry.register_alias("my_store", f"file://{temp_dir}")

        backend = registry.get("my_store")
        assert backend.backend_type == "local"


def test_get_with_backend_override() -> None:
    """Test getting backend with override."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        # Force local backend
        backend = registry.get(f"file://{temp_dir}", backend="local")
        assert backend.backend_type == "local"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed")
def test_get_fsspec_backend() -> None:
    """Test getting fsspec backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        backend = registry.get(f"file://{temp_dir}", backend="fsspec")
        assert backend.backend_type == "fsspec"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore not installed")
def test_get_obstore_backend() -> None:
    """Test getting obstore backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        backend = registry.get(f"file://{temp_dir}", backend="obstore")
        assert backend.backend_type == "obstore"


def test_get_invalid_alias_raises_error() -> None:
    """Test getting invalid alias raises error."""
    registry = StorageRegistry()

    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("nonexistent_alias")


def test_get_empty_uri_raises_error() -> None:
    """Test getting empty URI raises error."""
    registry = StorageRegistry()

    with pytest.raises(ImproperConfigurationError, match="URI or alias cannot be empty"):
        registry.get("")


def test_get_invalid_backend_raises_error() -> None:
    """Test getting invalid backend type raises error."""
    registry = StorageRegistry()

    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.get("file:///tmp", backend="invalid")


def test_register_alias_with_base_path() -> None:
    """Test alias registration with base_path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        registry.register_alias("test_store", f"file://{temp_dir}/data")
        backend = registry.get("test_store")

        # Write and read to verify base_path works
        backend.write_text("test.txt", "content")
        assert backend.exists("test.txt")


def test_register_alias_with_backend_override() -> None:
    """Test alias registration with backend override."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        registry.register_alias("test_store", f"file://{temp_dir}", backend="local")
        backend = registry.get("test_store")
        assert backend.backend_type == "local"


def test_cache_functionality() -> None:
    """Test registry caching."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        # Get same backend twice
        backend1 = registry.get(f"file://{temp_dir}")
        backend2 = registry.get(f"file://{temp_dir}")

        # Should be the same instance
        assert backend1 is backend2


def test_clear_cache() -> None:
    """Test cache clearing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        backend1 = registry.get(f"file://{temp_dir}")
        registry.clear_cache(f"file://{temp_dir}")
        backend2 = registry.get(f"file://{temp_dir}")

        # Should be different instances after cache clear
        assert backend1 is not backend2


def test_clear_aliases() -> None:
    """Test clearing aliases."""
    registry = StorageRegistry()

    registry.register_alias("test_store", "file:///tmp")
    assert registry.is_alias_registered("test_store")

    registry.clear_aliases()
    assert not registry.is_alias_registered("test_store")
    assert len(registry.list_aliases()) == 0


def test_clear_instances() -> None:
    """Test clearing instances."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        backend1 = registry.get(f"file://{temp_dir}")
        registry.clear_instances()
        backend2 = registry.get(f"file://{temp_dir}")

        # Should be different instances after clear
        assert backend1 is not backend2


def test_clear_all() -> None:
    """Test clearing everything."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()

        registry.register_alias("test_store", f"file://{temp_dir}")
        backend1 = registry.get("test_store")

        registry.clear()

        assert not registry.is_alias_registered("test_store")
        assert len(registry.list_aliases()) == 0

        # Should create new instance
        registry.register_alias("test_store", f"file://{temp_dir}")
        backend2 = registry.get("test_store")
        assert backend1 is not backend2


def test_path_object_conversion() -> None:
    """Test Path object conversion to file:// URI."""
    with tempfile.TemporaryDirectory() as temp_dir:
        registry = StorageRegistry()
        path_obj = Path(temp_dir)

        backend = registry.get(path_obj)
        assert backend.backend_type == "local"


def test_cloud_storage_without_backends() -> None:
    """Test cloud storage URIs without backends raise proper errors."""
    if OBSTORE_INSTALLED or FSSPEC_INSTALLED:
        pytest.skip("Storage backends are installed")

    registry = StorageRegistry()

    with pytest.raises(MissingDependencyError, match="No backend available"):
        registry.get("s3://bucket")

    with pytest.raises(MissingDependencyError, match="No backend available"):
        registry.get("gs://bucket")
