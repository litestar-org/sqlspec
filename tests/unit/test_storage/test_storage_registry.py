# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for StorageRegistry."""

from pathlib import Path

import pytest

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.storage.registry import StorageRegistry
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED
from sqlspec.utils.type_guards import is_local_path


def test_is_local_path_type_guard() -> None:
    """Test is_local_path type guard function."""
    assert is_local_path("file:///absolute/path")
    assert is_local_path("file://C:/Windows/path")

    assert is_local_path("/absolute/path")
    assert is_local_path("C:\\Windows\\path")

    assert is_local_path("./relative/path")
    assert is_local_path("../parent/path")
    assert is_local_path("~/home/path")
    assert is_local_path("relative/path")

    assert not is_local_path("s3://bucket/key")
    assert not is_local_path("https://example.com")
    assert not is_local_path("gs://bucket")
    assert not is_local_path("")


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


def test_get_local_backend(tmp_path: Path) -> None:
    """Test getting local backend (when explicitly requested)."""
    registry = StorageRegistry()

    backend = registry.get(str(tmp_path), backend="local")
    assert backend.backend_type == "local"

    backend = registry.get(f"file://{tmp_path}", backend="local")
    assert backend.backend_type == "local"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_local_backend_prefers_obstore(tmp_path: Path) -> None:
    """Test that local paths prefer obstore when available."""
    registry = StorageRegistry()

    backend = registry.get(f"file://{tmp_path}")
    assert backend.backend_type == "obstore"

    backend = registry.get(str(tmp_path))
    assert backend.backend_type == "obstore"

    backend = registry.get(f"file://{tmp_path}", backend="local")
    assert backend.backend_type == "local"


def test_get_local_backend_fallback_priority(tmp_path: Path) -> None:
    """Test backend fallback priority for local paths."""
    registry = StorageRegistry()

    backend = registry.get(f"file://{tmp_path}")

    if OBSTORE_INSTALLED:
        assert backend.backend_type == "obstore"
    elif FSSPEC_INSTALLED:
        assert backend.backend_type == "fsspec"
    else:
        assert backend.backend_type == "local"


def test_get_alias(tmp_path: Path) -> None:
    """Test getting backend by alias."""
    registry = StorageRegistry()
    registry.register_alias("my_store", f"file://{tmp_path}")

    backend = registry.get("my_store")
    assert backend.backend_type in ("obstore", "fsspec", "local")


def test_get_with_backend_override(tmp_path: Path) -> None:
    """Test getting backend with override."""
    registry = StorageRegistry()

    backend = registry.get(f"file://{tmp_path}", backend="local")
    assert backend.backend_type == "local"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_get_fsspec_backend(tmp_path: Path) -> None:
    """Test getting fsspec backend."""
    registry = StorageRegistry()

    backend = registry.get(f"file://{tmp_path}", backend="fsspec")
    assert backend.backend_type == "fsspec"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_obstore_backend(tmp_path: Path) -> None:
    """Test getting obstore backend."""
    registry = StorageRegistry()

    backend = registry.get(f"file://{tmp_path}", backend="obstore")
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


def test_register_alias_with_base_path(tmp_path: Path) -> None:
    """Test alias registration with base_path."""
    registry = StorageRegistry()

    registry.register_alias("test_store", f"file://{tmp_path}/data")
    backend = registry.get("test_store")

    backend.write_text("test.txt", "content")
    assert backend.exists("test.txt")


def test_register_alias_with_backend_override(tmp_path: Path) -> None:
    """Test alias registration with backend override."""
    registry = StorageRegistry()

    registry.register_alias("test_store", f"file://{tmp_path}", backend="local")
    backend = registry.get("test_store")
    assert backend.backend_type == "local"


def test_cache_functionality(tmp_path: Path) -> None:
    """Test registry caching."""
    registry = StorageRegistry()

    backend1 = registry.get(f"file://{tmp_path}")
    backend2 = registry.get(f"file://{tmp_path}")

    assert backend1 is backend2


def test_clear_cache(tmp_path: Path) -> None:
    """Test cache clearing."""
    registry = StorageRegistry()

    backend1 = registry.get(f"file://{tmp_path}")
    registry.clear_cache(f"file://{tmp_path}")
    backend2 = registry.get(f"file://{tmp_path}")

    assert backend1 is not backend2


def test_clear_aliases() -> None:
    """Test clearing aliases."""
    registry = StorageRegistry()

    registry.register_alias("test_store", "file:///tmp")
    assert registry.is_alias_registered("test_store")

    registry.clear_aliases()
    assert not registry.is_alias_registered("test_store")
    assert len(registry.list_aliases()) == 0


def test_clear_instances(tmp_path: Path) -> None:
    """Test clearing instances."""
    registry = StorageRegistry()

    backend1 = registry.get(f"file://{tmp_path}")
    registry.clear_instances()
    backend2 = registry.get(f"file://{tmp_path}")

    assert backend1 is not backend2


def test_clear_all(tmp_path: Path) -> None:
    """Test clearing everything."""
    registry = StorageRegistry()

    registry.register_alias("test_store", f"file://{tmp_path}")
    backend1 = registry.get("test_store")

    registry.clear()

    assert not registry.is_alias_registered("test_store")
    assert len(registry.list_aliases()) == 0

    registry.register_alias("test_store", f"file://{tmp_path}")
    backend2 = registry.get("test_store")
    assert backend1 is not backend2


def test_path_object_conversion(tmp_path: Path) -> None:
    """Test Path object conversion to file:// URI."""
    registry = StorageRegistry()
    path_obj = tmp_path

    backend = registry.get(path_obj)
    assert backend.backend_type in ("obstore", "fsspec", "local")


def test_cloud_storage_without_backends() -> None:
    """Test cloud storage URIs without backends raise proper errors."""
    if OBSTORE_INSTALLED or FSSPEC_INSTALLED:
        pytest.skip("Storage backends are installed")

    registry = StorageRegistry()

    with pytest.raises(MissingDependencyError, match="No backend available"):
        registry.get("s3://bucket")

    with pytest.raises(MissingDependencyError, match="No backend available"):
        registry.get("gs://bucket")
