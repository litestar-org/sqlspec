"""Source-level regressions for storage registry hot paths.

These tests load the Python source module directly so they remain stable even
when a stale compiled extension exists in the workspace.
"""

import importlib.util
import types
from pathlib import Path
from typing import Any, ClassVar

import pytest

from sqlspec.exceptions import ImproperConfigurationError


class _SourceStoreStub:
    """Minimal ObjectStoreProtocol stub for source-loaded registry checks."""

    backend_type: ClassVar[str] = "custom"

    def __init__(self, uri: str, **kwargs: Any) -> None:
        self.uri = uri
        self.kwargs = kwargs
        self.protocol = str(kwargs.get("protocol", "custom"))

    def resolve_uri(self, path: str | Path) -> str:
        return f"{self.uri.rstrip('/')}/{path}"

    def read_bytes_sync(self, path: str | Path, **kwargs: Any) -> bytes:
        _ = (path, kwargs)
        return b""

    def write_bytes_sync(self, path: str | Path, data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        _ = (path, data, overwrite, kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        _ = (path, kwargs)
        return False


def _load_registry_source_module() -> "types.ModuleType":
    module_path = Path(__file__).resolve().parents[3] / "sqlspec" / "storage" / "registry.py"
    spec = importlib.util.spec_from_file_location("storage_registry_source_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_source_registry_clear_cache_removes_parameterized_uri_entries(tmp_path: Path) -> None:
    """Source implementation should evict backend-override cache entries by base URI."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()

    backend1 = registry.get(f"file://{tmp_path}", backend="local")
    registry.clear_cache(f"file://{tmp_path}")
    backend2 = registry.get(f"file://{tmp_path}", backend="local")

    assert backend1 is not backend2


def test_source_registry_register_backend_overwrite_evicts_cached_instances(tmp_path: Path) -> None:
    """Overwriting a custom backend in the source module evicts cached override entries."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    uri = f"file://{tmp_path}"

    def _first(target_uri: str, **kwargs: Any) -> _SourceStoreStub:
        return _SourceStoreStub(target_uri, version="v1", **kwargs)

    def _second(target_uri: str, **kwargs: Any) -> _SourceStoreStub:
        return _SourceStoreStub(target_uri, version="v2", **kwargs)

    registry.register_backend("oci", _first)
    first_instance = registry.get(uri, backend="OCI")
    assert registry.get(uri, backend="  oci  ") is first_instance

    registry.register_backend("oci", _second, overwrite=True)
    second_instance = registry.get(uri, backend="oci")
    assert second_instance is not first_instance
    assert second_instance.kwargs["version"] == "v2"


def test_source_registry_unregister_backend_evicts_cached_instances(tmp_path: Path) -> None:
    """Unregistering a custom backend in the source module evicts its cached instances."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    uri = f"file://{tmp_path}"

    registry.register_backend("oci", _SourceStoreStub)
    _ = registry.get(uri, backend="oci")
    local_instance = registry.get(uri, backend="local")

    assert registry.unregister_backend("  OCI  ") is True
    assert registry.unregister_backend("oci") is False
    assert registry.get(uri, backend="local") is local_instance
    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.get(uri, backend="oci")


def test_source_registry_clear_backends_evicts_only_custom_entries(tmp_path: Path) -> None:
    """clear_backends on the source module clears custom factories and keeps built-in cache entries."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    uri = f"file://{tmp_path}"

    registry.register_backend("oci", _SourceStoreStub)
    bare_local = registry.get(uri)
    explicit_local = registry.get(uri, backend="local")
    _ = registry.get(uri, backend="oci")

    registry.clear_backends()

    assert registry.list_backends() == ["fsspec", "local", "obstore"]
    assert registry.get(uri) is bare_local
    assert registry.get(uri, backend="local") is explicit_local
    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.get(uri, backend="oci")


@pytest.mark.parametrize("bad_name", [123, None, b"oci"], ids=["int", "none", "bytes"])
@pytest.mark.parametrize("method_name", ["register_backend", "unregister_backend", "is_backend_registered"])
def test_source_registry_register_backend_non_string_name_raises_type_error(method_name: str, bad_name: Any) -> None:
    """Source module enforces TypeError on non-str backend names matching MyPyC C-entry checks."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    with pytest.raises(TypeError):
        if method_name == "register_backend":
            registry.register_backend(bad_name, _SourceStoreStub)
        elif method_name == "unregister_backend":
            registry.unregister_backend(bad_name)
        else:
            registry.is_backend_registered(bad_name)


def test_source_registry_exports_storage_backend_factory() -> None:
    """Source module exposes StorageBackendFactory at runtime in __all__."""
    module = _load_registry_source_module()
    assert "StorageBackendFactory" in module.__all__
    assert hasattr(module, "StorageBackendFactory")


def test_source_registry_instance_alias_isolation_and_unregister(tmp_path: Path) -> None:
    """Source module isolates instance aliases from transient caches and drops them on unregister_alias."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    store = _SourceStoreStub("custom://inst")

    registry.register_alias("inst", backend=store)
    assert registry.is_alias_registered("inst") is True
    assert registry.get("inst") is store
    assert registry.get("inst/nested/query.sql") is store
    assert store not in registry._instances.values()

    registry.clear_cache("inst")
    registry.clear_cache()
    registry.clear_instances()
    assert registry.is_alias_registered("inst") is True
    assert registry.get("inst") is store

    with pytest.raises(ImproperConfigurationError, match="pre-configured instance"):
        registry.get("inst", backend="local")
    with pytest.raises(ImproperConfigurationError, match="pre-configured instance"):
        registry.get("inst/nested/query.sql", timeout=5)

    assert registry.unregister_alias("inst") is True
    assert registry.unregister_alias("inst") is False
    assert registry.is_alias_registered("inst") is False
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("inst")

    registry.register_alias("cfg", f"file://{tmp_path}", backend="local")
    first_cfg = registry.get("cfg")
    assert registry.unregister_alias("cfg") is True
    registry.register_alias("cfg", f"file://{tmp_path}", backend="local")
    assert registry.get("cfg") is not first_cfg


@pytest.mark.parametrize("bad_alias", [123, None, b"a"], ids=["int", "none", "bytes"])
def test_source_registry_alias_non_string_raises_type_error(bad_alias: Any) -> None:
    """Source module raises TypeError for non-str alias values matching MyPyC C-entry checks."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        registry.register_alias(bad_alias, "file:///tmp")
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        registry.unregister_alias(bad_alias)
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        registry.is_alias_registered(bad_alias)


@pytest.mark.parametrize("bad_uri", [123, Path("/tmp")], ids=["int", "path"])
def test_source_registry_non_string_uri_raises_type_error(bad_uri: Any) -> None:
    """Source module raises TypeError for non-None non-str uri matching MyPyC C-entry checks."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    with pytest.raises(TypeError, match="Storage URI must be a string or None"):
        registry.register_alias("inst", bad_uri)
    with pytest.raises(TypeError, match="Storage URI must be a string or None"):
        registry.register_alias("inst", bad_uri, backend=_SourceStoreStub("custom://x"))


def test_source_registry_non_string_base_path_raises_type_error() -> None:
    """Source module raises TypeError for non-str base_path."""
    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    with pytest.raises(TypeError, match="base_path must be a string"):
        registry.register_alias("cfg", "file:///tmp", base_path=123)


def test_source_registry_unhashable_override_skips_cache(tmp_path: Path) -> None:
    """Source module skips _instances caching for unhashable callable backend overrides."""

    class _UnhashableSourceFactory:
        __hash__ = None  # type: ignore[assignment]

        def __call__(self, target_uri: str, **kwargs: Any) -> _SourceStoreStub:
            return _SourceStoreStub(target_uri, **kwargs)

    module = _load_registry_source_module()
    registry = module.StorageRegistry()
    factory = _UnhashableSourceFactory()
    first = registry.get(f"file://{tmp_path}", backend=factory)
    second = registry.get(f"file://{tmp_path}", backend=factory)
    assert first is not second
    assert registry._instances == {}
