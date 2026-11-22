# pyright: reportAttributeAccessIssue=false
"""Unit tests for StorageRegistry."""

import inspect
import logging
from functools import partial
from pathlib import Path
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest

import sqlspec.storage as storage_pkg
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage import StorageBackendFactory
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.pipeline import SyncStoragePipeline
from sqlspec.storage.registry import StorageBackendFactory as RegistryStorageBackendFactory
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


def test_get_local_backend_prefers_local(tmp_path: Path) -> None:
    """Test that local paths prefer LocalStore by default even when obstore or fsspec is installed."""
    registry = StorageRegistry()
    backend = registry.get(f"file://{tmp_path}")
    assert backend.backend_type == "local"
    backend = registry.get(str(tmp_path))
    assert backend.backend_type == "local"


def test_get_alias(tmp_path: Path) -> None:
    """Test getting backend by alias."""
    registry = StorageRegistry()
    registry.register_alias("my_store", f"file://{tmp_path}")
    backend = registry.get("my_store")
    assert backend.backend_type == "local"


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
    backend.write_text_sync("test.txt", "content")
    assert backend.exists_sync("test.txt")


def test_register_alias_with_backend_override(tmp_path: Path) -> None:
    """Test alias registration with backend override."""
    registry = StorageRegistry()
    registry.register_alias("test_store", f"file://{tmp_path}", backend="local")
    backend = registry.get("test_store")
    assert backend.backend_type == "local"


def test_register_alias_evicts_stale_cache(tmp_path: Path) -> None:
    """Re-registering an alias should evict the previously cached backend."""
    registry = StorageRegistry()
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    registry.register_alias("test_store", f"file://{first_root}", backend="local")
    backend_a = registry.get("test_store")
    registry.register_alias("test_store", f"file://{second_root}", backend="local")
    backend_b = registry.get("test_store")
    assert backend_b is not backend_a
    assert str(backend_b.base_path) == str(second_root.resolve())


def test_register_alias_evicts_stale_cache_with_kwargs(tmp_path: Path) -> None:
    """Alias cache eviction must also clear keyed instances created with kwargs."""
    registry = StorageRegistry()
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    registry.register_alias("test_store", f"file://{first_root}", backend="local")
    backend_a = registry.get("test_store", cache_label="stable")
    registry.register_alias("test_store", f"file://{second_root}", backend="local")
    backend_b = registry.get("test_store", cache_label="stable")
    assert backend_b is not backend_a
    assert str(backend_b.base_path) == str(second_root.resolve())


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


def test_protocol_conformance_protocol_stream_arrow_async_is_not_coroutine_function() -> None:
    """ObjectStoreProtocol.stream_arrow_async is a plain def returning AsyncIterator."""
    assert not inspect.iscoroutinefunction(ObjectStoreProtocol.stream_arrow_async)


def test_protocol_conformance_local_store_satisfies_protocol(tmp_path: Path) -> None:
    """LocalStore structurally satisfies ObjectStoreProtocol."""
    assert isinstance(LocalStore(base_path=tmp_path), ObjectStoreProtocol)


def test_protocol_conformance_local_store_stream_arrow_async_is_not_coroutine_function() -> None:
    """Concrete backends should not expose stream_arrow_async as async def."""
    assert not inspect.iscoroutinefunction(LocalStore.stream_arrow_async)


def test_protocol_conformance_stream_arrow_async_notes_are_present() -> None:
    """Source comments document why stream_arrow_async is not async def."""
    assert "Returns AsyncIterator directly" in Path("sqlspec/protocols.py").read_text()
    assert "Returns AsyncIterator directly" in Path("sqlspec/storage/backends/base.py").read_text()


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_alias_named_like_a_scheme_does_not_capture_scheme_uris(tmp_path: Path) -> None:
    registry = StorageRegistry()
    registry.register_alias("memory", f"file://{tmp_path}")

    by_alias = registry.get("memory")
    by_scheme = registry.get("memory://bucket/key")

    assert by_alias.protocol == "file"
    assert by_scheme.protocol == "memory"
    assert by_scheme.store_uri == "memory://bucket/key"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_alias_does_not_match_partial_path_segment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    alias_target = tmp_path / "db_store"
    registry = StorageRegistry()
    registry.register_alias("db", f"file://{alias_target}")

    via_alias = registry.get("db/queries.sql")
    not_alias = registry.get("db_backup/queries.sql")

    assert isinstance(via_alias, LocalStore)
    assert isinstance(not_alias, LocalStore)
    assert via_alias.base_path == alias_target.resolve()
    assert not_alias.base_path.name == "db_backup"
    assert alias_target.resolve() != not_alias.base_path


def test_storage_registry_sanitizes_uri_credentials_in_logs_and_errors(caplog: pytest.LogCaptureFixture) -> None:
    """StorageRegistry strips userinfo and query parameters from logged and raised URIs."""
    registry = StorageRegistry()
    secret_uri = (
        "https://alice:top-secret-password@storage.example.com:8443/bucket/path?X-Amz-Signature=deadbeef&token=tok123"
    )
    expected_sanitized = "https://storage.example.com:8443/bucket/path"

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_alias("sanitized_store", secret_uri, backend="local")
        backend_first = registry.get(secret_uri, backend="local")
        backend_cached = registry.get(secret_uri, backend="local")

    assert backend_first is backend_cached
    resolve_uris = [
        getattr(record, "extra_fields", {}).get("uri_or_alias")
        for record in caplog.records
        if record.getMessage() == "storage.resolve"
    ]
    register_uris = [
        getattr(record, "extra_fields", {}).get("uri")
        for record in caplog.records
        if record.getMessage() == "storage.alias.register"
    ]
    assert resolve_uris == [expected_sanitized, expected_sanitized]
    assert register_uris == [expected_sanitized]
    combined_logs = "\n".join(
        f"{record.getMessage()} {getattr(record, 'extra_fields', {})}" for record in caplog.records
    )
    assert "alice" not in combined_logs
    assert "top-secret-password" not in combined_logs
    assert "deadbeef" not in combined_logs
    assert "tok123" not in combined_logs

    with pytest.raises(ImproperConfigurationError) as exc_info:
        registry.get("nonexistent_alias?token=should-not-leak")
    assert "should-not-leak" not in str(exc_info.value)
    assert "nonexistent_alias" in str(exc_info.value)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize(
    ("uri", "options"),
    [
        ("s3a://example-bucket/dir/key.parquet", {"skip_signature": True, "region": "us-east-1"}),
        ("abfs://container@account.dfs.core.windows.net/dir/key.parquet", {"skip_signature": True}),
        ("abfss://container@account.dfs.core.windows.net/dir/key.parquet", {"skip_signature": True}),
    ],
    ids=["s3a", "abfs", "abfss"],
)
def test_storage_pipeline_strips_bucket_from_s3a_abfs_and_abfss_object_keys(
    uri: str, options: "dict[str, Any]"
) -> None:
    """The pipeline resolves s3a, abfs, and abfss URIs to a bucket-scoped backend and a bucket-relative key."""
    pipeline = SyncStoragePipeline(registry=StorageRegistry())

    backend, key, backend_name = pipeline._backend(uri, options)

    assert backend_name == "obstore"
    assert key == "dir/key.parquet"
    assert backend.resolve_uri(key) == uri


class _CustomStoreStub:
    """Minimal ObjectStoreProtocol-shaped stub for named backend tests."""

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


class _CallableCustomStoreStub(_CustomStoreStub):
    """Pre-configured backend instance that is also callable."""

    def __call__(self, uri: str, **kwargs: Any) -> "_CallableCustomStoreStub":
        return _CallableCustomStoreStub(uri, **kwargs)


class _CallableFactoryObject:
    """Callable object factory lacking a ``__name__`` attribute."""

    def __init__(self, marker: str = "callable-object") -> None:
        self.marker = marker

    def __call__(self, uri: str, **kwargs: Any) -> _CustomStoreStub:
        return _CustomStoreStub(uri, marker=self.marker, **kwargs)


def _custom_factory_function(uri: str, **kwargs: Any) -> _CustomStoreStub:
    return _CustomStoreStub(uri, via="function", **kwargs)


def test_list_backends_defaults() -> None:
    """Built-in backends are listed in sorted order at startup."""
    registry = StorageRegistry()
    assert registry.list_backends() == ["fsspec", "local", "obstore"]


@pytest.mark.parametrize("name", ["local", "LOCAL", " obstore ", "fsspec", "  FsSpEc  "])
def test_is_backend_registered_builtins(name: str) -> None:
    """Built-in backend lookup strips whitespace and folds case."""
    registry = StorageRegistry()
    assert registry.is_backend_registered(name) is True


def test_register_backend_and_get_with_normalization(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Registering a custom backend enables case- and whitespace-insensitive lookups and shared caching."""
    registry = StorageRegistry()
    calls: list[str] = []

    def _oci_factory(uri: str, **kwargs: Any) -> _CustomStoreStub:
        calls.append(uri)
        return _CustomStoreStub(uri, protocol="oci", **kwargs)

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_backend("  OCI  ", cast("Any", _oci_factory))

    assert registry.is_backend_registered("oci") is True
    assert registry.is_backend_registered("  OCI ") is True
    assert registry.list_backends() == ["fsspec", "local", "obstore", "oci"]

    target_uri = f"file://{tmp_path}"
    first = registry.get(target_uri, backend="OCI")
    second = registry.get(target_uri, backend="  oci  ")

    assert first is second
    assert isinstance(first, _CustomStoreStub)
    assert first.protocol == "oci"
    assert first.backend_type == "custom"
    assert len(calls) == 1

    register_records = [r for r in caplog.records if r.getMessage() == "storage.backend.register"]
    assert len(register_records) == 1
    extra = getattr(register_records[0], "extra_fields", {})
    assert extra.get("backend") == "oci"
    assert extra.get("factory") == "_oci_factory"
    assert extra.get("overwrite") is False


@pytest.mark.parametrize(
    ("factory_kind", "expected_label"),
    [
        ("class", "_CustomStoreStub"),
        ("function", "_custom_factory_function"),
        ("partial", "partial"),
        ("callable_object", "_CallableFactoryObject"),
        ("magic_mock", "MagicMock"),
    ],
)
def test_register_backend_callable_kinds(
    factory_kind: str, expected_label: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """All supported callable factory shapes register, invoke, and log without AttributeError."""
    registry = StorageRegistry()
    mock_result = _CustomStoreStub(f"file://{tmp_path}", protocol="mocked")
    factories: dict[str, Any] = {
        "class": _CustomStoreStub,
        "function": _custom_factory_function,
        "partial": partial(_custom_factory_function, region="us-phoenix-1"),
        "callable_object": _CallableFactoryObject(marker="obj"),
        "magic_mock": MagicMock(return_value=mock_result),
    }
    factory = factories[factory_kind]

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_backend("oci", factory)
        instance = registry.get(f"file://{tmp_path}", backend="oci")

    assert isinstance(instance, _CustomStoreStub)
    register_records = [r for r in caplog.records if r.getMessage() == "storage.backend.register"]
    assert len(register_records) == 1
    extra = getattr(register_records[0], "extra_fields", {})
    assert extra.get("backend") == "oci"
    assert extra.get("factory") == expected_label
    assert extra.get("overwrite") is False


@pytest.mark.parametrize("use_partial", [False, True], ids=["class", "partial"])
def test_register_backend_via_register_alias(use_partial: bool, caplog: pytest.LogCaptureFixture) -> None:
    """Aliases bound to a custom backend name resolve through that factory and preserve metadata."""
    registry = StorageRegistry()
    factory: Any = (
        partial(_custom_factory_function, protocol="oci", namespace="corp") if use_partial else _CustomStoreStub
    )
    registry.register_backend("oci", factory)

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_alias("data", "custom://bucket", backend="  OCI  ", protocol="oci")

    instance = registry.get("data")
    assert isinstance(instance, _CustomStoreStub)
    assert instance.uri == "custom://bucket"
    assert instance.protocol == "oci"
    assert instance.backend_type == "custom"
    alias_logs = [r for r in caplog.records if r.getMessage() == "storage.alias.register"]
    assert len(alias_logs) == 1
    expected_name = "partial" if use_partial else "_CustomStoreStub"
    assert getattr(alias_logs[0], "extra_fields", {}).get("backend_type") == expected_name


def test_register_alias_captures_backend_factory_across_overwrite_and_unregister() -> None:
    """Stored alias factory bindings survive later custom backend overwrite and unregistration."""
    registry = StorageRegistry()

    def _first(uri: str, **kwargs: Any) -> _CustomStoreStub:
        return _CustomStoreStub(uri, version=1, **kwargs)

    def _second(uri: str, **kwargs: Any) -> _CustomStoreStub:
        return _CustomStoreStub(uri, version=2, **kwargs)

    registry.register_backend("oci", cast("Any", _first))
    registry.register_alias("data", "oci://bucket", backend="oci")

    registry.register_backend("oci", cast("Any", _second), overwrite=True)
    registry.clear_instances()
    from_overwrite = registry.get("data")
    assert isinstance(from_overwrite, _CustomStoreStub)
    assert from_overwrite.kwargs["version"] == 1

    assert registry.unregister_backend("oci") is True
    registry.clear_instances()
    after_unregister = registry.get("data")
    assert isinstance(after_unregister, _CustomStoreStub)
    assert after_unregister.kwargs["version"] == 1


@pytest.mark.parametrize("bad_name", [123, None, b"oci"], ids=["int", "none", "bytes"])
@pytest.mark.parametrize("method_name", ["register_backend", "unregister_backend", "is_backend_registered"])
def test_register_backend_non_string_name_raises_type_error(method_name: str, bad_name: Any) -> None:
    """Non-str backend names raise TypeError to match MyPyC C-entry signature checks."""
    registry = StorageRegistry()
    with pytest.raises(TypeError):
        if method_name == "register_backend":
            registry.register_backend(bad_name, cast("Any", _CustomStoreStub))
        elif method_name == "unregister_backend":
            registry.unregister_backend(bad_name)
        else:
            registry.is_backend_registered(bad_name)


@pytest.mark.parametrize("duplicate_name", ["oci", "OCI", "  oci  "])
def test_register_backend_duplicate_rejected(duplicate_name: str) -> None:
    """Registering a duplicate backend without overwrite raises ValueError."""
    registry = StorageRegistry()
    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    with pytest.raises(ValueError, match="Storage backend 'oci' is already registered"):
        registry.register_backend(duplicate_name, cast("Any", _custom_factory_function))


def test_register_backend_overwrite_replaces_and_evicts_cache(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Overwriting a custom backend replaces the factory and evicts cached instances for that name."""
    registry = StorageRegistry()

    def _first(uri: str, **kwargs: Any) -> _CustomStoreStub:
        return _CustomStoreStub(uri, version="v1", **kwargs)

    def _second(uri: str, **kwargs: Any) -> _CustomStoreStub:
        return _CustomStoreStub(uri, version="v2", **kwargs)

    registry.register_backend("oci", cast("Any", _first))
    uri = f"file://{tmp_path}"
    first_instance = registry.get(uri, backend="oci")
    first_kw_instance = registry.get(uri, backend="OCI", region="phoenix")
    local_instance = registry.get(uri, backend="local")

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_backend("  OCI  ", cast("Any", _second), overwrite=True)

    second_instance = registry.get(uri, backend="oci")
    second_kw_instance = registry.get(uri, backend="oci", region="phoenix")
    assert second_instance is not first_instance
    assert second_kw_instance is not first_kw_instance
    assert isinstance(second_instance, _CustomStoreStub)
    assert second_instance.kwargs["version"] == "v2"
    assert registry.get(uri, backend="local") is local_instance

    register_records = [r for r in caplog.records if r.getMessage() == "storage.backend.register"]
    assert len(register_records) == 1
    assert getattr(register_records[0], "extra_fields", {}).get("overwrite") is True


@pytest.mark.parametrize(
    ("raw_name", "expected_normalized"),
    [("local", "local"), ("LOCAL", "local"), (" obstore ", "obstore"), ("fsspec", "fsspec")],
)
def test_register_and_unregister_backend_protect_builtins(raw_name: str, expected_normalized: str) -> None:
    """Built-in backend names cannot be overridden or unregistered."""
    registry = StorageRegistry()
    with pytest.raises(ValueError, match=f"Cannot override built-in storage backend: '{expected_normalized}'"):
        registry.register_backend(raw_name, cast("Any", _CustomStoreStub), overwrite=True)
    with pytest.raises(ValueError, match=f"Cannot unregister built-in storage backend: '{expected_normalized}'"):
        registry.unregister_backend(raw_name)


@pytest.mark.parametrize("invalid_factory", [123, None, "local", object()], ids=["int", "none", "str", "object"])
def test_register_backend_rejects_non_callable_factory(invalid_factory: Any) -> None:
    """Non-callable backend factories raise TypeError."""
    registry = StorageRegistry()
    with pytest.raises(TypeError):
        registry.register_backend("oci", invalid_factory)


@pytest.mark.parametrize("callable_instance", [False, True], ids=["plain_instance", "callable_instance"])
def test_register_backend_rejects_store_instance(callable_instance: bool) -> None:
    """Pre-configured ObjectStoreProtocol instances raise TypeError naming register_alias."""
    registry = StorageRegistry()
    instance: Any = (
        _CallableCustomStoreStub("custom://bucket") if callable_instance else _CustomStoreStub("custom://bucket")
    )
    with pytest.raises(TypeError, match="register_alias"):
        registry.register_backend("oci", instance)


@pytest.mark.parametrize("empty_name", ["", "   ", "\t\n"])
def test_register_backend_empty_name_raises_value_error(empty_name: str, tmp_path: Path) -> None:
    """Empty or whitespace-only backend names raise ValueError on register/resolve entrypoints."""
    registry = StorageRegistry()
    registry.register_alias("local_alias", f"file://{tmp_path}")

    with pytest.raises(ValueError, match=r"^Backend name cannot be empty\.$"):
        registry.register_backend(empty_name, cast("Any", _CustomStoreStub))
    with pytest.raises(ValueError, match=r"^Backend name cannot be empty\.$"):
        registry.register_alias("other", f"file://{tmp_path}", backend=empty_name)
    with pytest.raises(ValueError, match=r"^Backend name cannot be empty\.$"):
        registry.get(f"file://{tmp_path}", backend=empty_name)
    with pytest.raises(ValueError, match=r"^Backend name cannot be empty\.$"):
        registry.get("local_alias", backend=empty_name)


def test_get_unknown_alias_with_empty_backend_raises_improper_configuration_first() -> None:
    """Unknown alias detection runs before empty backend name normalization."""
    registry = StorageRegistry()
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("nonexistent_alias", backend="")


@pytest.mark.parametrize("empty_name", ["", "   ", "\t\n"])
def test_empty_backend_name_returns_false_for_lookup_and_unregister(empty_name: str) -> None:
    """Empty or whitespace-only backend names return False on unregister and membership check."""
    registry = StorageRegistry()
    assert registry.unregister_backend(empty_name) is False
    assert registry.is_backend_registered(empty_name) is False


def test_unknown_backend_error_lists_registered_backends(tmp_path: Path) -> None:
    """Unknown backend errors include all sorted built-in and custom backend names."""
    registry = StorageRegistry()
    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    with pytest.raises(ValueError, match="Unknown backend type") as exc_info:
        registry.get(f"file://{tmp_path}", backend="missing")
    message = str(exc_info.value)
    assert "'fsspec', 'local', 'obstore', 'oci'" in message


def test_unregister_backend_evicts_cached_instances(tmp_path: Path) -> None:
    """Unregistering a custom backend returns True once and evicts its cached instances."""
    registry = StorageRegistry()
    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    uri = f"file://{tmp_path}"
    _ = registry.get(uri, backend="oci")
    local_inst = registry.get(uri, backend="local")

    assert registry.unregister_backend("  OCI  ") is True
    assert registry.unregister_backend("oci") is False
    assert registry.is_backend_registered("oci") is False
    assert registry.list_backends() == ["fsspec", "local", "obstore"]
    assert registry.get(uri, backend="local") is local_inst

    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.get(uri, backend="oci")


def test_clear_backends_isolates_builtin_and_bare_cached_entries(tmp_path: Path) -> None:
    """clear_backends drops custom factories and evicts only instances built with them."""
    registry = StorageRegistry()
    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    uri = f"file://{tmp_path}"
    bare_local = registry.get(uri)
    explicit_local = registry.get(uri, backend="local")
    _ = registry.get(uri, backend="oci")
    _ = registry.get(uri, backend="oci", bucket="alpha")

    registry.clear_backends()

    assert registry.list_backends() == ["fsspec", "local", "obstore"]
    assert registry.is_backend_registered("oci") is False
    assert registry.get(uri) is bare_local
    assert registry.get(uri, backend="local") is explicit_local
    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.get(uri, backend="oci")


def test_clear_resets_custom_backends_and_aliases(tmp_path: Path) -> None:
    """clear() removes aliases, cached instances, and custom backends."""
    registry = StorageRegistry()
    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    registry.register_alias("data", f"file://{tmp_path}", backend="oci")
    _ = registry.get("data")

    registry.clear()

    assert registry.list_backends() == ["fsspec", "local", "obstore"]
    assert registry.list_aliases() == []
    assert registry.is_backend_registered("oci") is False


def test_storage_backend_factory_lazy_export() -> None:
    """StorageBackendFactory is lazily re-exported from sqlspec.storage."""
    assert StorageBackendFactory is RegistryStorageBackendFactory
    assert "StorageBackendFactory" in storage_pkg.__all__
    assert "StorageBackendFactory" in dir(storage_pkg)


class _PartialStoreDouble:
    """Incomplete store double missing ``read_bytes_sync``."""

    backend_type: ClassVar[str] = "partial"
    protocol: str = "partial"

    def resolve_uri(self, path: str | Path) -> str:
        return str(path)

    def write_bytes_sync(self, path: str | Path, data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        _ = (path, data, overwrite, kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        _ = (path, kwargs)
        return False


class _RecordingCallableStore(_CustomStoreStub):
    """Callable backend instance recording whether ``__call__`` is invoked."""

    def __init__(self, uri: str, **kwargs: Any) -> None:
        super().__init__(uri, **kwargs)
        self.call_count = 0

    def __call__(self, uri: str, **kwargs: Any) -> "_RecordingCallableStore":
        self.call_count += 1
        return _RecordingCallableStore(uri, **kwargs)


class _UnhashableFactory:
    """Unhashable callable backend factory forcing cache bypass."""

    __hash__ = None  # type: ignore[assignment]

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, uri: str, **kwargs: Any) -> _CustomStoreStub:
        self.calls.append((uri, dict(kwargs)))
        return _CustomStoreStub(uri, invocation=len(self.calls), **kwargs)


def test_register_alias_with_class_backend_and_caching() -> None:
    """register_alias accepts a backend class directly and caches resolved instances."""
    registry = StorageRegistry()
    registry.register_alias(
        "cls_alias",
        "custom://bucket",
        backend=cast("Any", _CustomStoreStub),
        base_path="sub",
        token="test-token-placeholder",
    )

    assert registry.is_alias_registered("cls_alias") is True
    assert registry.list_aliases() == ["cls_alias"]

    first = registry.get("cls_alias")
    second = registry.get("cls_alias")
    assert first is second
    assert isinstance(first, _CustomStoreStub)
    assert first.uri == "custom://bucket"
    assert first.kwargs["base_path"] == "sub"
    assert first.kwargs["token"] == "test-token-placeholder"


def test_register_alias_with_local_store_class(tmp_path: Path) -> None:
    """Typed ObjectStoreProtocol subclass (LocalStore) registers directly without casts."""
    registry = StorageRegistry()
    registry.register_alias("typed_local", f"file://{tmp_path}", backend=LocalStore)
    first = registry.get("typed_local")
    second = registry.get("typed_local")
    assert first is second
    assert isinstance(first, LocalStore)


@pytest.mark.parametrize(
    ("factory_kind", "expected_label"),
    [
        ("function", "_custom_factory_function"),
        ("lambda", "<lambda>"),
        ("partial", "partial"),
        ("callable_object", "_CallableFactoryObject"),
        ("magic_mock", "MagicMock"),
    ],
)
def test_register_alias_with_callable_factory_kinds(
    factory_kind: str, expected_label: str, caplog: pytest.LogCaptureFixture
) -> None:
    """register_alias accepts every callable factory shape, caches instances, and logs safely."""
    registry = StorageRegistry()
    mock_result = _CustomStoreStub("custom://mocked", protocol="mocked")
    factories: dict[str, Any] = {
        "function": _custom_factory_function,
        "lambda": lambda uri, **kwargs: _CustomStoreStub(uri, via="lambda", **kwargs),
        "partial": partial(_custom_factory_function, region="us-east-1"),
        "callable_object": _CallableFactoryObject(marker="obj-marker"),
        "magic_mock": MagicMock(return_value=mock_result),
    }
    factory = factories[factory_kind]

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_alias("factory_alias", "custom://bucket", backend=factory, token="test-token-placeholder")

    first = registry.get("factory_alias")
    second = registry.get("factory_alias")
    assert first is second
    assert isinstance(first, _CustomStoreStub)

    register_records = [r for r in caplog.records if r.getMessage() == "storage.alias.register"]
    assert len(register_records) == 1
    assert getattr(register_records[0], "extra_fields", {}).get("backend_type") == expected_label


@pytest.mark.parametrize("uri_value", [None, "", "   "], ids=["none", "empty", "whitespace"])
def test_register_alias_instance_alias_without_uri(uri_value: str | None, caplog: pytest.LogCaptureFixture) -> None:
    """Pre-configured instance aliases register without a URI and stay outside _instances."""
    registry = StorageRegistry()
    store = cast("ObjectStoreProtocol", _CustomStoreStub("custom://preconfigured", protocol="oci"))

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        if uri_value is None:
            registry.register_alias("inst", backend=store)
        else:
            registry.register_alias("inst", uri_value, backend=store)

    assert registry.is_alias_registered("inst") is True
    assert registry.list_aliases() == ["inst"]
    assert registry.get("inst") is store
    assert registry.get("inst/nested/query.sql") is store
    assert store not in registry._instances.values()
    assert "inst" not in registry._instances
    assert "inst" not in registry._alias_configs

    register_logs = [r for r in caplog.records if r.getMessage() == "storage.alias.register"]
    assert len(register_logs) == 1
    extra = getattr(register_logs[0], "extra_fields", {})
    assert extra.get("alias") == "inst"
    assert extra.get("uri") is None
    assert extra.get("base_path") is None
    assert extra.get("backend_type") == "_CustomStoreStub"


def test_register_alias_instance_alias_with_real_local_store(tmp_path: Path) -> None:
    """Typed LocalStore instance satisfies the pre-configured instance overload directly."""
    registry = StorageRegistry()
    store = LocalStore(base_path=tmp_path)
    registry.register_alias("local_inst", backend=store)
    assert registry.get("local_inst") is store
    assert registry.get("local_inst/subdir/file.sql") is store


def test_register_alias_instance_alias_sanitizes_informational_uri(caplog: pytest.LogCaptureFixture) -> None:
    """Informational URIs passed with pre-configured instances are sanitized in debug logs."""
    registry = StorageRegistry()
    store = cast("ObjectStoreProtocol", _CustomStoreStub("https://host/bucket", protocol="https"))
    secret_uri = "https://user:top-secret-password@host.example.com/bucket?token=test-token-placeholder&sig=secret"

    with caplog.at_level(logging.DEBUG, logger="sqlspec"):
        registry.register_alias("inst", secret_uri, backend=store)
        resolved = registry.get("inst/nested/a.sql")

    assert resolved is store
    register_logs = [r for r in caplog.records if r.getMessage() == "storage.alias.register"]
    assert len(register_logs) == 1
    assert getattr(register_logs[0], "extra_fields", {}).get("uri") == "https://host.example.com/bucket"
    resolve_logs = [r for r in caplog.records if r.getMessage() == "storage.resolve"]
    assert len(resolve_logs) == 1
    assert getattr(resolve_logs[0], "extra_fields", {}).get("cached") is True

    combined = "\n".join(f"{record.getMessage()} {getattr(record, 'extra_fields', {})}" for record in caplog.records)
    assert "top-secret-password" not in combined
    assert "test-token-placeholder" not in combined
    assert "sig=secret" not in combined


def test_register_alias_callable_store_instance_is_not_invoked() -> None:
    """Callable backend instances register inside _alias_instances without calling __call__."""
    registry = StorageRegistry()
    callable_store = _RecordingCallableStore("custom://callable")
    callable_store_proto = cast("ObjectStoreProtocol", callable_store)

    registry.register_alias("callable_inst", backend=callable_store_proto)

    assert "callable_inst" in registry._alias_instances
    assert "callable_inst" not in registry._alias_configs
    assert registry.get("callable_inst") is callable_store_proto
    assert registry.get("callable_inst/subdir/query.sql") is callable_store_proto
    assert callable_store.call_count == 0


@pytest.mark.parametrize(
    ("extra_kwargs",),
    [
        ({"base_path": "sub"},),
        ({"base_path": " "},),
        ({"region": "us-east-1"},),
        ({"base_path": "sub", "region": "us-east-1"},),
    ],
    ids=["base_path", "whitespace_base_path", "kwarg", "both"],
)
def test_register_alias_instance_alias_rejects_options(extra_kwargs: dict[str, Any]) -> None:
    """Instance aliases reject non-empty base_path or extra kwargs at runtime."""
    registry = StorageRegistry()
    store = _CustomStoreStub("custom://bucket")
    with pytest.raises(ImproperConfigurationError, match="pre-configured instance"):
        cast("Any", registry).register_alias("inst", backend=store, **extra_kwargs)
    assert registry.is_alias_registered("inst") is False
    assert registry.list_aliases() == []


def test_register_alias_instance_alias_rejects_overload_option_with_direct_call(tmp_path: Path) -> None:
    """Direct untyped call passing base_path to an instance alias raises ImproperConfigurationError."""
    registry = StorageRegistry()
    store = LocalStore(base_path=tmp_path)
    with pytest.raises(ImproperConfigurationError, match="pre-configured instance"):
        registry.register_alias("inst", backend=store, base_path="sub")  # type: ignore[call-overload]  # pyright: ignore[reportCallIssue]


def test_register_alias_failed_reregistration_is_atomic(tmp_path: Path) -> None:
    """Invalid re-registrations must leave the previous alias entry and cache intact."""
    registry = StorageRegistry()
    registry.register_alias("existing_cfg", f"file://{tmp_path}", backend="local")
    cached_cfg = registry.get("existing_cfg")

    new_store = _CustomStoreStub("custom://replacement")
    with pytest.raises(ImproperConfigurationError):
        cast("Any", registry).register_alias("existing_cfg", backend=new_store, base_path="bad")
    with pytest.raises(ImproperConfigurationError):
        cast("Any", registry).register_alias("existing_cfg", None, backend="local")
    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.register_alias("existing_cfg", f"file://{tmp_path}", backend="missing_backend")
    with pytest.raises(TypeError):
        cast("Any", registry).register_alias("existing_cfg", backend=123)
    with pytest.raises(TypeError):
        cast("Any", registry).register_alias("existing_cfg", 123)

    assert registry.is_alias_registered("existing_cfg") is True
    assert registry.get("existing_cfg") is cached_cfg

    existing_store = cast("ObjectStoreProtocol", _CustomStoreStub("custom://original"))
    registry.register_alias("existing_inst", backend=existing_store)
    with pytest.raises(ImproperConfigurationError):
        cast("Any", registry).register_alias("existing_inst", backend=new_store, region="us")
    with pytest.raises(ImproperConfigurationError):
        cast("Any", registry).register_alias("existing_inst", None, backend="local")
    with pytest.raises(ValueError, match="Unknown backend type"):
        registry.register_alias("existing_inst", f"file://{tmp_path}", backend="missing_backend")
    with pytest.raises(TypeError):
        cast("Any", registry).register_alias("existing_inst", backend=object())

    assert registry.is_alias_registered("existing_inst") is True
    assert registry.get("existing_inst") is existing_store


def test_register_alias_reregistration_swaps_kind_and_evicts_cache(tmp_path: Path) -> None:
    """Re-registering switches cleanly across config -> instance -> config -> instance2."""
    registry = StorageRegistry()
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    inst_one = cast("ObjectStoreProtocol", _CustomStoreStub("custom://one"))
    inst_two = cast("ObjectStoreProtocol", _CustomStoreStub("custom://two"))

    registry.register_alias("swap", f"file://{root_a}", backend="local")
    cfg_a = registry.get("swap")
    _ = registry.get("swap", cache_tag="extra")
    assert "swap" in registry._alias_configs
    assert "swap" not in registry._alias_instances

    registry.register_alias("swap", backend=inst_one)
    assert "swap" not in registry._alias_configs
    assert "swap" in registry._alias_instances
    assert registry._instances == {}
    assert registry.get("swap") is inst_one
    assert registry.list_aliases() == ["swap"]

    registry.register_alias("swap", f"file://{root_b}", backend="local")
    assert "swap" in registry._alias_configs
    assert "swap" not in registry._alias_instances
    cfg_b = registry.get("swap")
    assert cfg_b is not cfg_a
    assert cfg_b is not inst_one
    assert str(cfg_b.base_path) == str(root_b.resolve())

    registry.register_alias("swap", backend=inst_two)
    assert "swap" not in registry._alias_configs
    assert "swap" in registry._alias_instances
    assert registry._instances == {}
    assert registry.get("swap") is inst_two
    assert registry.list_aliases() == ["swap"]


@pytest.mark.parametrize("bad_alias", [123, None, b"alias"], ids=["int", "none", "bytes"])
def test_alias_methods_reject_non_string_alias_with_type_error(bad_alias: Any) -> None:
    """Non-str alias raises TypeError across register_alias, unregister_alias, and is_alias_registered."""
    registry = StorageRegistry()
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        cast("Any", registry).register_alias(bad_alias, "file:///tmp")
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        registry.unregister_alias(bad_alias)
    with pytest.raises(TypeError, match="Storage alias must be a string"):
        registry.is_alias_registered(bad_alias)


@pytest.mark.parametrize("bad_uri", [123, Path("/tmp"), b"file:///tmp"], ids=["int", "path", "bytes"])
def test_register_alias_rejects_non_string_uri_with_type_error(bad_uri: Any) -> None:
    """Non-None non-str uri raises TypeError matching MyPyC C-entry checks."""
    registry = StorageRegistry()
    store = _CustomStoreStub("custom://bucket")
    with pytest.raises(TypeError, match="Storage URI must be a string or None"):
        cast("Any", registry).register_alias("bad_uri", bad_uri)
    with pytest.raises(TypeError, match="Storage URI must be a string or None"):
        cast("Any", registry).register_alias("bad_uri_inst", bad_uri, backend=store)


@pytest.mark.parametrize("bad_base_path", [123, Path("/tmp"), None], ids=["int", "path", "none"])
def test_register_alias_rejects_non_string_base_path_with_type_error(bad_base_path: Any) -> None:
    """Non-str base_path raises TypeError matching MyPyC C-entry checks."""
    registry = StorageRegistry()
    with pytest.raises(TypeError, match="base_path must be a string"):
        cast("Any", registry).register_alias("bad_bp", "file:///tmp", base_path=bad_base_path)


@pytest.mark.parametrize(
    "bad_alias",
    ["", "   ", "\t\n", "a/b", "a\\b", "memory:", "C:", "alias://name", "s3://bucket", " pad ", "pad ", " pad"],
)
def test_register_alias_rejects_invalid_alias_names(bad_alias: str) -> None:
    """Invalid alias strings with whitespace, slashes, backslashes, or colons raise ImproperConfigurationError."""
    registry = StorageRegistry()
    with pytest.raises(ImproperConfigurationError):
        registry.register_alias(bad_alias, "file:///tmp")


@pytest.mark.parametrize("empty_uri", [None, "", "   "], ids=["none", "empty", "whitespace"])
@pytest.mark.parametrize(
    "backend_arg",
    [None, "local", _CustomStoreStub, _custom_factory_function],
    ids=["auto", "name", "class", "function"],
)
def test_register_alias_requires_non_empty_uri_for_non_instance_backends(
    empty_uri: str | None, backend_arg: Any
) -> None:
    """Empty or missing URIs raise ImproperConfigurationError for auto, named, class, and factory backends."""
    registry = StorageRegistry()
    with pytest.raises(ImproperConfigurationError, match="non-empty string"):
        cast("Any", registry).register_alias("empty_uri_alias", empty_uri, backend=backend_arg)


@pytest.mark.parametrize(
    "bad_backend", [123, True, object(), _PartialStoreDouble()], ids=["int", "bool", "object", "partial_double"]
)
@pytest.mark.parametrize("uri_arg", [None, "file:///tmp"], ids=["without_uri", "with_uri"])
def test_register_alias_invalid_backend_raises_type_error_before_uri_check(
    bad_backend: Any, uri_arg: str | None
) -> None:
    """Non-callable, non-store backend raises TypeError immediately even when uri is omitted."""
    registry = StorageRegistry()
    with pytest.raises(TypeError):
        cast("Any", registry).register_alias("bad", uri_arg, backend=bad_backend)


@pytest.mark.parametrize(
    ("target", "kwargs"),
    [
        ("inst", {"backend": "local"}),
        ("inst", {"backend": LocalStore}),
        ("inst", {"backend": 123}),
        ("inst", {"backend": ""}),
        ("inst", {"region": "us-east-1"}),
        ("inst/a.sql", {"region": "us-east-1"}),
        ("inst/subdir/query.sql", {"timeout": 5}),
    ],
    ids=["backend_str", "backend_cls", "backend_int", "backend_empty", "kwarg", "subpath_kwarg", "nested_timeout"],
)
def test_get_instance_alias_rejects_overrides_before_backend_validation(target: str, kwargs: dict[str, Any]) -> None:
    """Overrides and kwargs on instance-backed aliases raise ImproperConfigurationError immediately."""
    registry = StorageRegistry()
    store = _CustomStoreStub("custom://bucket")
    registry.register_alias("inst", backend=cast("ObjectStoreProtocol", store))
    with pytest.raises(ImproperConfigurationError, match="pre-configured instance"):
        registry.get(target, **kwargs)


@pytest.mark.parametrize("bad_backend", [123, "", _CustomStoreStub("custom://x")], ids=["int", "empty", "instance"])
def test_get_unknown_alias_checked_before_backend_validation(bad_backend: Any) -> None:
    """Unknown alias errors precede backend override validation."""
    registry = StorageRegistry()
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("missing_alias", backend=bad_backend)


@pytest.mark.parametrize(
    "bad_backend", [123, True, object(), _PartialStoreDouble()], ids=["int", "bool", "object", "partial_double"]
)
def test_get_rejects_non_callable_backend_override(tmp_path: Path, bad_backend: Any) -> None:
    """Non-callable backend overrides on get() raise TypeError."""
    registry = StorageRegistry()
    registry.register_alias("cfg", f"file://{tmp_path}")
    with pytest.raises(TypeError):
        registry.get(f"file://{tmp_path}", backend=bad_backend)
    with pytest.raises(TypeError):
        registry.get("cfg", backend=bad_backend)


@pytest.mark.parametrize("callable_instance", [False, True], ids=["plain", "callable"])
def test_get_rejects_store_instance_override_with_register_alias_hint(tmp_path: Path, callable_instance: bool) -> None:
    """Passing a pre-configured store instance as backend= to get() raises TypeError mentioning register_alias."""
    registry = StorageRegistry()
    store: Any = _RecordingCallableStore("custom://inst") if callable_instance else _CustomStoreStub("custom://inst")
    with pytest.raises(TypeError, match="register_alias"):
        registry.get(f"file://{tmp_path}", backend=store)


def test_get_hashable_callable_override_caches_per_factory(tmp_path: Path) -> None:
    """Hashable class, function, lambda, and partial backend overrides cache cleanly and isolate keys."""
    registry = StorageRegistry()
    uri = f"file://{tmp_path}"
    partial_factory = partial(_custom_factory_function, via_tag="partial")
    lambdas: tuple[Any, Any] = (
        lambda target_uri, **kw: _CustomStoreStub(target_uri, kind="a", **kw),
        lambda target_uri, **kw: _CustomStoreStub(target_uri, kind="b", **kw),
    )
    lambda_a, lambda_b = lambdas

    cls_1 = registry.get(uri, backend=cast("Any", _CustomStoreStub))
    cls_2 = registry.get(uri, backend=cast("Any", _CustomStoreStub))
    assert cls_1 is cls_2

    fn_1 = registry.get(uri, backend=cast("Any", _custom_factory_function))
    fn_2 = registry.get(uri, backend=cast("Any", _custom_factory_function))
    assert fn_1 is fn_2
    assert fn_1 is not cls_1

    lam_a_1 = registry.get(uri, backend=lambda_a)
    lam_a_2 = registry.get(uri, backend=lambda_a)
    lam_b_1 = registry.get(uri, backend=lambda_b)
    assert lam_a_1 is lam_a_2
    assert lam_a_1 is not lam_b_1

    part_1 = registry.get(uri, backend=cast("Any", partial_factory))
    part_2 = registry.get(uri, backend=cast("Any", partial_factory))
    assert part_1 is part_2
    assert part_1 is not fn_1


def test_get_factory_alias_with_backend_override_merges_config_and_kwargs(tmp_path: Path) -> None:
    """Overriding backend on a config alias invokes the override with merged alias config and kwargs."""
    registry = StorageRegistry()
    registry.register_alias("data", f"file://{tmp_path}", backend="local", base_opt="from-alias", shared="initial")

    overridden = registry.get(
        "data", backend=cast("Any", _custom_factory_function), shared="override", extra="from-call"
    )
    assert isinstance(overridden, _CustomStoreStub)
    assert overridden.uri == f"file://{tmp_path}"
    assert overridden.kwargs["base_opt"] == "from-alias"
    assert overridden.kwargs["shared"] == "override"
    assert overridden.kwargs["extra"] == "from-call"
    assert overridden.kwargs["via"] == "function"

    local_override = registry.get("data", backend=LocalStore)
    assert isinstance(local_override, LocalStore)


def test_get_unhashable_callable_override_bypasses_instance_cache(tmp_path: Path) -> None:
    """Unhashable callable factories skip _instances lookup and insertion completely."""
    registry = StorageRegistry()
    factory = _UnhashableFactory()
    uri = f"file://{tmp_path}"

    first = registry.get(uri, backend=cast("Any", factory))
    second = registry.get(uri, backend=cast("Any", factory))

    assert first is not second
    assert len(factory.calls) == 2
    assert registry._instances == {}

    registry.register_alias("cfg", uri, backend="local", tag="from-alias")
    alias_factory = _UnhashableFactory()
    alias_first = registry.get("cfg", backend=cast("Any", alias_factory), step=1)
    alias_second = registry.get("cfg", backend=cast("Any", alias_factory), step=1)

    assert alias_first is not alias_second
    assert len(alias_factory.calls) == 2
    assert alias_factory.calls[0][1] == {"tag": "from-alias", "step": 1}
    assert registry._instances == {}


def test_unregister_alias_removes_config_instance_and_cache(tmp_path: Path) -> None:
    """unregister_alias removes config/instance entries, evicts cached instances, and returns bool."""
    registry = StorageRegistry()
    registry.register_alias("cfg", f"file://{tmp_path}", backend="local")
    cached_bare = registry.get("cfg")
    _ = registry.get("cfg", extra="value")
    other_store = registry.get(f"file://{tmp_path}")

    assert registry.unregister_alias("cfg") is True
    assert registry.unregister_alias("cfg") is False
    assert registry.unregister_alias("missing") is False
    assert registry.is_alias_registered("cfg") is False
    assert registry.get(f"file://{tmp_path}") is other_store

    registry.register_alias("cfg", f"file://{tmp_path}", backend="local")
    fresh_cfg = registry.get("cfg")
    assert fresh_cfg is not cached_bare

    inst = _CustomStoreStub("custom://inst")
    registry.register_alias("inst", backend=cast("ObjectStoreProtocol", inst))
    assert registry.unregister_alias("inst") is True
    assert registry.unregister_alias("inst") is False
    assert registry.is_alias_registered("inst") is False
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("inst")


def test_unregister_alias_and_list_aliases_handle_duplicate_internal_keys(tmp_path: Path) -> None:
    """list_aliases deduplicates and unregister_alias removes from both maps without short-circuiting."""
    registry = StorageRegistry()
    inst = _CustomStoreStub("custom://dup")
    registry.register_alias("zebra", f"file://{tmp_path}", backend="local")
    registry.register_alias("alpha", backend=cast("ObjectStoreProtocol", inst))
    registry._alias_configs["dup"] = (cast("Any", _CustomStoreStub), "custom://dup", {})
    registry._alias_instances["dup"] = cast("ObjectStoreProtocol", inst)

    assert registry.list_aliases() == ["alpha", "dup", "zebra"]
    assert registry.unregister_alias("dup") is True
    assert "dup" not in registry._alias_configs
    assert "dup" not in registry._alias_instances
    assert registry.is_alias_registered("dup") is False
    assert registry.list_aliases() == ["alpha", "zebra"]


def test_lifecycle_matrix_preserves_instance_aliases_and_isolates_maps(tmp_path: Path) -> None:
    """Verify clear_cache, clear_instances, clear_aliases, clear_backends, and clear isolation matrix."""
    registry = StorageRegistry()
    inst = cast("ObjectStoreProtocol", _CustomStoreStub("custom://inst"))
    root_uri = f"file://{tmp_path}"

    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    registry.register_alias("cfg", root_uri, backend="local")
    registry.register_alias("inst", backend=inst)

    direct_cached = registry.get(root_uri)
    cfg_cached = registry.get("cfg")
    _ = registry.get(root_uri, backend="oci")

    registry.clear_cache("cfg")
    assert registry.is_alias_registered("cfg") is True
    assert registry.is_alias_registered("inst") is True
    assert registry.get("inst") is inst
    assert registry.get(root_uri) is direct_cached
    assert registry.get("cfg") is not cfg_cached
    assert registry.is_backend_registered("oci") is True

    cfg_cached_two = registry.get("cfg")
    registry.clear_cache()
    assert registry.is_alias_registered("cfg") is True
    assert registry.is_alias_registered("inst") is True
    assert registry.get("inst") is inst
    assert registry.get("cfg") is not cfg_cached_two
    assert registry.is_backend_registered("oci") is True

    cfg_cached_three = registry.get("cfg")
    registry.clear_instances()
    assert registry.is_alias_registered("cfg") is True
    assert registry.is_alias_registered("inst") is True
    assert registry.get("inst") is inst
    assert registry.get("cfg") is not cfg_cached_three
    assert registry.is_backend_registered("oci") is True

    direct_before_clear_aliases = registry.get(root_uri)
    registry.clear_aliases()
    assert registry.is_alias_registered("cfg") is False
    assert registry.is_alias_registered("inst") is False
    assert registry.list_aliases() == []
    assert registry.get(root_uri) is direct_before_clear_aliases
    assert registry.is_backend_registered("oci") is True
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias"):
        registry.get("inst")

    registry.register_alias("inst", backend=inst)
    registry.clear_backends()
    assert registry.is_backend_registered("oci") is False
    assert registry.get("inst") is inst

    registry.register_backend("oci", cast("Any", _CustomStoreStub))
    registry.register_alias("cfg", root_uri, backend="local")
    registry.clear()
    assert registry.list_aliases() == []
    assert registry.list_backends() == ["fsspec", "local", "obstore"]
    assert registry._alias_configs == {}
    assert registry._alias_instances == {}
    assert registry._instances == {}
    assert registry._backends == {}


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_instance_alias_scheme_isolation_guarded_by_scheme_is_none() -> None:
    """Scheme URIs bypass both _alias_instances and _alias_configs even with manual colon keys."""
    registry = StorageRegistry()
    store = cast("ObjectStoreProtocol", _CustomStoreStub("custom://mem", protocol="custom"))
    registry.register_alias("memory", backend=store)
    registry._alias_instances["memory:"] = store
    registry._alias_configs["memory:"] = (cast("Any", _CustomStoreStub), "custom://mem", {})

    assert registry.get("memory") is store
    by_scheme = registry.get("memory://bucket/key")
    assert by_scheme is not store
    assert by_scheme.protocol == "memory"


def test_instance_alias_does_not_match_partial_path_segment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An instance alias named 'db' does not intercept 'db_backup/file.sql'."""
    monkeypatch.chdir(tmp_path)
    store = cast("ObjectStoreProtocol", _CustomStoreStub("custom://db"))
    registry = StorageRegistry()
    registry.register_alias("db", backend=store)

    assert registry.get("db/queries.sql") is store
    not_alias = registry.get("db_backup/queries.sql")
    assert not_alias is not store
    assert isinstance(not_alias, LocalStore)
    assert not_alias.base_path.name == "db_backup"
