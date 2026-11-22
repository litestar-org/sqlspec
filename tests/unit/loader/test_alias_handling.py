from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.loader import SQLFileLoader
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.registry import StorageBackendFactory, StorageRegistry


class _InMemorySQLStore:
    """In-memory object store test double satisfying the instance alias contract."""

    protocol: str = "inmem"
    backend_type: ClassVar[str] = "inmem"

    def __init__(
        self,
        files: dict[str, str],
        *,
        uri: str = "inmem://queries",
        missing_error: type[FileNotFoundError] | type[KeyError] = FileNotFoundError,
    ) -> None:
        self.uri = uri
        self.files = dict(files)
        self.missing_error = missing_error
        self.read_calls: list[tuple[str, str]] = []

    def resolve_uri(self, path: str | Path) -> str:
        return f"{self.uri.rstrip('/')}/{path}"

    def read_bytes_sync(self, path: str | Path, **_kwargs: Any) -> bytes:
        return self.read_text_sync(path).encode("utf-8")

    def write_bytes_sync(self, path: str | Path, data: bytes, *, overwrite: bool = True, **_kwargs: Any) -> None:
        _ = overwrite
        self.files[str(path)] = data.decode("utf-8")

    def exists_sync(self, path: str | Path, **_kwargs: Any) -> bool:
        return str(path) in self.files

    def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **_kwargs: Any) -> str:
        key = str(path)
        self.read_calls.append((key, encoding))
        if key not in self.files:
            raise self.missing_error(key)
        return self.files[key]


def test_load_file_with_alias_is_not_double_dir_stripped(tmp_path: Path) -> None:
    """Test loading a file through an alias to verify path handling."""
    sql_file = tmp_path / "my_query.sql"
    sql_file.write_text("-- name: query1\nSELECT 1;")

    registry = StorageRegistry()
    # Register an alias to the parent directory
    registry.register_alias("my_store", f"file://{tmp_path}")

    loader = SQLFileLoader(storage_registry=registry)
    # The storage backend for 'my_store/my_query.sql' will be scoped to 'file://tmp_path'
    loader.load_sql("my_store/my_query.sql")

    assert loader.has_query("query1")


def test_load_file_from_alias_nested(tmp_path: Path) -> None:
    """Test loading a nested file through an alias."""
    nested_dir = tmp_path / "nested"
    nested_dir.mkdir()
    sql_file = nested_dir / "my_query.sql"
    sql_file.write_text("-- name: query2\nSELECT 2;")

    registry = StorageRegistry()
    registry.register_alias("my_store", f"file://{tmp_path}")

    loader = SQLFileLoader(storage_registry=registry)
    # 'my_store/nested/my_query.sql'
    loader.load_sql("my_store/nested/my_query.sql")

    assert loader.has_query("query2")


@pytest.mark.parametrize(
    ("load_path", "expected_key", "expected_query", "expected_sql"),
    [
        ("sql_store/users.sql", "users.sql", "get_users", "SELECT id, name FROM users;"),
        ("sql_store/nested/orders.sql", "nested/orders.sql", "get_orders", "SELECT id, total FROM orders;"),
    ],
    ids=["flat", "nested"],
)
def test_load_sql_from_instance_backed_alias(
    load_path: str, expected_key: str, expected_query: str, expected_sql: str
) -> None:
    """SQLFileLoader loads flat and nested paths from a pre-configured instance alias."""
    store = _InMemorySQLStore({
        "users.sql": "-- name: get_users\nSELECT id, name FROM users;",
        "nested/orders.sql": "-- name: get_orders\nSELECT id, total FROM orders;",
    })
    registered_store = cast("ObjectStoreProtocol", store)
    registry = StorageRegistry()
    registry.register_alias("sql_store", backend=registered_store)

    loader = SQLFileLoader(storage_registry=registry)
    loader.load_sql(load_path)

    assert loader.has_query(expected_query)
    assert loader.get_sql(expected_query).sql.strip() == expected_sql
    assert store.read_calls == [(expected_key, "utf-8")]
    assert registry.get("sql_store") is registered_store


@pytest.mark.parametrize(
    "clear_action", ["clear_cache_all", "clear_cache_alias", "clear_instances"], ids=["all", "alias", "instances"]
)
def test_load_sql_instance_alias_survives_cache_clear(clear_action: str) -> None:
    """Pre-configured instance aliases remain attached to the identical object across cache clears."""
    store = _InMemorySQLStore({
        "users.sql": "-- name: get_users\nSELECT 1;",
        "nested/orders.sql": "-- name: get_orders\nSELECT 2;",
    })
    registered_store = cast("ObjectStoreProtocol", store)
    registry = StorageRegistry()
    registry.register_alias("sql_store", backend=registered_store)

    first_loader = SQLFileLoader(storage_registry=registry)
    first_loader.load_sql("sql_store/users.sql")

    if clear_action == "clear_cache_all":
        registry.clear_cache()
    elif clear_action == "clear_cache_alias":
        registry.clear_cache("sql_store")
    else:
        registry.clear_instances()

    assert registry.is_alias_registered("sql_store")
    assert registry.get("sql_store") is registered_store

    second_loader = SQLFileLoader(storage_registry=registry)
    second_loader.load_sql("sql_store/users.sql", "sql_store/nested/orders.sql")

    assert second_loader.has_query("get_users")
    assert second_loader.has_query("get_orders")
    assert store.read_calls == [("users.sql", "utf-8"), ("users.sql", "utf-8"), ("nested/orders.sql", "utf-8")]


@pytest.mark.parametrize(
    ("load_path", "expected_key", "expected_query"),
    [
        ("sql_named/users.sql", "users.sql", "get_users"),
        ("sql_named/nested/orders.sql", "nested/orders.sql", "get_orders"),
    ],
    ids=["flat", "nested"],
)
def test_load_sql_from_named_custom_backend_alias(load_path: str, expected_key: str, expected_query: str) -> None:
    """SQLFileLoader loads SQL via a named custom backend registered with StorageRegistry."""
    factory_calls: list[tuple[str, dict[str, Any]]] = []
    created_stores: list[_InMemorySQLStore] = []

    def make_store(uri: str, **kwargs: Any) -> ObjectStoreProtocol:
        factory_calls.append((uri, dict(kwargs)))
        store = _InMemorySQLStore(
            {"users.sql": "-- name: get_users\nSELECT 1;", "nested/orders.sql": "-- name: get_orders\nSELECT 2;"},
            uri=uri,
        )
        created_stores.append(store)
        return cast("ObjectStoreProtocol", store)

    registry = StorageRegistry()
    registry.register_backend("inmem", cast("StorageBackendFactory", make_store))
    registry.register_alias("sql_named", "inmem://queries", backend="inmem")

    loader = SQLFileLoader(storage_registry=registry)
    loader.load_sql(load_path)

    assert factory_calls == [("inmem://queries", {})]
    assert len(created_stores) == 1
    assert loader.has_query(expected_query)
    assert created_stores[0].read_calls == [(expected_key, "utf-8")]


@pytest.mark.parametrize("error_cls", [FileNotFoundError, KeyError], ids=["file_not_found", "key_error"])
def test_load_sql_missing_key_on_instance_alias_raises_not_found(
    error_cls: type[FileNotFoundError] | type[KeyError],
) -> None:
    """Missing query paths on a custom store translate FileNotFoundError and KeyError into SQLFileNotFoundError."""
    store = _InMemorySQLStore({}, missing_error=error_cls)
    registry = StorageRegistry()
    registry.register_alias("sql_store", backend=cast("ObjectStoreProtocol", store))

    loader = SQLFileLoader(storage_registry=registry)
    with pytest.raises(SQLFileNotFoundError) as exc_info:
        loader.load_sql("sql_store/missing.sql")

    assert isinstance(exc_info.value.__cause__, error_cls)
    assert store.read_calls == [("missing.sql", "utf-8")]
