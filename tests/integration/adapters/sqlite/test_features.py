"""Shared sync and async SQLite driver behavior that remains adapter-specific."""

import inspect
import math
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.aiosqlite import core as aiosqlite_core
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite import core as sqlite_core
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("sqlite")

SQLiteFamilyDriver = SqliteDriver | AiosqliteDriver


async def _invoke(method: "Callable[..., Any]", *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _method(driver: "SQLiteFamilyDriver", name: str) -> "Callable[..., Any]":
    return cast("Callable[..., Any]", getattr(driver, name))


def _prefix(driver: "SQLiteFamilyDriver") -> str:
    return "async" if isinstance(driver, AiosqliteDriver) else "sync"


@pytest.fixture(params=("sqlite", "aiosqlite"), ids=("sync", "async"))
async def sqlite_family_driver(request: "pytest.FixtureRequest") -> "AsyncGenerator[SQLiteFamilyDriver, None]":
    """Provide equivalent fresh sync and async SQLite sessions."""
    if request.param == "sqlite":
        config = SqliteConfig(connection_config={"database": ":memory:"})
        try:
            with config.provide_session() as driver:
                driver.execute_script("""
                    CREATE TABLE test_table (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        value INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                yield driver
        finally:
            config.close_pool()
        return

    config_async = AiosqliteConfig()
    try:
        async with config_async.provide_session() as driver:
            await driver.execute_script("""
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            yield driver
    finally:
        if config_async.connection_instance:
            await config_async.close_pool()
        config_async.connection_instance = None


@pytest.mark.parametrize("rowid", [-7, 0])
async def test_insert_preserves_integer_lastrowid(sqlite_family_driver: "SQLiteFamilyDriver", rowid: int) -> None:
    prefix = _prefix(sqlite_family_driver)
    await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        f"CREATE TABLE {prefix}_lastrowid_values (id INTEGER PRIMARY KEY, value TEXT)",
    )
    result = await _invoke(
        _method(sqlite_family_driver, "execute"),
        f"INSERT INTO {prefix}_lastrowid_values (id, value) VALUES (?, ?)",
        (rowid, "value"),
    )
    assert result.last_inserted_id == rowid


async def test_update_and_delete_ignore_sticky_lastrowid(sqlite_family_driver: "SQLiteFamilyDriver") -> None:
    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_lastrowid_sticky"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"), f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"
    )
    inserted = await _invoke(
        _method(sqlite_family_driver, "execute"), f"INSERT INTO {table} (value) VALUES (?)", ("before",)
    )
    updated = await _invoke(
        _method(sqlite_family_driver, "execute"), f"UPDATE {table} SET value = ? WHERE id = ?", ("after", 1)
    )
    deleted = await _invoke(_method(sqlite_family_driver, "execute"), f"DELETE FROM {table} WHERE id = ?", (1,))
    assert isinstance(inserted.last_inserted_id, int)
    assert updated.last_inserted_id is None
    assert deleted.last_inserted_id is None


async def test_repeated_insert_cache_hit_preserves_lastrowid(
    sqlite_family_driver: "SQLiteFamilyDriver", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_lastrowid_cached"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
    )
    statement = f"INSERT INTO {table} (value) VALUES (?)"
    first = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("first",))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated INSERT should use the cached fast path")

    monkeypatch.setattr(type(sqlite_family_driver), "dispatch_execute", fail_dispatch)
    second = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("second",))
    assert isinstance(first.last_inserted_id, int)
    assert isinstance(second.last_inserted_id, int)
    assert second.last_inserted_id != first.last_inserted_id


async def test_insert_returning_preserves_rows_and_cached_lastrowid(
    sqlite_family_driver: "SQLiteFamilyDriver", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_lastrowid_returning"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
    )
    statement = f"INSERT INTO {table} (value) VALUES (?) RETURNING id, value"
    first = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("first",))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated INSERT RETURNING should use the cached fast path")

    monkeypatch.setattr(type(sqlite_family_driver), "dispatch_execute", fail_dispatch)
    second = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("second",))
    assert first.get_data() == [{"id": 1, "value": "first"}]
    assert first.last_inserted_id == 1
    assert second.get_data() == [{"id": 2, "value": "second"}]
    assert second.last_inserted_id == 2


async def test_without_rowid_insert_never_reuses_stale_lastrowid(
    sqlite_family_driver: "SQLiteFamilyDriver", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    prefix = _prefix(sqlite_family_driver)
    rowid_table = f"{prefix}_rowid_source"
    without_rowid_table = f"{prefix}_without_rowid"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        f"""
            CREATE TABLE {rowid_table} (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT);
            CREATE TABLE {without_rowid_table} (id TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
        """,
    )
    prior = await _invoke(
        _method(sqlite_family_driver, "execute"), f"INSERT INTO {rowid_table} (value) VALUES (?)", ("prior",)
    )
    statement = f"INSERT INTO {without_rowid_table} (id, value) VALUES (?, ?)"
    first = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("first", "value"))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated WITHOUT ROWID INSERT should use the cached fast path")

    monkeypatch.setattr(type(sqlite_family_driver), "dispatch_execute", fail_dispatch)
    second = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("second", "value"))
    assert isinstance(prior.last_inserted_id, int)
    assert first.last_inserted_id is None
    assert second.last_inserted_id is None


async def test_repeated_cached_insert_reuses_rowid_eligibility_lookup(
    sqlite_family_driver: "SQLiteFamilyDriver", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    rowid_core = aiosqlite_core if isinstance(sqlite_family_driver, AiosqliteDriver) else sqlite_core

    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_rowid_lookup"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"), f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"
    )
    lookup_count = 0
    original_lookup = rowid_core._target_supports_rowid

    def counted_lookup(connection: object, target: "tuple[str | None, str]") -> bool:
        nonlocal lookup_count
        lookup_count += 1
        return cast("Callable[[object, tuple[str | None, str]], bool]", original_lookup)(connection, target)

    monkeypatch.setattr(rowid_core, "_target_supports_rowid", counted_lookup)
    statement = f"INSERT INTO {table} (value) VALUES (?)"
    first = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("first",))
    second = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("second",))
    assert isinstance(first.last_inserted_id, int)
    assert isinstance(second.last_inserted_id, int)
    assert lookup_count == 1


async def test_schema_change_invalidates_rowid_eligibility_cache(sqlite_family_driver: "SQLiteFamilyDriver") -> None:
    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_rowid_replaced"
    statement = f"INSERT INTO {table} (id, value) VALUES (?, ?)"
    await _invoke(_method(sqlite_family_driver, "execute"), f"CREATE TABLE {table} (id TEXT PRIMARY KEY, value TEXT)")
    first = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("first", "value"))
    await _invoke(_method(sqlite_family_driver, "execute"), f"DROP TABLE {table}")
    await _invoke(
        _method(sqlite_family_driver, "execute"),
        f"CREATE TABLE {table} (id TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID",
    )
    second = await _invoke(_method(sqlite_family_driver, "execute"), statement, ("second", "value"))
    assert isinstance(first.last_inserted_id, int)
    assert second.last_inserted_id is None


async def test_native_data_types(sqlite_family_driver: "SQLiteFamilyDriver") -> None:
    prefix = _prefix(sqlite_family_driver)
    table = f"{prefix}_data_types"
    await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        f"""
            CREATE TABLE {table} (
                id INTEGER PRIMARY KEY,
                text_col TEXT,
                integer_col INTEGER,
                real_col REAL,
                blob_col BLOB,
                null_col TEXT
            )
        """,
    )
    insert_result = await _invoke(
        _method(sqlite_family_driver, "execute"),
        f"INSERT INTO {table} (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        ("text_value", 42, math.pi, b"binary_data", None),
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1
    select_result = await _invoke(
        _method(sqlite_family_driver, "execute"),
        f"SELECT text_col, integer_col, real_col, blob_col, null_col FROM {table}",
    )
    assert select_result.get_data() == [
        {"text_col": "text_value", "integer_col": 42, "real_col": math.pi, "blob_col": b"binary_data", "null_col": None}
    ]


@requires_interpreted
async def test_statement_stack_continue_on_error(sqlite_family_driver: "SQLiteFamilyDriver") -> None:
    prefix = _prefix(sqlite_family_driver)
    await _invoke(_method(sqlite_family_driver, "execute"), "DELETE FROM test_table")
    await _invoke(_method(sqlite_family_driver, "commit"))
    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, f"{prefix}-initial", 5))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, f"{prefix}-duplicate", 15))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, f"{prefix}-final", 25))
    )
    results = await _invoke(_method(sqlite_family_driver, "execute_stack"), stack, continue_on_error=True)
    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1
    verify = await _invoke(_method(sqlite_family_driver, "execute"), "SELECT COUNT(*) AS total FROM test_table")
    assert verify.get_data()[0]["total"] == 2


async def test_schema_operations(sqlite_family_driver: "SQLiteFamilyDriver") -> None:
    create_result = await _invoke(
        _method(sqlite_family_driver, "execute_script"),
        """
            CREATE TABLE schema_test (
                id INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """,
    )
    assert create_result.operation_type == "SCRIPT"
    insert_result = await _invoke(
        _method(sqlite_family_driver, "execute"),
        "INSERT INTO schema_test (description) VALUES (?)",
        ("test description",),
    )
    assert insert_result.rows_affected == 1
    pragma_result = await _invoke(_method(sqlite_family_driver, "execute"), "PRAGMA table_info(schema_test)")
    assert len(pragma_result.get_data()) == 3
    drop_result = await _invoke(_method(sqlite_family_driver, "execute_script"), "DROP TABLE schema_test")
    assert drop_result.operation_type == "SCRIPT"


@pytest.mark.parametrize(
    ("lock_method", "lock_kwargs", "unsupported_clause"),
    (
        ("for_update", {}, "FOR UPDATE"),
        ("for_share", {}, "FOR SHARE"),
        ("for_update", {"skip_locked": True}, "FOR UPDATE"),
    ),
)
async def test_unsupported_lock_clause_is_stripped(
    sqlite_family_driver: "SQLiteFamilyDriver", lock_method: str, lock_kwargs: "dict[str, Any]", unsupported_clause: str
) -> None:
    prefix = _prefix(sqlite_family_driver)
    name = f"{prefix}-{lock_method}"
    await _invoke(
        _method(sqlite_family_driver, "execute"), "INSERT INTO test_table (name, value) VALUES (?, ?)", (name, 100)
    )
    query = sql.select("*").from_("test_table").where_eq("name", name)
    query = cast("Any", getattr(query, lock_method))(**lock_kwargs)
    statement = query.build()
    assert unsupported_clause not in statement.sql
    result = await _invoke(_method(sqlite_family_driver, "execute"), query)
    assert result.get_data()[0]["name"] == name
