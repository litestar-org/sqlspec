"""Unit tests for execute_script behavior in driver base classes."""

import inspect
from enum import Enum
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg.core import default_statement_config as asyncpg_statement_config
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.oracledb.core import default_statement_config as oracle_statement_config
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite.core import default_statement_config
from sqlspec.config import LifecycleConfig
from sqlspec.core import SQL
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.observability import (
    ObservabilityConfig,
    ObservabilityRuntime,
    RedactionConfig,
    SamplingConfig,
    StatementEvent,
)
from tests.conftest import requires_interpreted


def test_as_script_embeds_parameters_statically() -> None:
    """as_script() should force static parameter embedding regardless of the adapter default."""
    assert default_statement_config.parameter_config.needs_static_script_compilation is False

    sql = SQL(
        "INSERT INTO t (a) VALUES (:a); INSERT INTO t (a) VALUES (:a)", a=5, statement_config=default_statement_config
    ).as_script()
    compiled_sql, params = sql.compile()

    assert params is None
    assert ":a" not in compiled_sql
    assert "5" in compiled_sql


def test_as_script_embeds_distinct_qmark_values_across_statements() -> None:
    """A flat script-wide payload embeds each qmark occurrence and leaves no driver parameters."""
    statement = SQL(
        "INSERT INTO t (name, value) VALUES (?, ?); "
        "INSERT INTO t (name, value) VALUES (?, ?); "
        "UPDATE t SET value = ? WHERE name = ?",
        ("embed-one", 10, "embed-two", 20, 99, "embed-two"),
        statement_config=default_statement_config,
    ).as_script()

    compiled_sql, parameters = statement.compile()

    assert parameters is None
    assert "?" not in compiled_sql
    assert "VALUES ('embed-one', 10)" in compiled_sql
    assert "VALUES ('embed-two', 20)" in compiled_sql
    assert "SET value = 99 WHERE name = 'embed-two'" in compiled_sql


@requires_interpreted
def test_sync_execute_script_tracks_all_successful_statements(sqlite_sync_driver) -> None:
    """Sync execute_script should report all statements as successful."""
    result = sqlite_sync_driver.execute_script("SELECT * FROM users; SELECT * FROM users; SELECT * FROM users;")
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


@requires_interpreted
def test_sync_execute_script_with_parameters_runs_every_statement(sqlite_sync_driver) -> None:
    """A parameterized multi-statement script embeds its params and lands every row."""
    result = sqlite_sync_driver.execute_script(
        "INSERT INTO users (name) VALUES (:n); INSERT INTO users (name) VALUES (:n)", n="scripted"
    )
    assert result.successful_statements == 2

    inserted = sqlite_sync_driver.execute("SELECT COUNT(*) FROM users WHERE name = :n", n="scripted").data
    assert inserted[0][0] == 2


@requires_interpreted
async def test_async_execute_script_with_parameters_runs_every_statement(aiosqlite_async_driver) -> None:
    """A parameterized multi-statement script embeds its params and lands every row on async drivers."""
    result = await aiosqlite_async_driver.execute_script(
        "INSERT INTO users (name) VALUES (:n); INSERT INTO users (name) VALUES (:n)", n="scripted"
    )
    assert result.successful_statements == 2

    inserted = (await aiosqlite_async_driver.execute("SELECT COUNT(*) FROM users WHERE name = :n", n="scripted")).data
    assert inserted[0][0] == 2


@requires_interpreted
async def test_async_execute_script_tracks_all_successful_statements(aiosqlite_async_driver) -> None:
    """Async execute_script should report all statements as successful."""
    result = await aiosqlite_async_driver.execute_script(
        "SELECT * FROM users; SELECT * FROM users; SELECT * FROM users;"
    )
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


def test_dispatch_script_no_double_prepare_dispatch_execute_script_prepares_parameters_once(
    sqlite_sync_driver: Any, monkeypatch: Any
) -> None:
    calls = 0
    original_prepare = sqlite_sync_driver.prepare_driver_parameters

    def counted_prepare_driver_parameters(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(sqlite_sync_driver, "prepare_driver_parameters", counted_prepare_driver_parameters)
    result = sqlite_sync_driver.execute_script("SELECT 1; SELECT 2; SELECT 3;")
    assert calls == 1
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


def test_dispatch_script_no_double_prepare_sub_statement_returns_processed_cache_direct_sql(
    sqlite_sync_driver: Any,
) -> None:
    sub_statement = sqlite_sync_driver._sub_statement("SELECT ?", (1,))
    assert isinstance(sub_statement, SQL)
    assert sub_statement.is_processed is True
    assert getattr(sub_statement, "_is_cache_direct") is True
    assert sub_statement.get_processed_state().execution_parameters == (1,)


def test_dispatch_execute_script_releases_sub_statement_when_statement_fails(
    sqlite_sync_driver: Any, monkeypatch: Any
) -> None:
    released: list[SQL] = []
    original_release = sqlite_sync_driver._release_pooled_statement

    def track_release(stmt: SQL) -> None:
        released.append(stmt)
        original_release(stmt)

    def failing_dispatch_execute(self: Any, cursor: Any, statement: SQL) -> Any:
        if "FAIL_HERE" in statement.raw_sql:
            raise RuntimeError("boom")
        return None

    monkeypatch.setattr(sqlite_sync_driver, "_release_pooled_statement", track_release)
    monkeypatch.setattr(type(sqlite_sync_driver), "dispatch_execute", failing_dispatch_execute)
    script_stmt = sqlite_sync_driver.prepare_statement(SQL("SELECT 1; SELECT 'FAIL_HERE';")).as_script()
    try:
        SyncDriverAdapterBase.dispatch_execute_script(sqlite_sync_driver, None, script_stmt)
    except RuntimeError:
        pass
    assert len(released) == 2


def test_sync_execute_script_redacts_literals_when_mask_parameters_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sync execute_script redacts embedded parameter literals across spans, lifecycle, and observers."""
    events: list[StatementEvent] = []
    starts: list[dict[str, Any]] = []
    span_sqls: list[str] = []

    orig_start_span = ObservabilityRuntime.start_query_span

    def spy_start_span(self: ObservabilityRuntime, sql: str, *args: Any, **kwargs: Any) -> Any:
        span_sqls.append(sql)
        return orig_start_span(self, sql, *args, **kwargs)

    monkeypatch.setattr(ObservabilityRuntime, "start_query_span", spy_start_span)

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "script_redact_sync.sqlite")},
        observability_config=ObservabilityConfig(
            statement_observers=(events.append,),
            redaction=RedactionConfig(mask_parameters=True, mask_literals=False),
            lifecycle=LifecycleConfig(on_query_start=[starts.append]),
        ),
    )
    secret = "top-secret-token-sync"
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE items (name TEXT)")
            events.clear()
            starts.clear()
            span_sqls.clear()
            session.execute_script(
                "INSERT INTO items (name) VALUES (:secret); SELECT name FROM items WHERE name = :secret", secret=secret
            )
    finally:
        config.close_pool()

    assert events
    assert all(secret not in event.sql for event in events)
    assert starts
    assert all(secret not in str(ctx.get("sql", "")) for ctx in starts)
    assert span_sqls
    assert all(secret not in sql_text for sql_text in span_sqls)


async def test_async_execute_script_redacts_literals_when_mask_parameters_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async execute_script redacts embedded parameter literals across spans, lifecycle, and observers."""
    events: list[StatementEvent] = []
    starts: list[dict[str, Any]] = []
    span_sqls: list[str] = []

    orig_start_span = ObservabilityRuntime.start_query_span

    def spy_start_span(self: ObservabilityRuntime, sql: str, *args: Any, **kwargs: Any) -> Any:
        span_sqls.append(sql)
        return orig_start_span(self, sql, *args, **kwargs)

    monkeypatch.setattr(ObservabilityRuntime, "start_query_span", spy_start_span)

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "script_redact_async.sqlite")},
        observability_config=ObservabilityConfig(
            statement_observers=(events.append,),
            redaction=RedactionConfig(mask_parameters=True, mask_literals=False),
            lifecycle=LifecycleConfig(on_query_start=[starts.append]),
        ),
    )
    secret = "top-secret-token-async"
    try:
        async with config.provide_session() as session:
            await session.execute("CREATE TABLE items (name TEXT)")
            events.clear()
            starts.clear()
            span_sqls.clear()
            await session.execute_script(
                "INSERT INTO items (name) VALUES (:secret); SELECT name FROM items WHERE name = :secret", secret=secret
            )
    finally:
        await config.close_pool()

    assert events
    assert all(secret not in event.sql for event in events)
    assert starts
    assert all(secret not in str(ctx.get("sql", "")) for ctx in starts)
    assert span_sqls
    assert all(secret not in sql_text for sql_text in span_sqls)


def test_sync_execute_with_observability_emits_error_event_under_zero_sample_rate(tmp_path: Path) -> None:
    """Sync driver emits an error statement event when force_sample_on_error is enabled."""
    events: list[StatementEvent] = []
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "err_obs_sync.sqlite")},
        observability_config=ObservabilityConfig(
            statement_observers=(events.append,), sampling=SamplingConfig(sample_rate=0.0, force_sample_on_error=True)
        ),
    )
    try:
        with config.provide_session() as session, pytest.raises(SQLSpecError):
            session.execute("SELECT * FROM missing_table_sync_err")
    finally:
        config.close_pool()

    assert len(events) == 1
    assert events[0].rows_affected is None
    assert events[0].duration_s >= 0.0
    assert events[0].sql == "SELECT * FROM missing_table_sync_err"


async def test_async_execute_with_observability_emits_error_event_under_zero_sample_rate(tmp_path: Path) -> None:
    """Async driver emits an error statement event when force_sample_on_error is enabled."""
    events: list[StatementEvent] = []
    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "err_obs_async.sqlite")},
        observability_config=ObservabilityConfig(
            statement_observers=(events.append,), sampling=SamplingConfig(sample_rate=0.0, force_sample_on_error=True)
        ),
    )
    try:
        async with config.provide_session() as session:
            with pytest.raises(SQLSpecError):
                await session.execute("SELECT * FROM missing_table_async_err")
    finally:
        await config.close_pool()

    assert len(events) == 1
    assert events[0].rows_affected is None
    assert events[0].duration_s >= 0.0
    assert events[0].sql == "SELECT * FROM missing_table_async_err"


class _Status(str, Enum):  # noqa: UP042
    ACTIVE = "active"


async def _run(result: Any) -> Any:
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=["sync", "async"])
async def script_driver(request: pytest.FixtureRequest, sqlite_sync_driver: Any, aiosqlite_async_driver: Any) -> Any:
    return sqlite_sync_driver if request.param == "sync" else aiosqlite_async_driver


async def _rows(driver: Any, sql: str, *parameters: Any) -> "list[Any]":
    result = await _run(driver.execute(sql, *parameters))
    return result.get_data()


@requires_interpreted
async def test_script_binds_named_parameters_per_statement(script_driver: Any) -> None:
    first = "x'; DROP TABLE t; --"
    second = "y\\z"
    await _run(
        script_driver.execute_script(
            "CREATE TABLE t(a TEXT, b TEXT); INSERT INTO t VALUES (:a, :b); INSERT INTO t VALUES (:b, :a)",
            a=first,
            b=second,
        )
    )

    assert await _rows(script_driver, "SELECT a, b FROM t ORDER BY rowid") == [
        {"a": first, "b": second},
        {"a": second, "b": first},
    ]


@requires_interpreted
async def test_script_binds_qmark_parameters_in_order(script_driver: Any) -> None:
    result = await _run(
        script_driver.execute_script(
            "CREATE TABLE t(v INT); INSERT INTO t VALUES (?); INSERT INTO t VALUES (?)", (1, 2)
        )
    )

    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert await _rows(script_driver, "SELECT v FROM t ORDER BY rowid") == [{"v": 1}, {"v": 2}]


@requires_interpreted
async def test_script_binds_str_enum_value(script_driver: Any) -> None:
    await _run(script_driver.execute_script("CREATE TABLE t(s TEXT); INSERT INTO t VALUES (:s)", s=_Status.ACTIVE))

    assert await _rows(script_driver, "SELECT s FROM t") == [{"s": "active"}]


@requires_interpreted
async def test_script_inlines_ddl_placeholders(script_driver: Any) -> None:
    await _run(script_driver.execute_script("CREATE TABLE t (a INT DEFAULT :d); INSERT INTO t DEFAULT VALUES", d=5))

    assert await _rows(script_driver, "SELECT a FROM t") == [{"a": 5}]


@requires_interpreted
async def test_script_inlines_pragma_placeholders(script_driver: Any) -> None:
    await _run(script_driver.execute_script("PRAGMA user_version = :v", v=7))

    assert await _rows(script_driver, "PRAGMA user_version") == [{"user_version": 7}]


def test_script_renumbers_numeric_parameters_on_duckdb() -> None:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script(
                "CREATE TABLE t(v INT); INSERT INTO t VALUES ($1); INSERT INTO t VALUES ($2)", (10, 20)
            )
            assert session.select("SELECT v FROM t ORDER BY v") == [{"v": 10}, {"v": 20}]
    finally:
        config.close_pool()


def test_script_inlines_set_placeholders_on_duckdb() -> None:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script("SET threads = :n", n=2)
            assert session.select_value("SELECT current_setting('threads')") == 2
    finally:
        config.close_pool()


def _asyncpg_script_driver(sqlite_sync_driver: Any) -> Any:
    return SqliteDriver(sqlite_sync_driver.connection, statement_config=asyncpg_statement_config)


@requires_interpreted
@pytest.mark.parametrize(
    ("script", "parameters"),
    [
        ("SET search_path = :s", {"s": "public"}),
        ("DO $$BEGIN PERFORM :x; END$$", {"x": 1}),
        ("NOTIFY chan, :p", {"p": "payload"}),
        ("COPY t FROM :f", {"f": "/tmp/file.csv"}),
        ("VACUUM (ANALYZE) :t", {"t": "users"}),
        ("BEGIN; SELECT :v", {"v": 1}),
    ],
    ids=["set", "do", "notify", "copy", "vacuum", "begin"],
)
def test_script_inlines_non_dml_statements(sqlite_sync_driver: Any, script: str, parameters: "dict[str, Any]") -> None:
    driver = _asyncpg_script_driver(sqlite_sync_driver)
    statement = SQL(script, statement_config=asyncpg_statement_config, **parameters).as_script()

    pieces = driver._bound_script_statements(statement)

    assert pieces is not None
    for piece in pieces:
        keyword = piece.raw_sql.split()[0].upper()
        _, execution_parameters = piece.compile()
        assert bool(execution_parameters) is (keyword == "SELECT")


@requires_interpreted
def test_script_binds_dml_statements_on_postgres_profile(sqlite_sync_driver: Any) -> None:
    driver = _asyncpg_script_driver(sqlite_sync_driver)
    statement = SQL(
        "INSERT INTO t VALUES (:v); SET search_path = :s", statement_config=asyncpg_statement_config, v=1, s="public"
    ).as_script()

    pieces = driver._bound_script_statements(statement)

    assert pieces is not None
    assert [piece.statement_config.parameter_config.needs_static_script_compilation for piece in pieces] == [
        False,
        True,
    ]
    assert list(pieces[0].compile()[1]) == [1]


@requires_interpreted
def test_script_oracle_begin_block_binds(sqlite_sync_driver: Any) -> None:
    driver = SqliteDriver(sqlite_sync_driver.connection, statement_config=oracle_statement_config)
    statement = SQL("BEGIN INSERT INTO t VALUES (:v); END;", statement_config=oracle_statement_config, v=1).as_script()

    pieces = driver._bound_script_statements(statement)

    assert pieces is not None
    assert [piece.statement_config.parameter_config.needs_static_script_compilation for piece in pieces] == [False]


@requires_interpreted
def test_script_missing_named_parameter_raises(sqlite_sync_driver: Any) -> None:
    with pytest.raises(SQLSpecError, match="which was not supplied"):
        sqlite_sync_driver.execute_script(
            "CREATE TABLE t(a); INSERT INTO t VALUES (:a); INSERT INTO t VALUES (:b)", a=1
        )


@requires_interpreted
def test_script_extra_positional_parameters_raise(sqlite_sync_driver: Any) -> None:
    with pytest.raises(SQLSpecError, match="more parameters"):
        sqlite_sync_driver.execute_script("CREATE TABLE t(a); INSERT INTO t VALUES (?)", (1, 2))


@requires_interpreted
def test_script_without_parameters_uses_existing_path(sqlite_sync_driver: Any) -> None:
    statement = SQL("SELECT 1; SELECT 2", statement_config=sqlite_sync_driver.statement_config).as_script()

    assert sqlite_sync_driver._bound_script_statements(statement) is None
    assert sqlite_sync_driver.execute_script("SELECT 1; SELECT 2").total_statements == 2


def test_script_dml_events_carry_bound_parameters(tmp_path: Path) -> None:
    """DML statements in a script reach observers with placeholders and bound values, not inlined text."""
    events: list[StatementEvent] = []
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "script_bound.sqlite")},
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE t (v TEXT)")
            events.clear()
            session.execute_script("INSERT INTO t VALUES (:v); CREATE INDEX i ON t (v)", v="needle")
    finally:
        config.close_pool()

    insert_events = [event for event in events if event.sql.startswith("INSERT") and "CREATE" not in event.sql]
    assert len(insert_events) == 1
    assert "needle" not in insert_events[0].sql
    assert insert_events[0].parameters == {"v": "needle"}
