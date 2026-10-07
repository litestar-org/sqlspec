"""Unit tests for Spanner DDL routing and write-session transaction renewal."""

import inspect
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

import sqlspec.adapters.spanner.driver as spanner_driver_module
from sqlspec.adapters.spanner._typing import SpannerAsyncSessionContext, SpannerSyncSessionContext
from sqlspec.adapters.spanner.core import default_statement_config, is_ddl_statement
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
from sqlspec.exceptions import SQLConversionError


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
        "create index idx_t_a on t (a)",
        "ALTER TABLE t ADD COLUMN b STRING(MAX)",
        "DROP TABLE IF EXISTS t",
        "CREATE SEARCH INDEX si ON t (tokens)",
        "GRANT SELECT ON TABLE t TO ROLE reader",
        "REVOKE SELECT ON TABLE t FROM ROLE reader",
        "RENAME TABLE a TO b",
        "ANALYZE",
        "-- name: create-t\nCREATE TABLE t (id INT64) PRIMARY KEY (id)",
        "/* migration */ ALTER DATABASE db SET OPTIONS (version_retention_period = '7d')",
    ],
)
def test_is_ddl_statement_detects_spanner_ddl(sql: str) -> None:
    """Verify schema statements are detected by their leading keyword, after comments."""
    assert is_ddl_statement(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "INSERT INTO t (id) VALUES (1)",
        "UPDATE t SET a = 1 WHERE TRUE",
        "DELETE FROM t WHERE TRUE",
        "-- CREATE TABLE in a comment\nSELECT 1",
        "",
    ],
)
def test_is_ddl_statement_ignores_queries_and_dml(sql: str) -> None:
    """Verify queries, DML and comment-only mentions of DDL keywords are not DDL."""
    assert not is_ddl_statement(sql)


def _build_driver(mode: str, events: "list[tuple[str, Any]]", *, writable: bool = True) -> Any:
    def record_ddl(statements: "list[str]") -> Any:
        events.append(("ddl", list(statements)))
        if mode == "sync":
            return SimpleNamespace(result=MagicMock(return_value=None))
        return SimpleNamespace(result=AsyncMock(return_value=None))

    def record_update(sql: str, **_: Any) -> int:
        events.append(("dml", sql))
        return 1

    database = SimpleNamespace(update_ddl=AsyncMock(side_effect=record_ddl) if mode == "async" else record_ddl)
    connection = MagicMock()
    connection._session = SimpleNamespace(_database=database)
    if writable:
        connection.execute_update = AsyncMock(side_effect=record_update) if mode == "async" else record_update
    else:
        del connection.execute_update
        del connection.batch_update
    driver_cls = SpannerAsyncDriver if mode == "async" else SpannerSyncDriver
    return driver_cls(connection=cast("Any", connection))


class _EmptyAsyncResult:
    """Async result set double with no rows."""

    def __aiter__(self) -> "_EmptyAsyncResult":
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@pytest.fixture(params=["sync", "async"])
def mode(request: pytest.FixtureRequest) -> str:
    return cast("str", request.param)


async def test_execute_routes_ddl_to_update_ddl(mode: str) -> None:
    """Verify a DDL statement runs through ``update_ddl`` instead of the transaction."""
    events: list[tuple[str, Any]] = []
    driver = _build_driver(mode, events)

    result = await _resolve(driver.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)"))

    assert events == [("ddl", ["CREATE TABLE t (id INT64) PRIMARY KEY (id)"])]
    assert result.rows_affected == 0


@pytest.mark.parametrize(
    "run",
    [
        pytest.param(lambda driver: driver.execute("DROP TABLE IF EXISTS t"), id="execute"),
        pytest.param(lambda driver: driver.execute_script("SELECT 1; DROP TABLE IF EXISTS t"), id="script"),
    ],
)
async def test_read_only_snapshot_rejects_ddl(mode: str, run: Any) -> None:
    """Verify read sessions refuse schema changes, as they refuse DML."""
    events: list[tuple[str, Any]] = []
    driver = _build_driver(mode, events, writable=False)
    driver.connection.execute_sql = (
        AsyncMock(return_value=_EmptyAsyncResult()) if mode == "async" else MagicMock(return_value=[])
    )

    with pytest.raises(SQLConversionError, match="Cannot execute DDL in a read-only Snapshot context"):
        await _resolve(run(driver))

    assert events == []


async def test_execute_script_batches_consecutive_ddl_in_order(mode: str) -> None:
    """Verify scripts batch adjacent DDL and apply it before the DML that follows."""
    events: list[tuple[str, Any]] = []
    driver = _build_driver(mode, events)
    script = """
        CREATE TABLE a (id INT64) PRIMARY KEY (id);
        CREATE INDEX idx_a_id ON a (id);
        INSERT INTO a (id) VALUES (1);
        ALTER TABLE a ADD COLUMN note STRING(MAX)
    """

    result = await _resolve(driver.execute_script(script))

    assert [kind for kind, _ in events] == ["ddl", "dml", "ddl"]
    assert events[0][1] == ["CREATE TABLE a (id INT64) PRIMARY KEY (id)", "CREATE INDEX idx_a_id ON a (id)"]
    assert events[2][1] == ["ALTER TABLE a ADD COLUMN note STRING(MAX)"]
    assert result.total_statements == 4


def _maybe_async(mode: str, value: Any) -> Any:
    if mode == "sync":
        return value

    async def _value() -> Any:
        return value

    return _value()


class _FakeTransaction:
    """Read-write transaction double whose session hands out fresh transactions."""

    def __init__(self, session: Any, events: "list[tuple[str, Any]]", mode: str, *, begun: bool) -> None:
        self._session = session
        self._events = events
        self._mode = mode
        self.committed: Any = None
        self.rolled_back = False
        self._transaction_id = b"txn" if begun else None
        self._mutations: list[Any] = []

    def commit(self) -> Any:
        self._events.append(("commit", self))
        self.committed = True
        return _maybe_async(self._mode, None)

    def rollback(self) -> Any:
        self._events.append(("rollback", self))
        self.rolled_back = True
        return _maybe_async(self._mode, None)

    def execute_update(self, sql: str, **_: Any) -> Any:
        if self._transaction_id is None:
            self._transaction_id = b"txn"
        self._events.append(("dml", (sql, self)))
        return _maybe_async(self._mode, 1)


def _transaction_driver(
    mode: str, events: "list[tuple[str, Any]]", monkeypatch: pytest.MonkeyPatch, *, begun: bool
) -> "tuple[Any, _FakeTransaction]":
    def record_ddl(statements: "list[str]") -> Any:
        events.append(("ddl", list(statements)))
        operation = SimpleNamespace(result=lambda timeout=None: _maybe_async(mode, None))
        return _maybe_async(mode, operation)

    session = SimpleNamespace(_database=SimpleNamespace(update_ddl=record_ddl))
    session.transaction = lambda: _FakeTransaction(session, events, mode, begun=False)
    monkeypatch.setattr(spanner_driver_module, "SpannerTransaction", _FakeTransaction)
    monkeypatch.setattr(spanner_driver_module, "SpannerAsyncTransaction", _FakeTransaction)
    transaction = _FakeTransaction(session, events, mode, begun=begun)
    driver_cls = SpannerAsyncDriver if mode == "async" else SpannerSyncDriver
    return driver_cls(connection=cast("Any", transaction)), transaction


async def test_ddl_commits_begun_transaction_and_renews_it(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify DDL commits begun work first and leaves the session on a fresh transaction."""
    events: list[tuple[str, Any]] = []
    driver, transaction = _transaction_driver(mode, events, monkeypatch, begun=True)

    await _resolve(driver.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)"))

    assert events == [("commit", transaction), ("ddl", ["CREATE TABLE t (id INT64) PRIMARY KEY (id)"])]
    assert driver.connection is not transaction
    assert driver.connection._transaction_id is None


async def test_ddl_keeps_unbegun_transaction(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify DDL leaves a transaction that has not begun untouched."""
    events: list[tuple[str, Any]] = []
    driver, transaction = _transaction_driver(mode, events, monkeypatch, begun=False)

    await _resolve(driver.execute("DROP TABLE IF EXISTS t"))

    assert events == [("ddl", ["DROP TABLE IF EXISTS t"])]
    assert driver.connection is transaction


async def test_managed_transaction_driver_never_commits_for_ddl(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify DDL inside ``run_in_transaction`` leaves commit and retry to the SDK."""
    events: list[tuple[str, Any]] = []
    session_driver, transaction = _transaction_driver(mode, events, monkeypatch, begun=True)
    driver = session_driver._transaction_driver(transaction)

    await _resolve(driver.execute("CREATE INDEX idx_t_id ON t (id)"))

    assert events == [("ddl", ["CREATE INDEX idx_t_id ON t (id)"])]
    assert driver.connection is transaction


async def test_script_dml_after_ddl_runs_on_renewed_transaction(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify script DML before DDL commits, and DML after it uses the renewed transaction."""
    events: list[tuple[str, Any]] = []
    driver, transaction = _transaction_driver(mode, events, monkeypatch, begun=False)

    await _resolve(
        driver.execute_script(
            "INSERT INTO a (id) VALUES (1); CREATE INDEX idx_a_id ON a (id); INSERT INTO a (id) VALUES (2)"
        )
    )

    assert [kind for kind, _ in events] == ["dml", "commit", "ddl", "dml"]
    assert events[0][1] == ("INSERT INTO a (id) VALUES (1)", transaction)
    renewed = driver.connection
    assert renewed is not transaction
    assert events[3][1] == ("INSERT INTO a (id) VALUES (2)", renewed)


async def test_session_context_releases_the_current_driver_connection(mode: str) -> None:
    """Verify session release completes the transaction the driver holds at exit."""
    released: list[Any] = []
    original = object()
    renewed = object()

    def acquire() -> Any:
        return _maybe_async(mode, original)

    def release(connection: Any, **_: Any) -> Any:
        released.append(connection)
        return _maybe_async(mode, None)

    options: dict[str, Any] = {
        "acquire_connection": acquire,
        "release_connection": release,
        "statement_config": default_statement_config,
        "driver_features": {},
        "prepare_driver": lambda driver: driver,
    }
    if mode == "async":
        async with SpannerAsyncSessionContext(**options) as async_driver:
            async_driver.connection = cast("Any", renewed)
    else:
        with SpannerSyncSessionContext(**options) as sync_driver:
            sync_driver.connection = cast("Any", renewed)

    assert released == [renewed]
