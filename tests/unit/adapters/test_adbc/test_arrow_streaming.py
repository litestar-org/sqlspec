"""Unit tests for ADBC Arrow streaming paths."""

from importlib import import_module
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from adbc_driver_manager import AdbcStatusCode, DatabaseError

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc._typing import AdbcSessionContext
from sqlspec.adapters.adbc.core import get_statement_config
from sqlspec.adapters.adbc.driver import AdbcDriver, AdbcSelectStreamSource
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.adbc._typing import AdbcConnection
    from sqlspec.driver import SyncRowStream


_STORAGE_CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "partition_strategies": [],
}


class _AdbcStreamingReader:
    def __init__(self) -> None:
        table = pa.table({"x": [1, 2, 3]})
        self.schema = table.schema
        self._reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))
        self.close_count = 0
        self.close_error: BaseException | None = None

    def __iter__(self) -> "_AdbcStreamingReader":
        return self

    def __next__(self) -> pa.RecordBatch:
        return self._reader.read_next_batch()

    def read_next_batch(self) -> pa.RecordBatch:
        return next(self)

    def close(self) -> None:
        self.close_count += 1
        self._reader.close()
        if self.close_error is not None:
            raise self.close_error


class _AdbcStreamingCursor:
    def __init__(self) -> None:
        self.closed = False
        self.close_count = 0
        self.close_error: BaseException | None = None
        self.reader_obj = _AdbcStreamingReader()
        self.executed: list[tuple[str, object]] = []
        self.ingest_calls: list[tuple[str, object, str, dict[str, Any]]] = []

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))

    def fetch_record_batch(self) -> _AdbcStreamingReader:
        return self.reader_obj

    def fetch_arrow_table(self) -> pa.Table:
        msg = "reader formats should use fetch_record_batch(), not fetch_arrow_table()"
        raise AssertionError(msg)

    def adbc_ingest(self, table: str, source: object, *, mode: str, **kwargs: Any) -> int:
        self.ingest_calls.append((table, source, mode, kwargs))
        return 3

    def close(self) -> None:
        self.closed = True
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


class _AdbcStreamingConnection:
    def __init__(self) -> None:
        self.cursor_obj = _AdbcStreamingCursor()

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": "sqlite", "driver_name": "sqlite"}

    def cursor(self) -> _AdbcStreamingCursor:
        return self.cursor_obj


@pytest.mark.parametrize("kind", ["row", "reader"])
@pytest.mark.parametrize("progress", ["never", "partial", "exhausted"])
def test_session_closes_lazy_stream_owners_before_release(kind: str, progress: str) -> None:
    """All lazy streams taint their session and close actual native owners before release."""
    connection = _AdbcStreamingConnection()
    released: list[AdbcDriver] = []

    def release(_connection: Any, **kwargs: Any) -> None:
        expected_closes = 0 if kind == "row" and progress == "never" else 1
        assert connection.cursor_obj.close_count == expected_closes
        assert connection.cursor_obj.reader_obj.close_count == expected_closes
        assert kwargs["driver"].has_streams is True
        released.append(kwargs["driver"])

    context = AdbcSessionContext(
        acquire_connection=lambda: connection,
        release_connection=release,
        statement_config=get_statement_config("sqlite"),
        driver_features={},
        prepare_driver=lambda driver: driver,
    )
    stream: SyncRowStream[dict[str, Any]] | None = None
    with context as driver:
        if kind == "row":
            stream = driver.select_stream("SELECT 1 AS x", native_only=True, chunk_size=1)
            if progress == "partial":
                assert next(stream) == {"x": 1}
            elif progress == "exhausted":
                assert list(stream) == [{"x": 1}, {"x": 2}, {"x": 3}]
        else:
            reader = driver.select_to_arrow("SELECT 1 AS x", return_format="reader").data
            if progress == "partial":
                assert reader.read_next_batch().to_pydict() == {"x": [1, 2]}
            elif progress == "exhausted":
                assert reader.read_all().to_pydict() == {"x": [1, 2, 3]}
    assert released == [driver]
    driver.close_streams()
    if stream is not None:
        assert list(stream) == []


def test_lazy_fallback_stream_taints_session_before_execution() -> None:
    """The generic lazy fallback also prevents reuse before it opens native resources."""
    connection = _AdbcStreamingConnection()
    released: list[AdbcDriver] = []

    def release(_connection: Any, **kwargs: Any) -> None:
        assert kwargs["driver"].has_streams is True
        assert connection.cursor_obj.executed == []
        assert connection.cursor_obj.close_count == 0
        released.append(kwargs["driver"])

    context = AdbcSessionContext(
        acquire_connection=lambda: connection,
        release_connection=release,
        statement_config=get_statement_config("sqlite"),
        driver_features={},
        prepare_driver=lambda driver: driver,
    )
    with context as driver:
        stream = driver.select_stream("UPDATE items SET x = 1")
    assert released == [driver]
    stream.close()


@pytest.mark.parametrize("kind,consume", [("row", False), ("reader", False), ("reader", True)])
def test_pooled_lazy_stream_closes_native_owners_then_evicts_origin(
    monkeypatch: pytest.MonkeyPatch, kind: str, consume: bool
) -> None:
    """Stream cleanup precedes native connection disposal and the next session gets another clone."""

    class _PooledStreamingConnection(_AdbcStreamingConnection):
        def __init__(self) -> None:
            super().__init__()
            self.close_count = 0
            self.rollback_count = 0

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            expected = 0 if kind == "row" else 1
            assert self.cursor_obj.close_count == expected
            assert self.cursor_obj.reader_obj.close_count == expected
            self.close_count += 1

    seed, replacement = MagicMock(), MagicMock()
    lease = _PooledStreamingConnection()
    seed.adbc_clone.side_effect = [lease, replacement]
    monkeypatch.setattr(import_module("adbc_driver_postgresql.dbapi"), "connect", lambda **_kwargs: seed)
    config = AdbcConfig(
        connection_config={"driver_name": "postgres"},
        pool_config={"max_size": 1, "timeout": 0},
        driver_features={"enable_pgvector": False, "enable_paradedb": False, "enable_pg_textsearch": False},
    )
    with config.provide_session() as session:
        assert session.connection is lease
        if kind == "row":
            session.select_stream("SELECT 1 AS x", native_only=True)
        else:
            reader = session.select_to_arrow("SELECT 1 AS x", return_format="reader").data
            if consume:
                assert reader.read_all().to_pydict() == {"x": [1, 2, 3]}
    assert lease.close_count == 1
    assert lease.rollback_count == 1
    with config.provide_session() as session:
        assert session.connection is replacement
    config.close_pool()
    seed.close.assert_called_once_with()


@pytest.mark.parametrize("stage", ["exit", "body", "setup"])
def test_session_stream_cleanup_attempts_all_owners_and_preserves_primary(stage: str) -> None:
    """Cleanup attempts every native reader/cursor and release, preserving setup/body errors."""
    cursors = [_AdbcStreamingCursor(), _AdbcStreamingCursor()]
    available = iter(cursors)

    class _MultipleStreamingConnection(_AdbcStreamingConnection):
        def cursor(self) -> _AdbcStreamingCursor:
            return next(available)

    connection = _MultipleStreamingConnection()
    primary = KeyboardInterrupt("primary interruption")
    first_cleanup = RuntimeError("first reader cleanup failed")
    cursors[0].reader_obj.close_error = first_cleanup
    cursors[0].close_error = RuntimeError("cursor cleanup failed")
    cursors[1].reader_obj.close_error = SystemExit("second reader cleanup failed")
    released: list[AdbcDriver] = []
    drivers: list[AdbcDriver] = []
    readers: list[Any] = []

    def open_readers(driver: AdbcDriver) -> None:
        drivers.append(driver)
        readers.extend(driver.select_to_arrow("SELECT 1 AS x", return_format="reader").data for _ in range(2))

    def prepare(driver: AdbcDriver) -> AdbcDriver:
        if stage == "setup":
            open_readers(driver)
            raise primary
        return driver

    def release(_connection: Any, **kwargs: Any) -> None:
        for cursor in cursors:
            assert cursor.close_count == 1
            assert cursor.reader_obj.close_count == 1
        released.append(kwargs["driver"])
        raise RuntimeError("release cleanup failed")

    context = AdbcSessionContext(
        acquire_connection=lambda: connection,
        release_connection=release,
        statement_config=get_statement_config("sqlite"),
        driver_features={},
        prepare_driver=prepare,
    )
    expected = RuntimeError if stage == "exit" else KeyboardInterrupt
    with pytest.raises(expected) as caught, context as driver:
        open_readers(driver)
        if stage == "body":
            raise primary
    assert caught.value is (first_cleanup if stage == "exit" else primary)
    assert released == drivers
    context.__exit__(None, None, None)
    drivers[0].close_streams()
    assert [cursor.close_count for cursor in cursors] == [1, 1]


def test_eager_arrow_batches_close_owners_without_tainting_session() -> None:
    """Eager batches release their native resources immediately and remain reusable."""
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="sqlite")
    result = driver.select_to_arrow("SELECT 1 AS x", return_format="batches")
    assert [batch.to_pydict() for batch in result.data] == [{"x": [1, 2]}, {"x": [3]}]
    assert driver.has_streams is False
    assert connection.cursor_obj.close_count == 1
    assert connection.cursor_obj.reader_obj.close_count == 1
    driver.close_streams()
    assert connection.cursor_obj.close_count == 1


def test_materialized_arrow_table_does_not_taint_session() -> None:
    """Materialized tables close their cursor and do not require stream eviction."""

    class _MaterializedCursor(_AdbcStreamingCursor):
        def fetch_arrow_table(self) -> pa.Table:
            return pa.table({"x": [1, 2, 3]})

    connection = _AdbcStreamingConnection()
    connection.cursor_obj = _MaterializedCursor()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="sqlite")
    result = driver.select_to_arrow("SELECT 1 AS x", return_format="table")
    assert result.data.to_pydict() == {"x": [1, 2, 3]}
    assert driver.has_streams is False
    assert connection.cursor_obj.close_count == 1
    driver.close_streams()
    assert connection.cursor_obj.close_count == 1


def test_select_to_arrow_reader_uses_fetch_record_batch_and_defers_cursor_close() -> None:
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="sqlite")

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="reader")

    assert result.rows_affected == -1
    assert isinstance(result.data, pa.RecordBatchReader)
    assert connection.cursor_obj.closed is False
    assert result.data.read_all().to_pydict() == {"x": [1, 2, 3]}
    assert connection.cursor_obj.closed is True


def test_select_stream_uses_record_batch_reader_without_eager_table_materialization() -> None:
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="sqlite")

    with driver.select_stream("SELECT 1 AS x", native_only=True, chunk_size=2) as stream:
        rows = list(stream)

    assert rows == [{"x": 1}, {"x": 2}, {"x": 3}]
    assert connection.cursor_obj.executed == [("SELECT 1 AS x", [])]
    assert connection.cursor_obj.closed is True


def test_load_from_arrow_passes_record_batch_reader_to_adbc_ingest() -> None:
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(
        cast("AdbcConnection", connection),
        dialect="sqlite",
        driver_features={"storage_capabilities": _STORAGE_CAPABILITIES},
    )
    table = pa.table({"x": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    job = driver.load_from_arrow("target_table", reader)

    target, source, mode, kwargs = connection.cursor_obj.ingest_calls[0]
    assert target == "target_table"
    assert source is reader
    assert mode == "create_append"
    assert kwargs == {}
    assert job.telemetry["rows_processed"] == 3
    assert job.telemetry["destination"] == "target_table"


@pytest.mark.parametrize("return_format", ["table", "reader"])
@pytest.mark.parametrize("primary", [RuntimeError("postgres failure"), KeyboardInterrupt("query interrupted")])
def test_select_to_arrow_postgres_rolls_back_on_error(return_format: Any, primary: BaseException) -> None:
    class _PostgresErrorCursor:
        def __init__(self, conn: "_PostgresErrorConnection") -> None:
            self.connection = conn
            self.closed = False

        def execute(self, sql: str, parameters: object = None) -> None:
            raise primary

        def close(self) -> None:
            self.closed = True

    class _PostgresErrorConnection:
        def __init__(self) -> None:
            self.rolled_back = False
            self.cursor_obj = _PostgresErrorCursor(self)

        def adbc_get_info(self) -> dict[str, str]:
            return {"vendor_name": "PostgreSQL", "driver_name": "adbc_driver_postgresql"}

        def rollback(self) -> None:
            self.rolled_back = True

        def cursor(self) -> _PostgresErrorCursor:
            return self.cursor_obj

    connection = _PostgresErrorConnection()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="postgres")

    with pytest.raises(type(primary)) as caught:
        driver.select_to_arrow("SELECT 1", return_format=return_format)
    assert caught.value is primary
    assert connection.rolled_back is True
    assert connection.cursor_obj.closed is True


def test_select_stream_start_closes_cursor_and_rolls_back_postgres_on_failure() -> None:
    """AdbcSelectStreamSource.start() must close cursor and roll back Postgres when execute raises."""

    class _FailingCursor:
        def __init__(self, conn: "_PostgresConn") -> None:
            self.connection = conn
            self.closed = False

        def execute(self, sql: str, parameters: object = None) -> None:
            raise DatabaseError("query failed", status_code=AdbcStatusCode.UNKNOWN)

        def close(self) -> None:
            self.closed = True

    class _PostgresConn:
        def __init__(self) -> None:
            self.rolled_back = False
            self.last_cursor: _FailingCursor | None = None

        def adbc_get_info(self) -> dict[str, str]:
            return {"vendor_name": "PostgreSQL", "driver_name": "adbc_driver_postgresql"}

        def rollback(self) -> None:
            self.rolled_back = True

        def cursor(self) -> _FailingCursor:
            self.last_cursor = _FailingCursor(self)
            return self.last_cursor

    conn = _PostgresConn()
    driver = AdbcDriver(cast("AdbcConnection", conn), dialect="postgres")
    source = AdbcSelectStreamSource(driver, "SELECT 1", [])

    with pytest.raises(SQLSpecError):
        source.start()

    assert conn.last_cursor is not None
    assert conn.last_cursor.closed is True
    assert conn.rolled_back is True


def test_load_from_arrow_splits_schema_qualified_table_name() -> None:
    """load_from_arrow should pass db_schema_name and catalog_name to adbc_ingest for qualified targets."""
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(
        cast("AdbcConnection", connection),
        dialect="postgres",
        driver_features={"storage_capabilities": _STORAGE_CAPABILITIES},
    )
    table = pa.table({"x": [1, 2]})

    driver.load_from_arrow("analytics.events", table)
    target, _source, _mode, kwargs = connection.cursor_obj.ingest_calls[0]
    assert target == "events"
    assert kwargs == {"db_schema_name": "analytics"}

    driver.load_from_arrow("warehouse.analytics.events", table)
    target2, _source2, _mode2, kwargs2 = connection.cursor_obj.ingest_calls[1]
    assert target2 == "events"
    assert kwargs2 == {"catalog_name": "warehouse", "db_schema_name": "analytics"}


def test_adbc_driver_commit_and_rollback_map_errors_and_reset_transaction_state() -> None:
    """commit and rollback must map AdbcError to SQLSpecError and reset _transaction_active in finally."""

    class _FailingTxConnection:
        def adbc_get_info(self) -> dict[str, str]:
            return {"vendor_name": "sqlite", "driver_name": "sqlite"}

        def commit(self) -> None:
            raise DatabaseError("commit failed", status_code=AdbcStatusCode.UNKNOWN)

        def rollback(self) -> None:
            raise DatabaseError("rollback failed", status_code=AdbcStatusCode.UNKNOWN)

    driver = AdbcDriver(cast("AdbcConnection", _FailingTxConnection()), dialect="sqlite")
    driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(SQLSpecError):
        driver.commit()
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]

    driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(SQLSpecError):
        driver.rollback()
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
