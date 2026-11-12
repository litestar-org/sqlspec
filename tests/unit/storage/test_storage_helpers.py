"""Unit tests for driver storage helper utilities."""

import contextlib
import importlib
import inspect
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Literal, Self, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.core import SQL, StatementConfig, build_arrow_result_from_reader, create_arrow_result
from sqlspec.driver._async import AsyncDataDictionaryBase, AsyncDriverAdapterBase
from sqlspec.driver._common import (
    AsyncExceptionHandler,
    CommonDriverAttributesMixin,
    ExecutionResult,
    SyncExceptionHandler,
)
from sqlspec.driver._stream import AsyncRowStream, SyncRowStream
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase
from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage.pipeline import (
    StorageBridgeJob,
    StorageTelemetry,
    SyncStoragePipeline,
    ingest_telemetry_from_batches,
)
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.arrow_helpers import arrow_batch_to_rows, arrow_reader_with_deferred_close, coerce_arrow_batches

pytestmark = pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")

HOOK_ROUTED_ADAPTERS: list[tuple[str, str]] = [
    ("sqlspec.adapters.adbc.driver", "AdbcDriver"),
    ("sqlspec.adapters.arrow_odbc.driver", "ArrowOdbcDriver"),
    ("sqlspec.adapters.spanner.driver", "SpannerSyncDriver"),
    ("sqlspec.adapters.spanner.driver", "SpannerAsyncDriver"),
    ("sqlspec.adapters.oracledb.driver", "OracleSyncDriver"),
    ("sqlspec.adapters.oracledb.driver", "OracleAsyncDriver"),
    ("sqlspec.adapters.bigquery.driver", "BigQueryDriver"),
    ("sqlspec.adapters.duckdb.driver", "DuckDBDriver"),
    ("sqlspec.adapters.sqlite.driver", "SqliteDriver"),
    ("sqlspec.adapters.aiosqlite.driver", "AiosqliteDriver"),
    ("sqlspec.adapters.pymysql.driver", "PyMysqlDriver"),
    ("sqlspec.adapters.aiomysql.driver", "AiomysqlDriver"),
    ("sqlspec.adapters.asyncmy.driver", "AsyncmyDriver"),
    ("sqlspec.adapters.mysqlconnector.driver", "MysqlConnectorSyncDriver"),
    ("sqlspec.adapters.mysqlconnector.driver", "MysqlConnectorAsyncDriver"),
    ("sqlspec.adapters.asyncpg.driver", "AsyncpgDriver"),
    ("sqlspec.adapters.psycopg.driver", "PsycopgSyncDriver"),
    ("sqlspec.adapters.psycopg.driver", "PsycopgAsyncDriver"),
    ("sqlspec.adapters.psqlpy.driver", "PsqlpyDriver"),
    ("sqlspec.adapters.pymssql.driver", "PymssqlDriver"),
    ("sqlspec.adapters.sqlanywhere.driver", "SqlanywhereDriver"),
    ("sqlspec.adapters.sybase.driver", "SybaseDriver"),
    ("sqlspec.adapters.db2.driver", "Db2SyncDriver"),
    ("sqlspec.adapters.db2.driver", "Db2AsyncDriver"),
    ("sqlspec.adapters.mssql_python.driver", "MssqlPythonDriver"),
]


class _NoopSyncExcHandler:
    pending_exception: Exception | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        return False


class _NoopAsyncExcHandler:
    pending_exception: Exception | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        return False


class _RecordingSyncSource:
    def __init__(self, chunks: list[list[dict[str, Any]]], *, column_types: dict[str, str] | None = None) -> None:
        self._chunks = list(chunks)
        self.column_types = column_types
        self.started = False
        self.closed = False
        self.close_calls = 0

    def start(self) -> None:
        self.started = True

    def fetch_chunk(self) -> list[dict[str, Any]]:
        if not self._chunks:
            return []
        return self._chunks.pop(0)

    def close(self, error: bool = False) -> None:
        _ = error
        self.closed = True
        self.close_calls += 1


class _RecordingAsyncSource:
    def __init__(self, chunks: list[list[dict[str, Any]]], *, column_types: dict[str, str] | None = None) -> None:
        self._chunks = list(chunks)
        self.column_types = column_types
        self.started = False
        self.closed = False
        self.close_calls = 0

    async def start(self) -> None:
        self.started = True

    async def fetch_chunk(self) -> list[dict[str, Any]]:
        if not self._chunks:
            return []
        return self._chunks.pop(0)

    async def close(self, error: bool = False) -> None:
        _ = error
        self.closed = True
        self.close_calls += 1


class _SyncDriverDouble(SyncDriverAdapterBase):
    dialect = "sqlite"

    def __init__(self, chunks: list[list[dict[str, Any]]], *, column_types: dict[str, str] | None = None) -> None:
        super().__init__(connection=object(), statement_config=StatementConfig(dialect="sqlite"))
        self.source = _RecordingSyncSource(chunks, column_types=column_types)
        self.select_to_arrow_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.last_chunk_size: int | None = None

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:
        return cast("SyncDataDictionaryBase", MagicMock())

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = (cursor, statement)
        return self.create_execution_result(cursor, selected_data=[], column_names=["a"], data_row_count=0)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = (cursor, statement)
        return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def with_cursor(self, connection: Any) -> Any:
        return contextlib.nullcontext(connection)

    def handle_database_exceptions(self) -> SyncExceptionHandler:
        return _NoopSyncExcHandler()

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> SyncRowStream[dict[str, Any]] | None:
        _ = statement
        self.last_chunk_size = chunk_size
        return SyncRowStream(self.source)

    def select_to_arrow(self, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
        self.select_to_arrow_calls.append(((statement, *parameters), dict(kwargs)))
        import pyarrow as pa

        empty = pa.schema([("a", pa.int64())]).empty_table()
        return create_arrow_result(statement=SQL("SELECT 1"), data=empty, rows_affected=0)


class _AsyncDriverDouble(AsyncDriverAdapterBase):
    dialect = "sqlite"

    def __init__(self, chunks: list[list[dict[str, Any]]], *, column_types: dict[str, str] | None = None) -> None:
        super().__init__(connection=object(), statement_config=StatementConfig(dialect="sqlite"))
        self.source = _RecordingAsyncSource(chunks, column_types=column_types)
        self.select_to_arrow_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.last_chunk_size: int | None = None

    @property
    def data_dictionary(self) -> AsyncDataDictionaryBase:
        return cast("AsyncDataDictionaryBase", MagicMock())

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = (cursor, statement)
        return self.create_execution_result(cursor, selected_data=[], column_names=["a"], data_row_count=0)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        _ = (cursor, statement)
        return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    def with_cursor(self, connection: Any) -> Any:
        return contextlib.nullcontext(connection)

    def handle_database_exceptions(self) -> AsyncExceptionHandler:
        return _NoopAsyncExcHandler()

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> AsyncRowStream[dict[str, Any]] | None:
        _ = statement
        self.last_chunk_size = chunk_size
        return AsyncRowStream(self.source)

    async def select_to_arrow(self, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
        self.select_to_arrow_calls.append(((statement, *parameters), dict(kwargs)))
        import pyarrow as pa

        empty = pa.schema([("a", pa.int64())]).empty_table()
        return create_arrow_result(statement=SQL("SELECT 1"), data=empty, rows_affected=0)


def test_ingest_telemetry_from_batches_populates_fields() -> None:
    """ingest_telemetry_from_batches builds a StorageTelemetry dictionary with stage metrics."""
    telemetry = ingest_telemetry_from_batches(
        1024,
        65536,
        duration_s=1.25,
        format_label="parquet",
        read_duration_s=0.25,
        encode_duration_s=0.35,
        write_duration_s=0.50,
        commit_duration_s=0.15,
        shards_processed=4,
        shards_skipped=1,
        wal_bytes=4096,
    )
    assert telemetry["rows_processed"] == 1024
    assert telemetry["bytes_processed"] == 65536
    assert telemetry["duration_s"] == 1.25
    assert telemetry["format"] == "parquet"
    assert telemetry.get("read_duration_s") == 0.25
    assert telemetry.get("encode_duration_s") == 0.35
    assert telemetry.get("write_duration_s") == 0.50
    assert telemetry.get("commit_duration_s") == 0.15
    assert telemetry.get("shards_processed") == 4
    assert telemetry.get("shards_skipped") == 1
    assert telemetry.get("wal_bytes") == 4096


def test_driver_common_exposes_streaming_arrow_batch_helpers() -> None:
    """CommonDriverAttributesMixin exposes _coerce_arrow_batches and _arrow_batch_to_rows."""
    import pyarrow as pa

    table = pa.table({"k": [10, 20, 30], "v": ["x", "y", "z"]})
    cols, batches = CommonDriverAttributesMixin._coerce_arrow_batches(table, batch_size=2)
    batch_list = list(batches)
    assert cols == ["k", "v"]
    assert len(batch_list) == 2
    assert CommonDriverAttributesMixin._arrow_batch_to_rows(batch_list[0]) == [(10, "x"), (20, "y")]
    assert coerce_arrow_batches is not None
    assert arrow_batch_to_rows is not None


def test_stream_exposes_source_column_types() -> None:
    """SyncRowStream and AsyncRowStream expose the underlying source column_types property."""
    sync_stream: SyncRowStream[dict[str, Any]] = SyncRowStream(
        _RecordingSyncSource([[{"a": 1}]], column_types={"a": "int32"})
    )
    assert sync_stream.column_types == {"a": "int32"}

    async_stream: AsyncRowStream[dict[str, Any]] = AsyncRowStream(
        _RecordingAsyncSource([[{"a": 1}]], column_types={"a": "int32"})
    )
    assert async_stream.column_types == {"a": "int32"}


def test_sync_select_for_storage_row_stream_path() -> None:
    """_select_for_storage streams chunks into a RecordBatchReader without calling select_to_arrow."""
    import pyarrow as pa

    driver = _SyncDriverDouble([[{"a": 1}, {"a": 2}], [{"a": 3}, {"a": 4}], [{"a": 5}]])
    result = driver._select_for_storage("SELECT a FROM t", batch_size=2)
    assert isinstance(result.data, pa.RecordBatchReader)
    assert driver.select_to_arrow_calls == []
    assert driver.last_chunk_size == 2
    assert result.metadata["streamed"] is True
    assert "read_ahead_columns" not in result.metadata
    assert result.data.read_all().to_pylist() == [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]
    assert driver.source.closed is True


def test_sync_select_for_storage_native_and_explicit_and_empty() -> None:
    """_select_for_storage routes native_arrow_export, explicit return_format, native_only, and empty streams."""

    class _NativeSyncDouble(_SyncDriverDouble):
        native_arrow_export: ClassVar[bool] = True

    native = _NativeSyncDouble([[{"a": 1}]])
    native._select_for_storage("SELECT a FROM t")
    assert len(native.select_to_arrow_calls) == 1
    assert native.select_to_arrow_calls[0][1]["return_format"] == "reader"

    explicit = _SyncDriverDouble([[{"a": 1}]])
    explicit._select_for_storage("SELECT a FROM t", return_format="table")
    assert len(explicit.select_to_arrow_calls) == 1
    assert explicit.select_to_arrow_calls[0][1]["return_format"] == "table"

    native_only = _SyncDriverDouble([[{"a": 1}]])
    native_only._select_for_storage("SELECT a FROM t", native_only=True)
    assert len(native_only.select_to_arrow_calls) == 1
    assert native_only.select_to_arrow_calls[0][1]["native_only"] is True

    empty = _SyncDriverDouble([])
    empty._select_for_storage("SELECT a FROM t")
    assert len(empty.select_to_arrow_calls) == 1


def test_sync_select_for_storage_forwards_widening_flag() -> None:
    """_select_for_storage forwards export_integers_may_widen to arrow_batches_from_rows."""
    import pyarrow as pa

    class _WideningSyncDouble(_SyncDriverDouble):
        export_integers_may_widen: ClassVar[bool] = True

    driver = _WideningSyncDouble([[{"a": 1}], [{"a": 1.5}]])
    result = driver._select_for_storage("SELECT a FROM t", batch_size=1)
    assert isinstance(result.data, pa.RecordBatchReader)
    table = result.data.read_all()
    assert table.schema.field("a").type == pa.float64()
    assert table.column("a").to_pylist() == [1.0, 1.5]


def test_sync_write_storage_result_records_read_ahead_and_omits_when_clean(tmp_path: Path) -> None:
    """_write_storage_result records read_ahead_columns in telemetry extra only when read-ahead occurred."""
    decimal_driver = _SyncDriverDouble([[{"n": Decimal("1.25")}], [{"n": Decimal("2.50")}]])
    decimal_result = decimal_driver._select_for_storage("SELECT n FROM t", batch_size=1)
    assert decimal_result.metadata["read_ahead_columns"] == ("n",)
    decimal_telemetry = decimal_driver._write_storage_result(
        decimal_result, tmp_path / "dec.parquet", format_hint="parquet"
    )
    assert decimal_telemetry["extra"]["read_ahead_columns"] == ("n",)

    int_driver = _SyncDriverDouble([[{"n": 1}], [{"n": 2}]])
    int_result = int_driver._select_for_storage("SELECT n FROM t", batch_size=1)
    assert "read_ahead_columns" not in int_result.metadata
    int_telemetry = int_driver._write_storage_result(int_result, tmp_path / "int.parquet", format_hint="parquet")
    assert "read_ahead_columns" not in int_telemetry.get("extra", {})


def test_sync_write_storage_result_closes_source_on_sink_failure() -> None:
    """_write_storage_result closes the stream source before re-raising when the storage sink fails mid-write."""
    driver = _SyncDriverDouble([[{"a": 1}], [{"a": 2}], [{"a": 3}]])
    result = driver._select_for_storage("SELECT a FROM t", batch_size=1)
    close_observed_on_error = False

    class _FailingSecondBatchPipeline(SyncStoragePipeline):
        def write_arrow_batches(self, destination: Any, batches: Iterator[Any], **kwargs: Any) -> StorageTelemetry:
            _ = (destination, kwargs)
            first = next(batches)
            assert first.num_rows == 1
            assert driver.source.closed is False
            raise OSError("sink disk full")

    try:
        driver._write_storage_result(
            result, "out.parquet", format_hint="parquet", pipeline=_FailingSecondBatchPipeline()
        )
    except OSError:
        close_observed_on_error = driver.source.closed

    assert close_observed_on_error is True


async def test_async_select_for_storage_parity(tmp_path: Path) -> None:
    """AsyncDriverAdapterBase._select_for_storage and _write_storage_result mirror sync parity."""
    driver = _AsyncDriverDouble([[{"n": Decimal("1.5")}], [{"n": Decimal("2.5")}]])
    result = await driver._select_for_storage("SELECT n FROM t", batch_size=1)
    assert hasattr(result.data, "__aiter__")
    assert driver.select_to_arrow_calls == []
    assert result.metadata["read_ahead_columns"] == ("n",)
    telemetry = await driver._write_storage_result(result, tmp_path / "async_dec.parquet", format_hint="parquet")
    assert telemetry["extra"]["read_ahead_columns"] == ("n",)
    assert driver.source.closed is True

    class _NativeAsyncDouble(_AsyncDriverDouble):
        native_arrow_export: ClassVar[bool] = True

    native = _NativeAsyncDouble([[{"a": 1}]])
    await native._select_for_storage("SELECT a FROM t")
    assert native.select_to_arrow_calls[0][1]["return_format"] == "reader"

    explicit = _AsyncDriverDouble([[{"a": 1}]])
    await explicit._select_for_storage("SELECT a FROM t", return_format="table")
    assert explicit.select_to_arrow_calls[0][1]["return_format"] == "table"

    empty = _AsyncDriverDouble([])
    await empty._select_for_storage("SELECT a FROM t")
    assert len(empty.select_to_arrow_calls) == 1


@pytest.mark.parametrize(("adapter_module", "class_name"), HOOK_ROUTED_ADAPTERS)
async def test_select_to_storage_routes_through_hook(
    adapter_module: str, class_name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Client-side select_to_storage routes query execution through _select_for_storage."""
    import pyarrow as pa

    module = importlib.import_module(adapter_module)
    cls = getattr(module, class_name)
    driver = cls(connection=MagicMock(), driver_features={"storage_capabilities": {"arrow_export_enabled": True}})
    calls: list[Any] = []
    dummy_result = create_arrow_result(statement=SQL("SELECT 1"), data=pa.table({"x": [1]}), rows_affected=1)
    dummy_telemetry: StorageTelemetry = {
        "destination": str(tmp_path / "out.parquet"),
        "bytes_processed": 10,
        "rows_processed": 1,
        "duration_s": 0.01,
        "format": "parquet",
        "backend": "local",
    }

    if inspect.iscoroutinefunction(cls.select_to_storage):

        async def fake_select_async(self: Any, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
            _ = (self, parameters, kwargs)
            calls.append(statement)
            return dummy_result

        async def fake_write_async(self: Any, result: Any, destination: Any, **kwargs: Any) -> StorageTelemetry:
            _ = (self, result, destination, kwargs)
            return cast("StorageTelemetry", dict(dummy_telemetry))

        monkeypatch.setattr(cls, "_select_for_storage", fake_select_async)
        monkeypatch.setattr(cls, "_write_storage_result", fake_write_async)
        await driver.select_to_storage("SELECT 1", tmp_path / "out.parquet")
    else:

        def fake_select_sync(self: Any, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
            _ = (self, parameters, kwargs)
            calls.append(statement)
            return dummy_result

        def fake_write_sync(self: Any, result: Any, destination: Any, **kwargs: Any) -> StorageTelemetry:
            _ = (self, result, destination, kwargs)
            return cast("StorageTelemetry", dict(dummy_telemetry))

        monkeypatch.setattr(cls, "_select_for_storage", fake_select_sync)
        monkeypatch.setattr(cls, "_write_storage_result", fake_write_sync)
        driver.select_to_storage("SELECT 1", tmp_path / "out.parquet")

    assert calls == ["SELECT 1"]


def test_sqlite_drivers_declare_export_integers_may_widen() -> None:
    """SqliteDriver and AiosqliteDriver enable integer-widening read-ahead for storage exports."""
    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
    from sqlspec.adapters.pymysql.driver import PyMysqlDriver
    from sqlspec.adapters.sqlite.driver import SqliteDriver

    assert SqliteDriver.export_integers_may_widen is True
    assert AiosqliteDriver.export_integers_may_widen is True
    assert PyMysqlDriver.export_integers_may_widen is False


def test_db2_and_mssql_python_native_arrow_export_flags() -> None:
    """MssqlPythonDriver uses native reader export while Db2 drivers use row streams."""
    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver
    from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

    assert MssqlPythonDriver.native_arrow_export is True
    assert Db2SyncDriver.native_arrow_export is False
    assert Db2AsyncDriver.native_arrow_export is False


async def test_async_select_to_storage_preflight_failure_closes_opened_row_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Async select_to_storage closes the opened AsyncRowStream before propagating pre-iteration StorageCapabilityError."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

    class _ExportAsyncDouble(_AsyncDriverDouble):
        async def select_to_storage(
            self, statement: Any, destination: Any, /, *parameters: Any, format_hint: Any = None, **kwargs: Any
        ) -> StorageBridgeJob:
            result = await self._select_for_storage(statement, *parameters, **kwargs)
            assert self.source.started is True
            assert self.source.closed is False
            assert result.metadata["row_stream_batches"] is True
            assert callable(result.metadata["stream_aclose"])
            telemetry = await self._write_storage_result(result, destination, format_hint=format_hint)
            return self._storage_job(telemetry, None)

    driver = _ExportAsyncDouble([[{"a": 1}], [{"a": 2}]], column_types={"a": "int64"})
    with pytest.raises(StorageCapabilityError):
        await driver.select_to_storage("SELECT a FROM t", tmp_path / "bad.json", format_hint="json", batch_size=1)
    assert driver.source.closed is True
    assert driver.source.close_calls == 1

    config = AiosqliteConfig()
    spec = SQLSpec()
    spec.add_config(config)
    captured_streams: list[AsyncRowStream[Any]] = []
    orig_dispatch = AiosqliteDriver.dispatch_select_stream

    def _capturing_dispatch(
        self: AiosqliteDriver, statement: SQL, chunk_size: int
    ) -> AsyncRowStream[dict[str, Any]] | None:
        stream = orig_dispatch(self, statement, chunk_size)
        if stream is not None:
            captured_streams.append(stream)
        return stream

    monkeypatch.setattr(AiosqliteDriver, "dispatch_select_stream", _capturing_dispatch)
    try:
        async with spec.provide_session(config) as session:
            await session.execute("CREATE TABLE export_items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            await session.execute_many(
                "INSERT INTO export_items (id, label) VALUES (?, ?)", [(1, "alpha"), (2, "beta"), (3, "gamma")]
            )
            await session.commit()
            with pytest.raises(StorageCapabilityError):
                await session.select_to_storage(
                    "SELECT label FROM export_items ORDER BY id",
                    tmp_path / "aiosqlite_bad.json",
                    format_hint="json",
                    batch_size=1,
                )
            assert len(captured_streams) == 1
            assert captured_streams[0]._closed is True
            rows = await session.select("SELECT COUNT(*) AS cnt FROM export_items")
            assert rows == [{"cnt": 3}]
    finally:
        await config.close_pool()


def test_sync_native_select_to_storage_preflight_failure_closes_deferred_reader(tmp_path: Path) -> None:
    """Sync native_arrow_export select_to_storage closes deferred-close RecordBatchReader before StorageCapabilityError."""
    import pyarrow as pa

    close_calls: list[str] = []

    class _NativeReaderSyncDouble(_SyncDriverDouble):
        native_arrow_export: ClassVar[bool] = True

        def select_to_arrow(self, statement: Any, /, *parameters: Any, **kwargs: Any) -> Any:
            self.select_to_arrow_calls.append(((statement, *parameters), dict(kwargs)))
            schema = pa.schema([("a", pa.int64())])
            batches = [
                pa.RecordBatch.from_pydict({"a": [1]}, schema=schema),
                pa.RecordBatch.from_pydict({"a": [2]}, schema=schema),
            ]
            reader = arrow_reader_with_deferred_close(
                pa.RecordBatchReader.from_batches(schema, batches), lambda: close_calls.append("cursor_closed")
            )
            return build_arrow_result_from_reader(
                SQL("SELECT a FROM t"), reader, return_format=kwargs.get("return_format", "table")
            )

        def select_to_storage(
            self, statement: Any, destination: Any, /, *parameters: Any, format_hint: Any = None, **kwargs: Any
        ) -> StorageBridgeJob:
            result = self._select_for_storage(statement, *parameters, **kwargs)
            assert close_calls == []
            telemetry = self._write_storage_result(result, destination, format_hint=format_hint)
            return self._storage_job(telemetry, None)

    driver = _NativeReaderSyncDouble([[{"a": 1}]])
    with pytest.raises(StorageCapabilityError):
        driver.select_to_storage("SELECT a FROM t", tmp_path / "native_bad.json", format_hint="json")
    assert close_calls == ["cursor_closed"]

    row_driver = _SyncDriverDouble([[{"a": 1}], [{"a": 2}]], column_types={"a": "int64"})
    row_result = row_driver._select_for_storage("SELECT a FROM t", batch_size=1)
    assert row_driver.source.started is True
    assert row_driver.source.closed is False
    with pytest.raises(StorageCapabilityError):
        row_driver._write_storage_result(row_result, tmp_path / "row_bad.json", format_hint="json")
    assert row_driver.source.closed is True
    assert row_driver.source.close_calls == 1
