"""Asyncmy load_from_arrow ingest paths."""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "partition_strategies": [],
}


class _FakeCursor:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.rowcount = 0
        self.payload: bytes | None = None
        self.payload_path: Path | None = None
        self.failure: BaseException | None = None

    async def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)
        if sql.startswith("LOAD DATA"):
            self.payload_path = Path(_args[0][0])
            self.payload = self.payload_path.read_bytes()
            if self.failure is not None:
                raise self.failure

    async def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, [tuple(row) for row in params]))

    async def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def _read_query_result(self, unbuffered: bool = False) -> None:
        pass

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return self._cursor


def _make_driver(connection: _FakeConnection) -> AsyncmyDriver:
    return AsyncmyDriver(connection=cast("Any", connection), driver_features={"storage_capabilities": _CAPS})


async def test_load_from_arrow_uses_executemany() -> None:
    conn = _FakeConnection()
    driver = _make_driver(conn)

    job = await driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.execute_calls == []
    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]


async def test_load_from_arrow_overwrite_truncates_first() -> None:
    conn = _FakeConnection()
    driver = _make_driver(conn)

    await driver.load_from_arrow("orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls[0] == "TRUNCATE TABLE `orders`"
    assert conn._cursor.executemany_calls[0][0].startswith("INSERT INTO")


async def test_load_from_storage_reads_parquet_and_delegates(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), parquet_path)
    conn = _FakeConnection()
    driver = _make_driver(conn)

    job = await driver.load_from_storage("orders", str(parquet_path), file_format="parquet")

    assert job.telemetry["rows_processed"] == 2
    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]


async def test_local_infile_payload_roundtrip_and_cleanup() -> None:
    conn = _FakeConnection()
    driver = AsyncmyDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )
    job = await driver.load_from_arrow(
        "order%`table", pa.table({"id": [1, 2], "text.with%tick`": ["é\t\n\r\\\x00\x1a", None], "flag": [True, False]})
    )
    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.executemany_calls == []
    assert conn._cursor.payload == "1\té\\t\\n\\r\\\\\\0\\Z\t1\n2\t\\N\t0\n".encode()
    assert "LOCAL INFILE %s" in conn._cursor.execute_calls[0]
    assert "`text.with%%tick```" in conn._cursor.execute_calls[0]
    assert conn._cursor.payload_path is not None
    assert not conn._cursor.payload_path.parent.exists()
    assert not conn.closed


@pytest.mark.parametrize("failure", [RuntimeError("transfer failed"), asyncio.CancelledError()])
async def test_local_infile_failure_discards_connection_and_payload(failure: BaseException) -> None:
    conn = _FakeConnection()
    conn._cursor.failure = failure
    driver = AsyncmyDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )
    with pytest.raises(type(failure)) as caught:
        await driver.load_from_arrow("orders", pa.table({"id": [1]}))
    assert caught.value is failure
    assert conn.closed
    assert conn._cursor.payload_path is not None
    assert not conn._cursor.payload_path.parent.exists()


async def test_local_infile_empty_table_does_not_send_payload() -> None:
    conn = _FakeConnection()
    driver = AsyncmyDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )
    job = await driver.load_from_arrow("orders", pa.table({"id": pa.array([], type=pa.int64())}), overwrite=True)
    assert job.telemetry["rows_processed"] == 0
    assert conn._cursor.execute_calls == ["TRUNCATE TABLE `orders`"]
    assert conn._cursor.executemany_calls == []
    assert conn._cursor.payload_path is None


@pytest.mark.parametrize(
    ("values", "expected"),
    [([[1, 2]], "[1,2]"), ([b"\x00\xff"], b"\x00\xff"), ([timedelta(days=1)], timedelta(days=1))],
)
async def test_local_infile_unsupported_values_fall_back_to_executemany(values: Any, expected: Any) -> None:
    conn = _FakeConnection()
    driver = AsyncmyDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )
    await driver.load_from_arrow("orders", pa.table({"data": values}))
    assert conn._cursor.execute_calls == []
    assert len(conn._cursor.executemany_calls) == 1
    assert conn._cursor.executemany_calls[0][1] == [(expected,)]
    assert conn._cursor.payload_path is None


@pytest.mark.parametrize("stage", ["encode", "create", "write"])
async def test_local_infile_preparation_failure_removes_private_directory(
    stage: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tempfile
    from unittest.mock import Mock

    import sqlspec.adapters.asyncmy.driver as driver_module

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    failure = OSError("payload preparation failed")
    if stage == "encode":
        monkeypatch.setattr(driver_module, "encode_records_for_local_infile", Mock(side_effect=failure))
    elif stage == "create":
        monkeypatch.setattr(tempfile, "NamedTemporaryFile", Mock(side_effect=failure))
    else:
        original = tempfile.NamedTemporaryFile

        def failing_writer(*args: Any, **kwargs: Any) -> Any:
            file = original(*args, **kwargs)
            file.write = Mock(side_effect=failure)
            return file

        monkeypatch.setattr(tempfile, "NamedTemporaryFile", failing_writer)
    conn = _FakeConnection()
    driver = AsyncmyDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )
    with pytest.raises(OSError, match="payload preparation failed"):
        await driver.load_from_arrow("orders", pa.table({"id": [1]}))
    assert list(tmp_path.iterdir()) == []  # noqa: ASYNC240
    assert conn._cursor.execute_calls == []
    assert not conn.closed


@pytest.mark.parametrize("unbuffered", [False, True])
@pytest.mark.parametrize("existing_hook", [False, True])
@pytest.mark.parametrize("outcome", ["success", "unexpected_filename", "invalid_ack", "sender_error", "cancelled"])
async def test_local_infile_native_handoff_and_hook_restoration(
    outcome: str, existing_hook: bool, unbuffered: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from asyncmy import Connection
    from asyncmy.protocol import MysqlPacket

    import sqlspec.adapters.asyncmy._typing as native
    from sqlspec.exceptions import SQLSpecError

    connection: Any = Connection(local_infile=True)  # type: ignore[no-untyped-call]
    connection._connected = True
    original_reader = AsyncMock()
    if existing_hook:
        connection._read_query_result = original_reader
    request = b"\xfb/tmp/unexpected" if outcome == "unexpected_filename" else b"\xfb/tmp/payload"
    ack = b"\xfe\x00\x00\x00\x00" if outcome == "invalid_ack" else b"\x00\x01\x00\x02\x00\x00\x00"
    connection.read_packet = AsyncMock(side_effect=[MysqlPacket(request, "utf8"), MysqlPacket(ack, "utf8")])
    failure = asyncio.CancelledError() if outcome == "cancelled" else OSError("native sender failed")
    send = AsyncMock(side_effect=failure if outcome in {"sender_error", "cancelled"} else None)
    sender = Mock(return_value=SimpleNamespace(send_data=send))
    monkeypatch.setattr(native, "_LoadLocalFile", sender)
    result_type = native._AsyncmyLocalInfileResult
    results: list[Any] = []

    def capture_result(raw: Any, filename: str) -> Any:
        result = result_type(raw, filename)
        results.append(result)
        return result

    monkeypatch.setattr(native, "_AsyncmyLocalInfileResult", capture_result)
    if outcome == "success":
        with native.asyncmy_local_infile(connection, "/tmp/payload"):
            await connection._read_query_result(unbuffered=unbuffered)
        assert connection._affected_rows == 1
        assert connection.server_status == 2
        assert connection.connected
        sender.assert_called_once_with("/tmp/payload", connection)
        send.assert_awaited_once()
    else:
        error_type = type(failure) if outcome in {"sender_error", "cancelled"} else SQLSpecError
        with pytest.raises(error_type):
            with native.asyncmy_local_infile(connection, "/tmp/payload"):
                await connection._read_query_result(unbuffered=unbuffered)
        assert not connection.connected
        if unbuffered:
            assert len(results) == 1
            assert not results[0].unbuffered_active
            assert results[0].connection is None
        if outcome == "unexpected_filename":
            sender.assert_not_called()
    if existing_hook:
        assert connection._read_query_result is original_reader
    else:
        assert "_read_query_result" not in connection.__dict__
    original_reader.assert_not_called()
