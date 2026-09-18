"""psqlpy transaction state follows begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyDatabaseError
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import PsqlpyStreamSource
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.anyio


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.statements.append(sql)

    def in_transaction(self) -> Any:
        msg = "in_transaction() is not a reliable transaction predicate"
        raise AssertionError(msg)


async def test_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False
    assert connection.statements == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]


@pytest.mark.parametrize("operation", ["commit", "rollback"])
async def test_failed_transaction_end_clears_the_flag(operation: str) -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    async def fail(sql: str, *args: Any, **kwargs: Any) -> None:
        raise PsqlpyDatabaseError(sql)

    connection.execute = fail  # type: ignore[method-assign]
    with pytest.raises(SQLSpecError):
        await getattr(driver, operation)()
    assert driver._connection_in_transaction() is False


class _FakeCursor:
    def __init__(self, owner: str) -> None:
        self.owner = owner
        self.closed = False

    async def start(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _FakeTransaction:
    def __init__(self, record: "list[str]") -> None:
        self._record = record

    async def begin(self) -> None:
        self._record.append("begin")

    async def commit(self) -> None:
        self._record.append("commit")

    async def rollback(self) -> None:
        self._record.append("rollback")

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("transaction")


class _StreamingConnection(_FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.transaction_calls: list[str] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self.transaction_calls)

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("connection")


async def test_stream_inside_a_user_transaction_does_not_commit_it() -> None:
    """A stream must never end a transaction it did not open."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == []
    assert driver._connection_in_transaction() is True
    assert connection.statements == ["BEGIN"]


async def test_stream_outside_a_transaction_manages_its_own() -> None:
    """With no caller transaction the stream still opens and commits its own."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()

    assert driver._connection_in_transaction() is True

    await source.close()

    assert connection.transaction_calls == ["begin", "commit"]
    assert driver._connection_in_transaction() is False


async def test_stream_error_close_rolls_back_only_its_own_transaction() -> None:
    """An errored stream rolls back the transaction it owns, not the caller's."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close(error=True)

    assert connection.transaction_calls == ["begin", "rollback"]
    assert driver._connection_in_transaction() is False


class _CopyConnection(_FakeConnection):
    """Records the native COPY call and fails loudly on a row-by-row fallback."""

    def __init__(self) -> None:
        super().__init__()
        self.copy_calls: list[tuple[str, list[Any], dict[str, Any]]] = []

    async def copy_records_to_table(self, table_name: str, records: Any, **kwargs: Any) -> int:
        self.copy_calls.append((table_name, list(records), kwargs))
        return len(self.copy_calls)

    def execute_many(self, *_args: Any, **_kwargs: Any) -> None:
        msg = "load_from_arrow must not fall back to row-by-row inserts"
        raise AssertionError(msg)


async def test_load_from_arrow_uses_the_native_copy_path() -> None:
    """Arrow ingest must go through copy_records_to_table, never execute_many."""
    pa = pytest.importorskip("pyarrow")
    connection = _CopyConnection()
    config = PsqlpyConfig()
    driver = PsqlpyDriver(
        cast("Any", connection), driver_features={"storage_capabilities": config.storage_capabilities()}
    )
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    await driver.load_from_arrow("analytics.events", table)

    assert len(connection.copy_calls) == 1
    table_name, records, kwargs = connection.copy_calls[0]
    assert table_name == "events"
    assert records == [(1, "a"), (2, "b")]
    assert kwargs == {"columns": ["id", "name"], "schema_name": "analytics"}
