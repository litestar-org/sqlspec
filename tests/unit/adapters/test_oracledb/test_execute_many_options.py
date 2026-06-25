"""Oracle execute_many batcherrors / arraydmlrowcounts execution-args options."""

from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core import SQL


class _FakeBatchError:
    def __init__(self, offset: int, code: int, message: str) -> None:
        self.offset = offset
        self.code = code
        self.message = message


class _FakeManyCursor:
    def __init__(self, batch_errors: "list[Any] | None" = None, dml_row_counts: "list[int] | None" = None) -> None:
        self.executemany_calls: list[tuple[str, Any, dict[str, Any]]] = []
        self._batch_errors = batch_errors or []
        self._dml_row_counts = dml_row_counts or []
        self.rowcount = 0
        self.description = None

    def executemany(self, sql: str, parameters: Any, **kwargs: Any) -> None:
        self.executemany_calls.append((sql, parameters, kwargs))

    async def aexecutemany(self, sql: str, parameters: Any, **kwargs: Any) -> None:
        self.executemany_calls.append((sql, parameters, kwargs))

    def getbatcherrors(self) -> "list[Any]":
        return self._batch_errors

    def getarraydmlrowcounts(self) -> "list[int]":
        return self._dml_row_counts


class _AsyncFakeManyCursor(_FakeManyCursor):
    async def executemany(self, sql: str, parameters: Any, **kwargs: Any) -> None:  # type: ignore[override]
        self.executemany_calls.append((sql, parameters, kwargs))


def _sync_driver() -> OracleSyncDriver:
    return OracleSyncDriver(cast("Any", object()))


def _async_driver() -> OracleAsyncDriver:
    return OracleAsyncDriver(cast("Any", object()))


def test_default_execute_many_passes_disabled_batch_options() -> None:
    driver = _sync_driver()
    statement = SQL("INSERT INTO t (a) VALUES (:a)", [{"a": 1}, {"a": 2}], statement_config=driver.statement_config, is_many=True)
    cursor = _FakeManyCursor()

    result = driver.dispatch_execute_many(cursor, statement)

    assert cursor.executemany_calls[0][2] == {"batcherrors": False, "arraydmlrowcounts": False}
    assert result.special_data is None
    assert result.rowcount_override == 2


def test_batch_errors_surface_in_special_data() -> None:
    driver = _sync_driver()
    config = driver.statement_config.replace(execution_args={"oracle_batch_errors": True})
    statement = SQL("INSERT INTO t (a) VALUES (:a)", [{"a": 1}, {"a": 2}], statement_config=config, is_many=True)
    cursor = _FakeManyCursor(batch_errors=[_FakeBatchError(offset=1, code=1, message="ORA-00001")])

    result = driver.dispatch_execute_many(cursor, statement)

    assert cursor.executemany_calls[0][2]["batcherrors"] is True
    assert result.special_data == {"oracle_batch_errors": [{"offset": 1, "code": 1, "message": "ORA-00001"}]}


def test_array_dml_row_counts_override_rowcount() -> None:
    driver = _sync_driver()
    config = driver.statement_config.replace(execution_args={"oracle_array_dml_row_counts": True})
    statement = SQL("UPDATE t SET a = :a WHERE id = :a", [{"a": 1}, {"a": 2}, {"a": 3}], statement_config=config, is_many=True)
    cursor = _FakeManyCursor(dml_row_counts=[2, 3, 5])

    result = driver.dispatch_execute_many(cursor, statement)

    assert cursor.executemany_calls[0][2]["arraydmlrowcounts"] is True
    assert result.special_data == {"oracle_dml_row_counts": [2, 3, 5]}
    assert result.rowcount_override == 10


@pytest.mark.anyio
async def test_async_batch_errors_surface_in_special_data() -> None:
    driver = _async_driver()
    config = driver.statement_config.replace(execution_args={"oracle_batch_errors": True})
    statement = SQL("INSERT INTO t (a) VALUES (:a)", [{"a": 1}, {"a": 2}], statement_config=config, is_many=True)
    cursor = _AsyncFakeManyCursor(batch_errors=[_FakeBatchError(offset=0, code=2291, message="ORA-02291")])

    result = await driver.dispatch_execute_many(cursor, statement)

    assert cursor.executemany_calls[0][2]["batcherrors"] is True
    assert result.special_data == {"oracle_batch_errors": [{"offset": 0, "code": 2291, "message": "ORA-02291"}]}
