"""Unit tests for Oracle native Arrow batch export."""

from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.oracledb.core import build_arrow_fetch_kwargs
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.utils.arrow_helpers import (
    coerce_oracle_df_to_arrow_batches,
    coerce_oracle_df_to_arrow_table,
    oracle_df_to_arrow_table,
)

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection


class _OracleBatchConnection:
    def __init__(self) -> None:
        self.fetch_batch_calls: list[dict[str, object]] = []

    def fetch_df_batches(self, **kwargs: object) -> Iterator[pa.Table]:
        self.fetch_batch_calls.append(kwargs)
        yield pa.table({"ID": [1, 2], "VALUE": [10, 20]})
        yield pa.table({"ID": [3], "VALUE": [30]})

    def fetch_df_all(self, **_kwargs: object) -> object:
        msg = "reader and batches formats should use fetch_df_batches(), not fetch_df_all()"
        raise AssertionError(msg)


class _OracleAllConnection:
    def __init__(self) -> None:
        self.fetch_all_calls: list[dict[str, object]] = []

    def fetch_df_all(self, **kwargs: object) -> pa.Table:
        self.fetch_all_calls.append(kwargs)
        return pa.table({"ID": [1], "VALUE": [10]})


class _OracleAsyncBatchConnection:
    def __init__(self) -> None:
        self.fetch_batch_calls: list[dict[str, object]] = []

    async def fetch_df_batches(self, **kwargs: object) -> AsyncIterator[pa.Table]:
        self.fetch_batch_calls.append(kwargs)
        yield pa.table({"ID": [1, 2], "VALUE": [10, 20]})
        yield pa.table({"ID": [3], "VALUE": [30]})

    async def fetch_df_all(self, **_kwargs: object) -> object:
        msg = "reader and batches formats should use fetch_df_batches(), not fetch_df_all()"
        raise AssertionError(msg)


class _OracleAsyncAllConnection:
    def __init__(self) -> None:
        self.fetch_all_calls: list[dict[str, object]] = []

    async def fetch_df_all(self, **kwargs: object) -> pa.Table:
        self.fetch_all_calls.append(kwargs)
        return pa.table({"ID": [1], "VALUE": [10]})


def test_sync_select_to_arrow_batches_uses_fetch_df_batches() -> None:
    connection = _OracleBatchConnection()
    driver = OracleSyncDriver(
        cast("OracleSyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = driver.select_to_arrow("SELECT id, value FROM example", return_format="batches", batch_size=2)

    batches = result.get_data()
    assert [batch.num_rows for batch in batches] == [2, 1]
    assert batches[0].schema.names == ["id", "value"]
    assert result.rows_affected == 3
    assert connection.fetch_batch_calls[0]["size"] == 2
    assert connection.fetch_batch_calls[0]["fetch_decimals"] is True
    assert "fetch_lobs" not in connection.fetch_batch_calls[0]


def test_sync_select_to_arrow_table_omits_fetch_lobs_from_fetch_df_all() -> None:
    connection = _OracleAllConnection()
    driver = OracleSyncDriver(
        cast("OracleSyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = driver.select_to_arrow("SELECT id, value FROM example", batch_size=2)

    assert result.get_data().to_pydict() == {"id": [1], "value": [10]}
    assert connection.fetch_all_calls[0]["arraysize"] == 2
    assert connection.fetch_all_calls[0]["fetch_decimals"] is True
    assert "fetch_lobs" not in connection.fetch_all_calls[0]


@pytest.mark.anyio
async def test_async_select_to_arrow_reader_uses_fetch_df_batches() -> None:
    connection = _OracleAsyncBatchConnection()
    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = await driver.select_to_arrow("SELECT id, value FROM example", return_format="reader", batch_size=2)

    assert result.rows_affected == 3
    assert result.get_data().read_all().to_pydict() == {"id": [1, 2, 3], "value": [10, 20, 30]}
    assert connection.fetch_batch_calls[0]["size"] == 2
    assert connection.fetch_batch_calls[0]["fetch_decimals"] is True
    assert "fetch_lobs" not in connection.fetch_batch_calls[0]


@pytest.mark.anyio
async def test_async_select_to_arrow_table_omits_fetch_lobs_from_fetch_df_all() -> None:
    connection = _OracleAsyncAllConnection()
    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = await driver.select_to_arrow("SELECT id, value FROM example", batch_size=2)

    assert result.get_data().to_pydict() == {"id": [1], "value": [10]}
    assert connection.fetch_all_calls[0]["arraysize"] == 2
    assert connection.fetch_all_calls[0]["fetch_decimals"] is True
    assert "fetch_lobs" not in connection.fetch_all_calls[0]


class _StreamOnlyOracleDataFrame:
    """Fake OracleDataFrame that exposes only __arrow_c_stream__."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table
        self.stream_calls = 0

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        self.stream_calls += 1
        return cast("Any", self._table).__arrow_c_stream__(requested_schema=requested_schema)

    def __arrow_c_array__(self, *_args: Any, **_kwargs: Any) -> Any:
        msg = "select_to_arrow must use __arrow_c_stream__ rather than __arrow_c_array__"
        raise AssertionError(msg)


def test_build_arrow_fetch_kwargs_excludes_fetch_lobs_and_passes_requested_schema() -> None:
    schema = pa.schema([("id", pa.int64())])
    kwargs = build_arrow_fetch_kwargs({"fetch_lobs": True, "fetch_decimals": True}, arrow_schema=schema, batch_size=500)
    assert "fetch_lobs" not in kwargs
    assert kwargs["arraysize"] == 500
    assert kwargs["fetch_decimals"] is True
    assert kwargs["requested_schema"] is schema


def test_select_to_arrow_consumes_arrow_c_stream_pycapsule_sync() -> None:
    raw_table = pa.table({"ID": [1, 2], "VALUE": [10, 20]})
    stream_df = _StreamOnlyOracleDataFrame(raw_table)

    class _StreamConn:
        def fetch_df_all(self, **_kwargs: object) -> _StreamOnlyOracleDataFrame:
            return stream_df

    driver = OracleSyncDriver(
        cast("OracleSyncConnection", _StreamConn()), driver_features={"enable_lowercase_column_names": True}
    )
    res_table = driver.select_to_arrow("SELECT id, value FROM example", return_format="table")
    assert res_table.get_data().to_pydict() == {"id": [1, 2], "value": [10, 20]}
    assert stream_df.stream_calls == 1

    batches = coerce_oracle_df_to_arrow_batches(stream_df, pa)
    assert sum(b.num_rows for b in batches) == 2
    assert coerce_oracle_df_to_arrow_table(stream_df, pa).num_rows == 2


@pytest.mark.anyio
async def test_select_to_arrow_consumes_arrow_c_stream_pycapsule_async() -> None:
    raw_table = pa.table({"ID": [1, 2], "VALUE": [10, 20]})
    stream_df = _StreamOnlyOracleDataFrame(raw_table)

    class _AsyncStreamConn:
        async def fetch_df_all(self, **_kwargs: object) -> _StreamOnlyOracleDataFrame:
            return stream_df

    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", _AsyncStreamConn()), driver_features={"enable_lowercase_column_names": True}
    )
    res_table = await driver.select_to_arrow("SELECT id, value FROM example", return_format="table")
    assert res_table.get_data().to_pydict() == {"id": [1, 2], "value": [10, 20]}


def test_oracle_df_to_arrow_table_lazy_pyarrow_resolution() -> None:
    """oracle_df_to_arrow_table lazily resolves pyarrow when pa_module is None."""
    raw_table = pa.table({"ID": [1, 2], "VALUE": [10, 20]})
    stream_df = _StreamOnlyOracleDataFrame(raw_table)
    table = oracle_df_to_arrow_table(stream_df, pa_module=None)
    assert table.num_rows == 2


def test_select_to_arrow_maps_native_database_error() -> None:
    """select_to_arrow routes oracledb.DatabaseError through exception handler."""
    import oracledb

    from sqlspec.exceptions import SQLSpecError

    class _FailingOracleConn:
        def fetch_df_all(self, **_kwargs: object) -> None:
            raise oracledb.DatabaseError("ORA-00942: table or view does not exist")

    driver = OracleSyncDriver(cast("OracleSyncConnection", _FailingOracleConn()))
    with pytest.raises(SQLSpecError):
        driver.select_to_arrow("SELECT * FROM non_existent", return_format="table")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("return_format", ["batches", "reader"])
async def test_arrow_batch_failure_preserves_database_error(is_async: bool, return_format: Any) -> None:
    import oracledb

    from sqlspec.exceptions import SQLParsingError

    class _FailingConnection:
        def fetch_df_all(self, **_kwargs: Any) -> None:
            raise AssertionError("batch export should use fetch_df_batches")

        def fetch_df_batches(self, **_kwargs: Any) -> Iterator[pa.Table]:
            raise oracledb.DatabaseError("ORA-00942: missing table")
            yield pa.table({"ID": [1]})

    class _FailingAsyncConnection:
        async def fetch_df_all(self, **_kwargs: Any) -> None:
            raise AssertionError("batch export should use fetch_df_batches")

        async def fetch_df_batches(self, **_kwargs: Any) -> AsyncIterator[pa.Table]:
            raise oracledb.DatabaseError("ORA-00942: missing table")
            yield pa.table({"ID": [1]})

    with pytest.raises(SQLParsingError, match="ORA-00942") as exc_info:
        if is_async:
            await OracleAsyncDriver(cast("Any", _FailingAsyncConnection())).select_to_arrow(
                "SELECT * FROM missing", return_format=return_format
            )
        else:
            OracleSyncDriver(cast("Any", _FailingConnection())).select_to_arrow(
                "SELECT * FROM missing", return_format=return_format
            )

    assert isinstance(exc_info.value.__cause__, oracledb.DatabaseError)


def test_lazy_arrow_reader_maps_errors_after_the_first_batch() -> None:
    import oracledb

    from sqlspec.exceptions import SQLParsingError

    class _FailingConnection:
        def fetch_df_batches(self, **_kwargs: Any) -> Iterator[pa.Table]:
            yield pa.table({"ID": [1]})
            raise oracledb.DatabaseError("ORA-00942: missing table")

        def fetch_df_all(self, **_kwargs: Any) -> None:
            raise AssertionError("reader export should use fetch_df_batches")

    result = OracleSyncDriver(cast("Any", _FailingConnection())).select_to_arrow(
        "SELECT * FROM example", return_format="reader"
    )
    reader = result.get_data()
    assert reader.read_next_batch().to_pydict() == {"ID": [1]}
    with pytest.raises(SQLParsingError, match="ORA-00942") as exc_info:
        reader.read_next_batch()
    assert isinstance(exc_info.value.__cause__, oracledb.DatabaseError)


def test_sync_select_to_arrow_reader_yields_lazily() -> None:
    """return_format=reader yields record batches lazily from the underlying stream."""
    yielded: list[int] = []

    def _generate_tables() -> Iterator[pa.Table]:
        yielded.append(1)
        yield pa.table({"ID": [1, 2], "VALUE": [10, 20]})
        yielded.append(2)
        yield pa.table({"ID": [3], "VALUE": [30]})

    class _LazyBatchConnection:
        def fetch_df_batches(self, **_kwargs: object) -> Iterator[pa.Table]:
            return _generate_tables()

    driver = OracleSyncDriver(
        cast("OracleSyncConnection", _LazyBatchConnection()), driver_features={"enable_lowercase_column_names": True}
    )

    result = driver.select_to_arrow("SELECT id, value FROM example", return_format="reader", batch_size=2)
    reader = result.get_data()
    assert yielded == [1]
    first_batch = reader.read_next_batch()
    assert first_batch.num_rows == 2
    assert yielded == [1]
    second_batch = reader.read_next_batch()
    assert second_batch.num_rows == 1
    assert yielded == [1, 2]
