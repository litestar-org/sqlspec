"""Unit tests for Oracle native Arrow batch export."""

from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver

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
    assert connection.fetch_batch_calls[0]["fetch_lobs"] is False


def test_sync_select_to_arrow_table_forwards_fetch_lobs_to_fetch_df_all() -> None:
    connection = _OracleAllConnection()
    driver = OracleSyncDriver(
        cast("OracleSyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = driver.select_to_arrow("SELECT id, value FROM example", batch_size=2)

    assert result.get_data().to_pydict() == {"id": [1], "value": [10]}
    assert connection.fetch_all_calls[0]["arraysize"] == 2
    assert connection.fetch_all_calls[0]["fetch_decimals"] is True
    assert connection.fetch_all_calls[0]["fetch_lobs"] is False


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
    assert connection.fetch_batch_calls[0]["fetch_lobs"] is False


@pytest.mark.anyio
async def test_async_select_to_arrow_table_forwards_fetch_lobs_to_fetch_df_all() -> None:
    connection = _OracleAsyncAllConnection()
    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", connection),
        driver_features={"enable_lowercase_column_names": True, "fetch_decimals": True, "fetch_lobs": False},
    )

    result = await driver.select_to_arrow("SELECT id, value FROM example", batch_size=2)

    assert result.get_data().to_pydict() == {"id": [1], "value": [10]}
    assert connection.fetch_all_calls[0]["arraysize"] == 2
    assert connection.fetch_all_calls[0]["fetch_decimals"] is True
    assert connection.fetch_all_calls[0]["fetch_lobs"] is False
