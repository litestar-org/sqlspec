"""Unit tests for Oracle fetch tuning options."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb._typing import OracleAsyncCursor, OracleSyncCursor
from sqlspec.adapters.oracledb.core import build_arrow_fetch_kwargs, build_fetch_kwargs
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection


class _RawCursor:
    def __init__(self) -> None:
        self.arraysize = 100
        self.prefetchrows = 2
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _SyncConnection:
    def __init__(self, cursor: _RawCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self) -> _RawCursor:
        return self.cursor_obj


class _StreamCursor:
    def __init__(self) -> None:
        self.arraysize = 100
        self.prefetchrows = 2
        self.execute_kwargs: dict[str, object] = {}
        self.closed = False

    def execute(self, _sql: str, _parameters: object, **kwargs: object) -> None:
        self.execute_kwargs = kwargs

    def fetchmany(self, _size: int) -> list[tuple[object, ...]]:
        return []

    def close(self) -> None:
        self.closed = True


class _AsyncStreamCursor(_StreamCursor):
    async def execute(self, _sql: str, _parameters: object, **kwargs: object) -> None:  # type: ignore[override]
        self.execute_kwargs = kwargs

    async def fetchmany(self, _size: int) -> list[tuple[object, ...]]:  # type: ignore[override]
        return []


def test_build_fetch_kwargs_omits_absent_options() -> None:
    assert build_fetch_kwargs({}) == {}


def test_build_fetch_kwargs_includes_explicit_fetch_options() -> None:
    assert build_fetch_kwargs({"fetch_lobs": False, "fetch_decimals": True}) == {
        "fetch_lobs": False,
        "fetch_decimals": True,
    }


def test_build_arrow_fetch_kwargs_matches_execute_fetch_options() -> None:
    assert build_arrow_fetch_kwargs({"fetch_lobs": False, "fetch_decimals": True}) == {
        "fetch_lobs": False,
        "fetch_decimals": True,
    }


def test_oracle_sync_cursor_applies_arraysize_and_prefetchrows() -> None:
    raw_cursor = _RawCursor()
    cursor = OracleSyncCursor(
        cast("OracleSyncConnection", _SyncConnection(raw_cursor)), arraysize=500, prefetchrows=501
    )

    with cursor as active_cursor:
        assert id(active_cursor) == id(raw_cursor)
        assert raw_cursor.arraysize == 500
        assert raw_cursor.prefetchrows == 501

    assert raw_cursor.closed is True


def test_oracle_sync_cursor_leaves_defaults_when_options_absent() -> None:
    raw_cursor = _RawCursor()
    cursor = OracleSyncCursor(cast("OracleSyncConnection", _SyncConnection(raw_cursor)))

    with cursor:
        assert raw_cursor.arraysize == 100
        assert raw_cursor.prefetchrows == 2


def test_oracle_fetch_kwargs_omit_absent_fetch_tuning_keys() -> None:
    """Per-statement fetch kwargs should stay empty unless fetch tuning is explicit."""
    assert build_fetch_kwargs({"arraysize": 100, "prefetchrows": 200}) == {}


def test_oracle_sync_stream_forwards_driver_fetch_lobs_default() -> None:
    raw_cursor = _StreamCursor()
    driver = OracleSyncDriver(
        cast("OracleSyncConnection", _SyncConnection(cast(_RawCursor, raw_cursor))),
        driver_features={"fetch_lobs": False},
    )

    stream = driver.select_stream("SELECT payload FROM example")

    assert list(stream) == []
    assert raw_cursor.execute_kwargs["fetch_lobs"] is False


def test_oracle_sync_stream_fetch_lobs_call_option_overrides_default() -> None:
    raw_cursor = _StreamCursor()
    driver = OracleSyncDriver(
        cast("OracleSyncConnection", _SyncConnection(cast(_RawCursor, raw_cursor))),
        driver_features={"fetch_lobs": False},
    )

    stream = driver.select_stream("SELECT payload FROM example", fetch_lobs=True)

    assert list(stream) == []
    assert raw_cursor.execute_kwargs["fetch_lobs"] is True


@pytest.mark.anyio
async def test_oracle_async_cursor_applies_arraysize_and_prefetchrows() -> None:
    raw_cursor = _RawCursor()
    cursor = OracleAsyncCursor(
        cast("OracleAsyncConnection", _SyncConnection(raw_cursor)), arraysize=700, prefetchrows=701
    )

    async with cursor as active_cursor:
        assert id(active_cursor) == id(raw_cursor)
        assert raw_cursor.arraysize == 700
        assert raw_cursor.prefetchrows == 701

    assert raw_cursor.closed is True


@pytest.mark.anyio
async def test_oracle_async_stream_forwards_driver_fetch_lobs_default() -> None:
    raw_cursor = _AsyncStreamCursor()
    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", _SyncConnection(cast(_RawCursor, raw_cursor))),
        driver_features={"fetch_lobs": False},
    )

    stream = driver.select_stream("SELECT payload FROM example")

    rows: list[dict[str, Any]] = [row async for row in stream]
    assert rows == []
    assert raw_cursor.execute_kwargs["fetch_lobs"] is False


@pytest.mark.anyio
async def test_oracle_async_stream_fetch_lobs_call_option_overrides_default() -> None:
    raw_cursor = _AsyncStreamCursor()
    driver = OracleAsyncDriver(
        cast("OracleAsyncConnection", _SyncConnection(cast(_RawCursor, raw_cursor))),
        driver_features={"fetch_lobs": False},
    )

    stream = driver.select_stream("SELECT payload FROM example", fetch_lobs=True)

    rows: list[dict[str, Any]] = [row async for row in stream]
    assert rows == []
    assert raw_cursor.execute_kwargs["fetch_lobs"] is True
