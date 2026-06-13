"""Unit tests for Oracle fetch tuning options."""

from typing import TYPE_CHECKING, cast

import pytest

from sqlspec.adapters.oracledb._typing import OracleAsyncCursor, OracleSyncCursor
from sqlspec.adapters.oracledb.core import build_fetch_kwargs

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


def test_build_fetch_kwargs_omits_absent_options() -> None:
    assert build_fetch_kwargs({}) == {}


def test_build_fetch_kwargs_includes_explicit_fetch_options() -> None:
    assert build_fetch_kwargs({"fetch_lobs": False, "fetch_decimals": True}) == {
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
