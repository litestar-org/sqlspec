"""Unit tests for Oracle zero-RTT session trace attribute piggybacking and UTF-8 truncation."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from sqlspec.adapters.oracledb.core import (
    ORACLE_ACTION_MAX_BYTES,
    ORACLE_CLIENT_IDENTIFIER_MAX_BYTES,
    ORACLE_ECID_MAX_BYTES,
    ORACLE_MODULE_MAX_BYTES,
    apply_driver_features,
    apply_oracle_session_trace_attributes,
    default_statement_config,
    truncate_utf8_bytes,
)
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.utils.correlation import CorrelationContext


def test_truncate_utf8_bytes_preserves_multibyte_codepoints() -> None:
    """truncate_utf8_bytes slices cleanly on multi-byte UTF-8 codepoint boundaries."""
    multibyte = "é" * 30
    truncated = truncate_utf8_bytes(multibyte, ORACLE_MODULE_MAX_BYTES)
    assert truncated == "é" * 24
    assert len(truncated.encode("utf-8")) == 48

    emoji_text = "🚀" * 20
    truncated_emoji = truncate_utf8_bytes(emoji_text, 15)
    assert truncated_emoji == "🚀" * 3
    assert len(truncated_emoji.encode("utf-8")) == 12


def test_apply_oracle_session_trace_attributes_ecid_fallback() -> None:
    """apply_oracle_session_trace_attributes falls back to conn.ecid when conn.econtext_id is absent."""

    class _StrictConn:
        __slots__ = ("action", "client_identifier", "ecid", "module")

        def __init__(self) -> None:
            self.module: str | None = None
            self.action: str | None = None
            self.client_identifier: str | None = None
            self.ecid: str | None = None

    conn = _StrictConn()
    apply_oracle_session_trace_attributes(
        conn, module="m" * 100, action="a" * 100, client_identifier="c" * 100, econtext_id="e" * 100
    )
    assert conn.module == "m" * ORACLE_MODULE_MAX_BYTES
    assert conn.action == "a" * ORACLE_ACTION_MAX_BYTES
    assert conn.client_identifier == "c" * ORACLE_CLIENT_IDENTIFIER_MAX_BYTES
    assert conn.ecid == "e" * ORACLE_ECID_MAX_BYTES


@pytest.mark.anyio
async def test_oracle_sync_and_async_driver_piggyback_trace_attributes_on_slow_and_fast_paths() -> None:
    """OracleSyncDriver and OracleAsyncDriver piggyback trace attributes on slow and cached fast paths without SQL comments."""
    sync_executed: list[tuple[str, dict[str, str | None]]] = []
    sync_conn: Any = SimpleNamespace(
        module=None, action=None, client_identifier=None, econtext_id=None, transaction_in_progress=False
    )

    class _SyncFakeCursor:
        def __init__(self) -> None:
            self.description = [("VAL", "NUMBER")]
            self.rowcount = 1
            self.arraysize = 100
            self.prefetchrows = 2

        def execute(self, sql: str, _params: Any = None, **_kwargs: Any) -> None:
            sync_executed.append((
                sql,
                {
                    "module": sync_conn.module,
                    "action": sync_conn.action,
                    "client_identifier": sync_conn.client_identifier,
                    "econtext_id": sync_conn.econtext_id,
                },
            ))

        def fetchall(self) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            return None

    sync_conn.cursor = _SyncFakeCursor
    base_config = default_statement_config.replace(
        enable_caching=True,
        enable_sqlcommenter=True,
        sqlcommenter_enable_context=True,
        sqlcommenter_enable_traceparent=True,
    )
    stmt_config, features = apply_driver_features(base_config, {"enable_session_tracing": True})
    sync_driver = OracleSyncDriver(
        connection=cast("Any", sync_conn), statement_config=stmt_config, driver_features=features
    )
    assert sync_driver._stmt_cache_enabled is True

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "1111111111111111"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/orders/1"}),
        CorrelationContext.context("corr-sync-1"),
    ):
        res1 = sync_driver.execute("SELECT 1 FROM DUAL")

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "2222222222222222"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/orders/2"}),
        CorrelationContext.context("corr-sync-2"),
    ):
        res2 = sync_driver.execute("SELECT 1 FROM DUAL")

    assert res1.data is not None
    assert res2.data is not None
    assert len(sync_executed) == 2
    assert "/*" not in sync_executed[0][0]
    assert "/*" not in sync_executed[1][0]
    assert sync_executed[0][1]["module"] == "litestar"
    assert sync_executed[0][1]["action"] == "/orders/1"
    assert sync_executed[0][1]["client_identifier"] == "corr-sync-1"
    assert sync_executed[0][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"
    assert sync_executed[1][1]["module"] == "litestar"
    assert sync_executed[1][1]["action"] == "/orders/2"
    assert sync_executed[1][1]["client_identifier"] == "corr-sync-2"
    assert sync_executed[1][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"

    async_executed: list[tuple[str, dict[str, str | None]]] = []
    async_conn: Any = SimpleNamespace(
        module=None, action=None, client_identifier=None, econtext_id=None, transaction_in_progress=False
    )

    class _AsyncFakeCursor:
        def __init__(self) -> None:
            self.description = [("VAL", "NUMBER")]
            self.rowcount = 1
            self.arraysize = 100
            self.prefetchrows = 2

        async def execute(self, sql: str, _params: Any = None, **_kwargs: Any) -> None:
            async_executed.append((
                sql,
                {
                    "module": async_conn.module,
                    "action": async_conn.action,
                    "client_identifier": async_conn.client_identifier,
                    "econtext_id": async_conn.econtext_id,
                },
            ))

        async def fetchall(self) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            return None

    async_conn.cursor = _AsyncFakeCursor
    async_driver = OracleAsyncDriver(
        connection=cast("Any", async_conn), statement_config=stmt_config, driver_features=features
    )
    assert async_driver._stmt_cache_enabled is True

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "aaaaaaaaaaaaaaaa"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/async/1"}),
        CorrelationContext.context("corr-async-1"),
    ):
        ares1 = await async_driver.execute("SELECT 1 FROM DUAL")

    with (
        patch(
            "sqlspec.adapters.oracledb.core.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "bbbbbbbbbbbbbbbb"),
        ),
        SQLCommenterContext.scope({"framework": "litestar", "route": "/async/2"}),
        CorrelationContext.context("corr-async-2"),
    ):
        ares2 = await async_driver.execute("SELECT 1 FROM DUAL")

    assert ares1.data is not None
    assert ares2.data is not None
    assert len(async_executed) == 2
    assert "/*" not in async_executed[0][0]
    assert "/*" not in async_executed[1][0]
    assert async_executed[0][1]["module"] == "litestar"
    assert async_executed[0][1]["action"] == "/async/1"
    assert async_executed[0][1]["client_identifier"] == "corr-async-1"
    assert async_executed[0][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-aaaaaaaaaaaaaaaa-01"
    assert async_executed[1][1]["module"] == "litestar"
    assert async_executed[1][1]["action"] == "/async/2"
    assert async_executed[1][1]["client_identifier"] == "corr-async-2"
    assert async_executed[1][1]["econtext_id"] == "00-0af7651916cd43dd8448eb211c80319c-bbbbbbbbbbbbbbbb-01"
