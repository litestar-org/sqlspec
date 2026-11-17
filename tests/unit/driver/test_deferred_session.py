"""Deferred async session behavior after the request connection is marked closed."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
from sqlspec.driver import DeferredAsyncSession
from sqlspec.driver._common import _CLOSED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize("opened", [False, True])
async def test_deferred_async_session_close_blocks_reuse_and_closes_driver(opened: bool, tmp_path: Path) -> None:
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "deferred_close.db")})
    session_ctx = config.provide_session()
    active_session: Any = None
    open_calls = 0

    async def open_session() -> Any:
        nonlocal active_session, open_calls
        open_calls += 1
        active_session = await session_ctx.__aenter__()
        return active_session

    deferred = DeferredAsyncSession(
        config=config,
        open_session=open_session,
        current_session=lambda: active_session,
        accessor="await plugin.get_session_async(request)",
    )
    try:
        if opened:
            assert await deferred.select_value("SELECT 1") == 1
            assert active_session.connection is not _CLOSED_CONNECTION

        deferred._mark_closed()

        if opened:
            assert active_session.connection is _CLOSED_CONNECTION

        with pytest.raises(ImproperConfigurationError, match="already closed"):
            await deferred.select_value("SELECT 1")

        with pytest.raises(ImproperConfigurationError, match="already closed"):
            _ = deferred.dialect

        assert open_calls == (1 if opened else 0)
    finally:
        if active_session is not None:
            await session_ctx.__aexit__(None, None, None)
        await config.close_pool()


class _InstanceDialectAiosqliteDriver(AiosqliteDriver):
    """aiosqlite driver that stores its dialect on the instance instead of the class."""

    __slots__ = ("dialect",)

    def __init__(self, connection: Any) -> None:
        super().__init__(connection)
        self.dialect = "sqlite"


@pytest.mark.parametrize("target_type", [None, _InstanceDialectAiosqliteDriver], ids=["class", "instance"])
async def test_deferred_async_session_serves_dialect_before_connection_opens(target_type: Any) -> None:
    """``dialect`` is available before checkout wherever the driver stores it."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    opened = False

    async def open_session() -> Any:
        nonlocal opened
        opened = True
        return None

    deferred = DeferredAsyncSession(
        config=config,
        open_session=open_session,
        current_session=lambda: None,
        accessor="await plugin.get_session_async(request)",
        target_type=target_type,
    )
    try:
        assert deferred.dialect == "sqlite"
        assert opened is False
    finally:
        await config.close_pool()
