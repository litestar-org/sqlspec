"""MySQL pool acquisition and shutdown lifecycle regressions."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.adapters.asyncmy.config import AsyncmyConfig


@pytest.mark.parametrize("config_type", [AiomysqlConfig, AsyncmyConfig])
async def test_cancelled_connection_hook_releases_pool_checkout(config_type: Any) -> None:
    connection = MagicMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=connection)
    context.__aexit__ = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.acquire.return_value = context
    hook = AsyncMock(side_effect=asyncio.CancelledError)
    config = config_type(connection_instance=pool, driver_features={"on_connection_create": hook})

    with pytest.raises(asyncio.CancelledError):
        async with config.provide_connection():
            pytest.fail("Cancelled hook must prevent connection delivery")

    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("config_type", [AiomysqlConfig, AsyncmyConfig])
async def test_pool_shutdown_failure_is_reported_and_pool_retained(config_type: Any) -> None:
    pool = MagicMock()
    pool.wait_closed = AsyncMock(side_effect=RuntimeError("shutdown failed"))
    config = config_type(connection_instance=pool)

    with pytest.raises(RuntimeError, match="shutdown failed"):
        await config.close_pool()

    assert config.connection_instance is pool
