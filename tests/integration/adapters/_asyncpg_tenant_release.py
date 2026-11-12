"""Native asyncpg pool reset ownership through tenant provider boundaries."""

import asyncio
from typing import Any

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.exceptions import TenantCleanupError
from sqlspec.utils.tenant import TenantContext


async def assert_owned_tenant_reset(
    config_type: type[AsyncpgConfig] | type[CockroachAsyncpgConfig], connection_config: dict[str, Any], scenario: str
) -> None:
    """Require the prior native reset to finish before the holder is reused."""
    reset_started = asyncio.Event()
    complete_reset = asyncio.Event()
    reset_tasks: list[asyncio.Task[object]] = []
    first_reset = True
    previous_pid: object = None

    async def reset(_connection: object) -> None:
        nonlocal first_reset
        if not first_reset:
            return
        first_reset = False
        task = asyncio.current_task()
        if task is None:
            pytest.fail("Native reset did not run in a task")
        reset_tasks.append(task)
        reset_started.set()
        await complete_reset.wait()
        if scenario == "reset_error":
            msg = "Native reset callback failed"
            raise RuntimeError(msg)

    params = {**connection_config, "min_size": 1, "max_size": 1, "reset": reset}
    config = config_type(connection_config=params, driver_features={"tenant_strategy": "rls"})

    async def tenant_a() -> None:
        nonlocal previous_pid
        with TenantContext.scope("a"):
            async with config.provide_connection() as connection:
                previous_pid = await connection.fetchval("SELECT pg_backend_pid()")

    previous_exit = asyncio.create_task(tenant_a())
    try:
        await asyncio.wait_for(reset_started.wait(), timeout=10)
        if scenario == "cancel":
            previous_exit.cancel()
        elif scenario == "reset_error":
            complete_reset.set()
        expected = asyncio.CancelledError if scenario == "cancel" else TenantCleanupError
        with pytest.raises(expected):
            await asyncio.wait_for(previous_exit, timeout=15)
        reset_finished_before_reuse = all(task.done() for task in reset_tasks)
        with TenantContext.scope("b"):
            async with config.provide_connection() as connection:
                assert await connection.fetchval("SELECT pg_backend_pid()") != previous_pid
                complete_reset.set()
                await asyncio.gather(*reset_tasks, return_exceptions=True)
                assert await connection.fetchval("SELECT current_setting('app.current_tenant_id', true)") == "b"
                assert await connection.fetchval("SELECT 42") == 42
        assert reset_finished_before_reuse
        assert all(task.done() for task in reset_tasks)
    finally:
        complete_reset.set()
        if not previous_exit.done():
            previous_exit.cancel()
        await asyncio.gather(previous_exit, *reset_tasks, return_exceptions=True)
        await asyncio.wait_for(config.close_pool(), timeout=15)
