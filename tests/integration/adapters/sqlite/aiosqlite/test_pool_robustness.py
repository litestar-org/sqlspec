"""Regression: the aiosqlite pool must surface connection-create failures, not stall to a timeout."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_connection_create_failure_surfaces_real_error() -> None:
    """A raising on_connection_create propagates its real error instead of stalling to a connect timeout."""

    async def failing_hook(connection: object) -> None:
        raise RuntimeError("hook boom")

    config = AiosqliteConfig(
        connection_config={"database": ":memory:", "connect_timeout": 1.0},
        driver_features={"on_connection_create": failing_hook},
    )
    try:
        with pytest.raises(RuntimeError, match="hook boom"):
            async with config.provide_session() as session:
                await session.execute("SELECT 1")
    finally:
        await config.close_pool()
