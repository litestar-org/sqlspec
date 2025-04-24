"""Spanner Configuration and Connection Integration Tests using pytest-databases."""

from typing import Any

import pytest
from google.cloud.spanner_v1.pool import AbstractSessionPool
from pytest_databases.docker.spanner import SpannerService

# Import sqlspec types
from sqlspec.adapters.spanner import SpannerConfig, SpannerPoolConfig


@pytest.fixture(scope="session")
def spanner_emulator_project(spanner_service: SpannerService) -> str:
    return spanner_service.project


@pytest.fixture(scope="session")
def spanner_emulator_instance(spanner_service: SpannerService) -> str:
    return spanner_service.instance  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def spanner_emulator_database(spanner_service: SpannerService) -> str:
    return spanner_service.database  # type: ignore[attr-defined]


@pytest.fixture(scope="module")  # Use module scope for config fixtures
def sync_config(
    spanner_emulator_project: str,
    spanner_emulator_instance: str,
    spanner_emulator_database: str,
) -> Any:  # -> SpannerConfig:
    """Provides a SpannerConfig configured for the emulator."""
    config = SpannerConfig(
        pool_config=SpannerPoolConfig(
            project=spanner_emulator_project,
            instance_id=spanner_emulator_instance,
            database_id=spanner_emulator_database,
        )
    )
    yield config
    # Cleanup pool resources after tests in the module are done
    config.close_pool()


def test_sync_config_properties(sync_config: Any) -> None:
    assert sync_config.is_async is False
    assert sync_config.support_connection_pooling is True  # Spanner uses pools
    assert issubclass(sync_config.driver_type, SpannerSyncDriver)
    # Check connection_type can be resolved (might need adjustment based on actual Union)
    assert sync_config.connection_type is not None


def test_sync_provide_pool(sync_config: Any) -> None:
    pool = sync_config.provide_pool()
    assert pool is not None
    assert isinstance(pool, AbstractSessionPool)  # Check type
    assert pool is sync_config.pool_instance
    pool2 = sync_config.provide_pool()
    assert pool is pool2  # Should return the same instance


def test_sync_provide_connection(sync_config: Any) -> None:
    # provide_connection for Spanner usually yields a Transaction
    from google.cloud.spanner_v1.transaction import Transaction  # Import here for isinstance check

    with sync_config.provide_connection() as connection:
        assert connection is not None
        # Check if connection is of expected Spanner sync type (Transaction)
        assert isinstance(connection, Transaction)
    # Check if context manager cleaned up properly (specific checks depend on impl)


def test_sync_provide_session(sync_config: Any) -> None:
    from google.cloud.spanner_v1.transaction import Transaction  # Import here for isinstance check

    with sync_config.provide_session() as driver:
        assert isinstance(driver, SpannerSyncDriver)
        assert driver.connection is not None
        assert isinstance(driver.connection, Transaction)


def test_sync_close_pool(sync_config: Any) -> None:
    # Need a fresh config instance for this test to avoid state pollution
    # Re-create config based on emulator details
    config = SpannerConfig(
        project=sync_config.project,
        instance_id=sync_config.instance_id,
        database_id=sync_config.database_id,
    )
    _pool = config.provide_pool()  # Ensure pool exists
    assert config.pool_instance is not None
    # Check internal state if _ping_thread exists and is accessible
    assert hasattr(config, "_ping_thread")
    assert config._ping_thread is not None
    config.close_pool()
    assert config.pool_instance is None
    assert config._database is None  # Check internal cleanup
    assert config._client is None
    assert hasattr(config, "_ping_thread")
    assert config._ping_thread is None


# --- Async Tests ---


@pytest.mark.asyncio
async def test_async_config_properties(async_config: Any) -> None:
    assert async_config.is_async is True
    assert async_config.support_connection_pooling is True
    assert issubclass(async_config.driver_type, SpannerAsyncDriver)
    assert async_config.connection_type is not None


@pytest.mark.asyncio
async def test_async_provide_pool(async_config: Any) -> None:
    # Pool creation itself is sync in the current config implementation
    pool = async_config.provide_pool()
    assert pool is not None
    assert isinstance(pool, AbstractSessionPool)
    assert pool is async_config.pool_instance
    pool2 = async_config.provide_pool()
    assert pool is pool2


@pytest.mark.asyncio
async def test_async_provide_connection(async_config: Any) -> None:
    # provide_connection for Spanner usually yields an AsyncTransaction
    async with async_config.provide_connection() as connection:
        assert connection is not None
        # Check if connection is of expected Spanner async type
        assert isinstance(connection, AsyncTransaction)


@pytest.mark.asyncio
async def test_async_provide_session(async_config: Any) -> None:
    async with async_config.provide_session() as driver:
        assert isinstance(driver, SpannerAsyncDriver)
        assert driver.connection is not None
        assert isinstance(driver.connection, AsyncTransaction)


@pytest.mark.asyncio
async def test_async_close_pool(async_config: Any) -> None:
    # Need a fresh config instance for this test
    config = AsyncSpannerConfig(
        project=async_config.project,
        instance_id=async_config.instance_id,
        database_id=async_config.database_id,
    )
    _pool = config.provide_pool()  # Ensure pool exists
    assert config.pool_instance is not None
    # Check internal state if _ping_thread exists and is accessible
    assert hasattr(config, "_ping_thread")
    assert config._ping_thread is not None  # noqa: SLF001
    # Close pool is sync in current implementation
    config.close_pool()
    assert config.pool_instance is None
    assert config._database is None  # noqa: SLF001
    assert config._client is None  # noqa: SLF001
    assert hasattr(config, "_ping_thread")
    assert config._ping_thread is None  # noqa: SLF001
