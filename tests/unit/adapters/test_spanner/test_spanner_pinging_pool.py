"""Unit tests for Spanner PingingPool fallback and configuration."""

from google.cloud.spanner_v1.pool import PingingPool

from sqlspec.adapters.spanner.config import SpannerSyncConfig


def test_disable_multiplexed_sessions_defaults_to_pinging_pool() -> None:
    """Verify that disabling multiplexed sessions defaults to PingingPool with 1800s interval."""
    config = SpannerSyncConfig(
        connection_config={
            "project": "test-project",
            "instance_id": "test-instance",
            "database_id": "test-db",
            "enable_multiplexed_sessions": False,
        }
    )
    assert config.connection_config.get("pool_type") is PingingPool
    assert config.connection_config.get("ping_interval") == 1800

    pool = config.provide_pool()
    assert isinstance(pool, PingingPool)
    assert pool._delta.total_seconds() == 1800


def test_pinging_pool_custom_ping_interval() -> None:
    """Verify that custom ping_interval is respected when configuring PingingPool."""
    config = SpannerSyncConfig(
        connection_config={
            "project": "test-project",
            "instance_id": "test-instance",
            "database_id": "test-db",
            "enable_multiplexed_sessions": False,
            "ping_interval": 900,
        }
    )
    assert config.connection_config.get("ping_interval") == 900

    pool = config.provide_pool()
    assert isinstance(pool, PingingPool)
    assert pool._delta.total_seconds() == 900


def test_provide_pool_fallback_defaults_to_pinging_pool() -> None:
    """Verify that calling provide_pool directly falls back to PingingPool with default ping_interval."""
    config = SpannerSyncConfig(
        connection_config={"project": "test-project", "instance_id": "test-instance", "database_id": "test-db"}
    )
    pool = config.provide_pool()
    assert isinstance(pool, PingingPool)
    assert pool._delta.total_seconds() == 1800
