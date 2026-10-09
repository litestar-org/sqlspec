"""Unit tests for EventRuntimeHints and hint resolution."""

from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import EventsConfig
from sqlspec.extensions.events import EventRuntimeHints, get_runtime_hints


def test_event_runtime_hints_defaults() -> None:
    """EventRuntimeHints has sensible defaults."""
    hints = EventRuntimeHints()

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30
    assert hints.retention_seconds == 86_400
    assert hints.select_for_update is False
    assert hints.skip_locked is False
    assert hints.cleanup_on_ack is True
    assert hints.use_run_in_transaction is False


def test_event_runtime_hints_custom_values() -> None:
    """EventRuntimeHints accepts custom values."""
    hints = EventRuntimeHints(
        poll_interval=0.5,
        lease_seconds=60,
        retention_seconds=3600,
        select_for_update=True,
        skip_locked=True,
        cleanup_on_ack=False,
        use_run_in_transaction=True,
    )

    assert hints.poll_interval == 0.5
    assert hints.lease_seconds == 60
    assert hints.retention_seconds == 3600
    assert hints.select_for_update is True
    assert hints.skip_locked is True
    assert hints.cleanup_on_ack is False
    assert hints.use_run_in_transaction is True


def test_event_runtime_hints_frozen() -> None:
    """EventRuntimeHints is immutable."""
    hints = EventRuntimeHints()

    with pytest.raises(AttributeError):
        setattr(cast("Any", hints), "poll_interval", 2.0)


def test_get_runtime_hints_none_config() -> None:
    """get_runtime_hints returns defaults when config is None."""
    hints = get_runtime_hints(None, None)

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30


def test_get_runtime_hints_config_without_provider() -> None:
    """get_runtime_hints returns defaults for configs without hint provider."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    hints = get_runtime_hints("sqlite", config)

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30


def test_get_runtime_hints_config_with_provider() -> None:
    """get_runtime_hints calls config's hint provider when available."""

    class FakeConfig:
        def get_event_runtime_hints(self) -> EventRuntimeHints:
            return EventRuntimeHints(poll_interval=0.25, lease_seconds=10)

    config = FakeConfig()
    hints = get_runtime_hints("fake", config)

    assert hints.poll_interval == 0.25
    assert hints.lease_seconds == 10


def test_oracle_event_runtime_hints_enable_row_locking() -> None:
    """Oracle table-backed event queues use SKIP LOCKED row claims by default."""
    for config in (OracleSyncConfig(), OracleAsyncConfig()):
        hints = get_runtime_hints("oracledb", config)
        assert hints.select_for_update is True
        assert hints.skip_locked is True


def test_spanner_event_runtime_hints_disable_cleanup_on_ack_and_enable_run_in_transaction() -> None:
    """Spanner configs disable synchronous ack cleanup, enable run_in_transaction, and mark default session transaction."""
    conn_cfg = {"project": "test-proj", "instance_id": "test-inst", "database_id": "test-db"}
    for config in (SpannerSyncConfig(connection_config=conn_cfg), SpannerAsyncConfig(connection_config=conn_cfg)):
        assert config._DEFAULT_SESSION_TRANSACTION is True
        hints = get_runtime_hints("spanner", config)
        assert hints == EventRuntimeHints(cleanup_on_ack=False, use_run_in_transaction=True)


def test_events_config_typed_dict_includes_shard_count() -> None:
    """EventsConfig TypedDict includes shard_count."""
    assert "shard_count" in EventsConfig.__annotations__


def test_get_runtime_hints_provider_returns_non_hints() -> None:
    """get_runtime_hints returns defaults if provider returns non-hints."""

    class FakeConfig:
        def get_event_runtime_hints(self) -> dict[str, float]:
            return {"poll_interval": 0.5}

    config = FakeConfig()
    hints = get_runtime_hints("fake", config)

    assert hints.poll_interval == 1.0


def test_get_runtime_hints_adapter_ignored_when_config_provided() -> None:
    """Adapter name is not used when config provides hints."""

    class FakeConfig:
        def get_event_runtime_hints(self) -> EventRuntimeHints:
            return EventRuntimeHints(poll_interval=0.1)

    config = FakeConfig()
    hints = get_runtime_hints("any_adapter", config)

    assert hints.poll_interval == 0.1
