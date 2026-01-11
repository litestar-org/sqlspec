"""Unit tests for EventRuntimeHints and hint resolution."""

import pytest

from sqlspec.extensions.events import EventRuntimeHints, get_runtime_hints


def test_event_runtime_hints_defaults() -> None:
    """EventRuntimeHints has sensible defaults."""
    hints = EventRuntimeHints()

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30
    assert hints.retention_seconds == 86_400
    assert hints.select_for_update is False
    assert hints.skip_locked is False
    assert hints.json_passthrough is False


def test_event_runtime_hints_custom_values() -> None:
    """EventRuntimeHints accepts custom values."""
    hints = EventRuntimeHints(
        poll_interval=0.5,
        lease_seconds=60,
        retention_seconds=3600,
        select_for_update=True,
        skip_locked=True,
        json_passthrough=True,
    )

    assert hints.poll_interval == 0.5
    assert hints.lease_seconds == 60
    assert hints.retention_seconds == 3600
    assert hints.select_for_update is True
    assert hints.skip_locked is True
    assert hints.json_passthrough is True


def test_event_runtime_hints_frozen() -> None:
    """EventRuntimeHints is immutable."""
    hints = EventRuntimeHints()

    with pytest.raises(AttributeError):
        hints.poll_interval = 2.0  # type: ignore[misc]


def test_get_runtime_hints_none_config() -> None:
    """get_runtime_hints returns defaults when config is None."""
    hints = get_runtime_hints(None, None)

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30


def test_get_runtime_hints_config_without_provider() -> None:
    """get_runtime_hints returns defaults for configs without hint provider."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"})
    hints = get_runtime_hints("sqlite", config)

    assert hints.poll_interval == 1.0
    assert hints.lease_seconds == 30


def test_get_runtime_hints_config_with_provider() -> None:
    """get_runtime_hints calls config's hint provider when available."""

    class FakeConfig:
        def get_event_runtime_hints(self):
            return EventRuntimeHints(poll_interval=0.25, lease_seconds=10)

    config = FakeConfig()
    hints = get_runtime_hints("fake", config)

    assert hints.poll_interval == 0.25
    assert hints.lease_seconds == 10


def test_get_runtime_hints_provider_returns_non_hints() -> None:
    """get_runtime_hints returns defaults if provider returns non-hints."""

    class FakeConfig:
        def get_event_runtime_hints(self):
            return {"poll_interval": 0.5}

    config = FakeConfig()
    hints = get_runtime_hints("fake", config)

    assert hints.poll_interval == 1.0


def test_get_runtime_hints_adapter_ignored_when_config_provided() -> None:
    """Adapter name is not used when config provides hints."""

    class FakeConfig:
        def get_event_runtime_hints(self):
            return EventRuntimeHints(poll_interval=0.1)

    config = FakeConfig()
    hints = get_runtime_hints("any_adapter", config)

    assert hints.poll_interval == 0.1
