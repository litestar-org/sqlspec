"""Unit tests for shared CockroachDB retry helpers."""

from dataclasses import FrozenInstanceError

import pytest

from sqlspec.adapters.cockroach._shared_core import CockroachRetryConfig, calculate_backoff_seconds, is_retryable_error

pytestmark = pytest.mark.xdist_group("cockroachdb")


def test_retry_config_defaults() -> None:
    """Default config should have sensible retry defaults."""
    config = CockroachRetryConfig()

    assert config.max_retries == 10
    assert config.base_delay_ms == 50.0
    assert config.max_delay_ms == 5000.0
    assert config.enable_logging is True


def test_retry_config_from_features() -> None:
    """Feature mappings should be coerced into retry config values."""
    config = CockroachRetryConfig.from_features({
        "max_retries": "5",
        "retry_delay_base_ms": "100",
        "retry_delay_max_ms": "3000",
        "enable_retry_logging": False,
    })

    assert config.max_retries == 5
    assert config.base_delay_ms == 100.0
    assert config.max_delay_ms == 3000.0
    assert config.enable_logging is False


def test_retry_config_is_frozen() -> None:
    """Retry config should be immutable."""
    config = CockroachRetryConfig()

    with pytest.raises(FrozenInstanceError):
        config.max_retries = 5  # type: ignore[misc]


def test_sqlstate_40001_is_retryable() -> None:
    """SQLSTATE 40001 should be retryable."""

    class MockErrorWith40001(BaseException):
        sqlstate = "40001"

    assert is_retryable_error(MockErrorWith40001()) is True


def test_other_sqlstate_is_not_retryable() -> None:
    """Other SQLSTATE values should not be retryable."""

    class MockErrorWithOtherState(BaseException):
        sqlstate = "23505"

    assert is_retryable_error(MockErrorWithOtherState()) is False


def test_missing_or_none_sqlstate_is_not_retryable() -> None:
    """Errors without SQLSTATE 40001 should not be retryable."""

    class MockErrorWithNone(BaseException):
        sqlstate: str | None = None

    assert is_retryable_error(ValueError("test")) is False
    assert is_retryable_error(MockErrorWithNone()) is False


def test_calculate_backoff_seconds_uses_exponential_jitter_and_cap() -> None:
    """Backoff should increase by attempt and stay under the configured cap."""
    config = CockroachRetryConfig(base_delay_ms=100.0, max_delay_ms=500.0)

    assert 0.0 <= calculate_backoff_seconds(0, config) <= 0.2
    assert calculate_backoff_seconds(10, config) <= 0.5


def test_calculate_backoff_seconds_varies_with_jitter() -> None:
    """Multiple calls should produce different delays due to jitter."""
    config = CockroachRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)

    delays = {calculate_backoff_seconds(1, config) for _ in range(20)}

    assert len(delays) > 1
