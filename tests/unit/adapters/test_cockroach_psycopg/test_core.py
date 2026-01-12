"""Unit tests for CockroachDB psycopg core module.

Tests cover:
- CockroachPsycopgRetryConfig dataclass and factory method
- is_retryable_error() function for SQLSTATE 40001 detection
- calculate_backoff_seconds() exponential backoff with jitter
"""

import pytest

from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    calculate_backoff_seconds,
    is_retryable_error,
)


@pytest.mark.xdist_group("cockroachdb")
class TestCockroachPsycopgRetryConfig:
    """Tests for CockroachPsycopgRetryConfig dataclass."""

    def test_default_values(self) -> None:
        """Default config should have sensible retry defaults."""
        config = CockroachPsycopgRetryConfig()
        assert config.max_retries == 10
        assert config.base_delay_ms == 50.0
        assert config.max_delay_ms == 5000.0
        assert config.enable_logging is True

    def test_custom_values(self) -> None:
        """Config should accept custom values."""
        config = CockroachPsycopgRetryConfig(
            max_retries=5, base_delay_ms=100.0, max_delay_ms=2000.0, enable_logging=False
        )
        assert config.max_retries == 5
        assert config.base_delay_ms == 100.0
        assert config.max_delay_ms == 2000.0
        assert config.enable_logging is False

    def test_from_features_with_empty_dict(self) -> None:
        """from_features with empty dict should return defaults."""
        config = CockroachPsycopgRetryConfig.from_features({})
        assert config.max_retries == 10
        assert config.base_delay_ms == 50.0
        assert config.max_delay_ms == 5000.0
        assert config.enable_logging is True

    def test_from_features_with_custom_values(self) -> None:
        """from_features should extract values from driver features."""
        features = {
            "max_retries": 3,
            "retry_delay_base_ms": 25.0,
            "retry_delay_max_ms": 1000.0,
            "enable_retry_logging": False,
        }
        config = CockroachPsycopgRetryConfig.from_features(features)
        assert config.max_retries == 3
        assert config.base_delay_ms == 25.0
        assert config.max_delay_ms == 1000.0
        assert config.enable_logging is False

    def test_from_features_type_coercion(self) -> None:
        """from_features should coerce string values to appropriate types."""
        features = {"max_retries": "5", "retry_delay_base_ms": "100", "retry_delay_max_ms": "3000"}
        config = CockroachPsycopgRetryConfig.from_features(features)
        assert config.max_retries == 5
        assert config.base_delay_ms == 100.0
        assert config.max_delay_ms == 3000.0

    def test_frozen_dataclass(self) -> None:
        """Config should be immutable (frozen dataclass)."""
        config = CockroachPsycopgRetryConfig()
        with pytest.raises(AttributeError):
            config.max_retries = 5  # type: ignore[misc]


@pytest.mark.xdist_group("cockroachdb")
class TestIsRetryableError:
    """Tests for is_retryable_error function."""

    def test_sqlstate_40001_is_retryable(self) -> None:
        """SQLSTATE 40001 (serialization failure) should be retryable."""

        class MockErrorWith40001(BaseException):
            sqlstate = "40001"

        assert is_retryable_error(MockErrorWith40001()) is True

    def test_other_sqlstate_not_retryable(self) -> None:
        """Other SQLSTATEs should not be retryable."""

        class MockErrorWithOtherState(BaseException):
            sqlstate = "23505"  # unique_violation

        assert is_retryable_error(MockErrorWithOtherState()) is False

    def test_error_without_sqlstate_not_retryable(self) -> None:
        """Errors without sqlstate attribute should not be retryable."""
        assert is_retryable_error(ValueError("test")) is False
        assert is_retryable_error(RuntimeError("test")) is False

    def test_none_sqlstate_not_retryable(self) -> None:
        """Errors with None sqlstate should not be retryable."""

        class MockErrorWithNone(BaseException):
            sqlstate: str | None = None

        assert is_retryable_error(MockErrorWithNone()) is False


@pytest.mark.xdist_group("cockroachdb")
class TestCalculateBackoffSeconds:
    """Tests for calculate_backoff_seconds function."""

    def test_first_attempt_base_delay(self) -> None:
        """First attempt (0) should use base delay with jitter."""
        config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
        delay = calculate_backoff_seconds(0, config)
        # base = 100 * 2^0 = 100ms, with jitter up to 100ms more
        # So delay should be between 0.1s and 0.2s (100-200ms)
        assert 0.0 <= delay <= 0.2

    def test_exponential_growth(self) -> None:
        """Delays should grow exponentially with attempt number."""
        config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=10000.0)

        # Run multiple times to account for jitter
        delays = []
        for _ in range(10):
            delay_0 = calculate_backoff_seconds(0, config)
            delay_1 = calculate_backoff_seconds(1, config)
            delay_2 = calculate_backoff_seconds(2, config)
            delays.append((delay_0, delay_1, delay_2))

        # Average should show exponential growth pattern
        # attempt 0: base = 100ms
        # attempt 1: base = 200ms
        # attempt 2: base = 400ms
        avg_delay_0 = sum(d[0] for d in delays) / len(delays)
        avg_delay_1 = sum(d[1] for d in delays) / len(delays)
        avg_delay_2 = sum(d[2] for d in delays) / len(delays)

        # Each should roughly double (with some variance from jitter)
        assert avg_delay_1 > avg_delay_0
        assert avg_delay_2 > avg_delay_1

    def test_respects_max_delay(self) -> None:
        """Delay should not exceed max_delay_ms."""
        config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=500.0)
        # High attempt number would normally produce huge delay
        delay = calculate_backoff_seconds(10, config)
        # Should be capped at 500ms = 0.5s
        assert delay <= 0.5

    def test_returns_seconds(self) -> None:
        """Delay should be returned in seconds, not milliseconds."""
        config = CockroachPsycopgRetryConfig(base_delay_ms=1000.0, max_delay_ms=5000.0)
        delay = calculate_backoff_seconds(0, config)
        # 1000ms base with up to 1000ms jitter = max 2000ms = 2.0s
        assert delay <= 2.0

    def test_jitter_variation(self) -> None:
        """Multiple calls should produce different delays due to jitter."""
        config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
        delays = [calculate_backoff_seconds(1, config) for _ in range(20)]
        # With jitter, we should see some variation
        unique_delays = set(delays)
        # Should have multiple unique values (not all identical)
        assert len(unique_delays) > 1
