# pyright: reportArgumentType=false, reportIncompatibleMethodOverride=false
"""Unit tests for CockroachDB AsyncPG core module.

Tests cover:
- CockroachAsyncpgRetryConfig slots class and factory method
- is_retryable_error() function for SQLSTATE 40001 detection
- calculate_backoff_seconds() exponential backoff with jitter
"""

from typing import TYPE_CHECKING, cast

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgRetryConfig
from sqlspec.adapters.cockroach_asyncpg.core import calculate_backoff_seconds, is_retryable_error
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver
from sqlspec.core import SQL
from sqlspec.driver import ExecutionResult

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgConnection


def test_cockroach_asyncpg_retry_config_default_values() -> None:
    """Default config should have sensible retry defaults."""
    config = CockroachAsyncpgRetryConfig()
    assert config.max_retries == 10
    assert config.base_delay_ms == 50.0
    assert config.max_delay_ms == 5000.0
    assert config.enable_logging is True


def test_cockroach_asyncpg_retry_config_custom_values() -> None:
    """Config should accept custom values."""
    config = CockroachAsyncpgRetryConfig(max_retries=5, base_delay_ms=100.0, max_delay_ms=2000.0, enable_logging=False)
    assert config.max_retries == 5
    assert config.base_delay_ms == 100.0
    assert config.max_delay_ms == 2000.0
    assert config.enable_logging is False


def test_cockroach_asyncpg_retry_config_from_features_with_empty_dict() -> None:
    """from_features with empty dict should return defaults."""
    config = CockroachAsyncpgRetryConfig.from_features({})
    assert config.max_retries == 10
    assert config.base_delay_ms == 50.0
    assert config.max_delay_ms == 5000.0
    assert config.enable_logging is True


def test_cockroach_asyncpg_retry_config_from_features_with_custom_values() -> None:
    """from_features should extract values from driver features."""
    features = {
        "max_retries": 3,
        "retry_delay_base_ms": 25.0,
        "retry_delay_max_ms": 1000.0,
        "enable_retry_logging": False,
    }
    config = CockroachAsyncpgRetryConfig.from_features(features)
    assert config.max_retries == 3
    assert config.base_delay_ms == 25.0
    assert config.max_delay_ms == 1000.0
    assert config.enable_logging is False


def test_cockroach_asyncpg_retry_config_from_features_type_coercion() -> None:
    """from_features should coerce string values to appropriate types."""
    features = {"max_retries": "5", "retry_delay_base_ms": "100", "retry_delay_max_ms": "3000"}
    config = CockroachAsyncpgRetryConfig.from_features(features)
    assert config.max_retries == 5
    assert config.base_delay_ms == 100.0
    assert config.max_delay_ms == 3000.0


def test_cockroach_asyncpg_retry_config_slots_class_is_mutable() -> None:
    """Config should keep slots while allowing runtime updates."""
    config = CockroachAsyncpgRetryConfig()
    config.max_retries = 5
    assert config.max_retries == 5
    assert CockroachAsyncpgRetryConfig.__slots__ == ("base_delay_ms", "enable_logging", "max_delay_ms", "max_retries")


def test_is_retryable_error_sqlstate_40001_is_retryable() -> None:
    """SQLSTATE 40001 (serialization failure) should be retryable."""

    class MockErrorWith40001(BaseException):
        sqlstate = "40001"

    assert is_retryable_error(MockErrorWith40001()) is True


def test_is_retryable_error_other_sqlstate_not_retryable() -> None:
    """Other SQLSTATEs should not be retryable."""

    class MockErrorWithOtherState(BaseException):
        sqlstate = "23505"

    assert is_retryable_error(MockErrorWithOtherState()) is False


def test_is_retryable_error_error_without_sqlstate_not_retryable() -> None:
    """Errors without sqlstate attribute should not be retryable."""
    assert is_retryable_error(ValueError("test")) is False
    assert is_retryable_error(RuntimeError("test")) is False


def test_is_retryable_error_none_sqlstate_not_retryable() -> None:
    """Errors with None sqlstate should not be retryable."""

    class MockErrorWithNone(BaseException):
        sqlstate: str | None = None

    assert is_retryable_error(MockErrorWithNone()) is False


def test_calculate_backoff_seconds_first_attempt_base_delay() -> None:
    """First attempt (0) should use base delay with jitter."""
    config = CockroachAsyncpgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
    delay = calculate_backoff_seconds(0, config)
    assert 0.0 <= delay <= 0.2


def test_calculate_backoff_seconds_exponential_growth() -> None:
    """Delays should grow exponentially with attempt number."""
    config = CockroachAsyncpgRetryConfig(base_delay_ms=100.0, max_delay_ms=10000.0)
    delays = []
    for _ in range(10):
        delay_0 = calculate_backoff_seconds(0, config)
        delay_1 = calculate_backoff_seconds(1, config)
        delay_2 = calculate_backoff_seconds(2, config)
        delays.append((delay_0, delay_1, delay_2))
    avg_delay_0 = sum(d[0] for d in delays) / len(delays)
    avg_delay_1 = sum(d[1] for d in delays) / len(delays)
    avg_delay_2 = sum(d[2] for d in delays) / len(delays)
    assert avg_delay_1 > avg_delay_0
    assert avg_delay_2 > avg_delay_1


def test_calculate_backoff_seconds_respects_max_delay() -> None:
    """Delay should not exceed max_delay_ms."""
    config = CockroachAsyncpgRetryConfig(base_delay_ms=100.0, max_delay_ms=500.0)
    delay = calculate_backoff_seconds(10, config)
    assert delay <= 0.5


def test_calculate_backoff_seconds_returns_seconds() -> None:
    """Delay should be returned in seconds, not milliseconds."""
    config = CockroachAsyncpgRetryConfig(base_delay_ms=1000.0, max_delay_ms=5000.0)
    delay = calculate_backoff_seconds(0, config)
    assert delay <= 2.0


def test_calculate_backoff_seconds_jitter_variation() -> None:
    """Multiple calls should produce different delays due to jitter."""
    config = CockroachAsyncpgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
    delays = [calculate_backoff_seconds(1, config) for _ in range(20)]
    unique_delays = set(delays)
    assert len(unique_delays) > 1


def _connection() -> "CockroachAsyncpgConnection":
    return cast("CockroachAsyncpgConnection", object())


def _execution_result(label: str) -> ExecutionResult:
    return ExecutionResult(
        cursor_result=label,
        rowcount_override=None,
        special_data=None,
        selected_data=None,
        column_names=None,
        data_row_count=None,
        statement_count=None,
        successful_statements=None,
        is_script_result=False,
        is_select_result=False,
        is_many_result=False,
    )


class _RecordingCockroachAsyncpgDriver(CockroachAsyncpgDriver):
    def __init__(self) -> None:
        super().__init__(connection=_connection(), driver_features={"enable_auto_retry": False})
        self.calls: list[str] = []

    async def _dispatch_execute_many_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = statement
        self.calls.append("many")
        return _execution_result("many")

    async def _dispatch_execute_script_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = statement
        self.calls.append("script")
        return _execution_result("script")


class _RetryableCockroachError(Exception):
    sqlstate = "40001"


class _RetryConnection:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.in_transaction = False

    async def execute(self, sql: str) -> None:
        self.commands.append(sql)
        if sql == "BEGIN":
            self.in_transaction = True
        elif sql in {"COMMIT", "ROLLBACK"}:
            self.in_transaction = False

    def is_in_transaction(self) -> bool:
        return self.in_transaction


class _RetryingCockroachAsyncpgDriver(CockroachAsyncpgDriver):
    def __init__(self, connection: _RetryConnection) -> None:
        super().__init__(
            connection=cast("CockroachAsyncpgConnection", connection),
            driver_features={
                "enable_auto_retry": True,
                "enable_retry_logging": False,
                "max_retries": 1,
                "retry_delay_base_ms": 0.0,
                "retry_delay_max_ms": 0.0,
            },
        )
        self.calls = 0

    async def _dispatch_execute_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = cursor, statement
        self.calls += 1
        if self.calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return _execution_result("retried")


@pytest.mark.anyio
async def test_driver_cockroach_asyncpg_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachAsyncpgDriver()
    result = await driver.dispatch_execute_many(object(), SQL("SELECT 1"))
    assert result.cursor_result == "many"
    assert driver.calls == ["many"]


@pytest.mark.anyio
async def test_driver_cockroach_asyncpg_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachAsyncpgDriver()
    result = await driver.dispatch_execute_script(object(), SQL("SELECT 1"))
    assert result.cursor_result == "script"
    assert driver.calls == ["script"]


@pytest.mark.anyio
async def test_driver_cockroach_asyncpg_dispatch_execute_does_not_retry_inside_statement() -> None:
    connection = _RetryConnection()
    driver = _RetryingCockroachAsyncpgDriver(connection)

    with pytest.raises(_RetryableCockroachError):
        await driver.dispatch_execute(object(), SQL("SELECT 1"))

    assert driver.calls == 1
    assert connection.commands == []


@pytest.mark.anyio
async def test_driver_cockroach_asyncpg_transaction_retry_replays_whole_callback() -> None:
    connection = _RetryConnection()
    driver = CockroachAsyncpgDriver(
        connection=cast("CockroachAsyncpgConnection", connection),
        driver_features={
            "enable_auto_retry": True,
            "enable_retry_logging": False,
            "max_retries": 1,
            "retry_delay_base_ms": 0.0,
            "retry_delay_max_ms": 0.0,
        },
    )
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return "ok"

    result = await driver.run_transaction_with_retry(operation)

    assert result == "ok"
    assert calls == 2
    assert connection.commands == ["BEGIN", "ROLLBACK", "BEGIN", "COMMIT"]
