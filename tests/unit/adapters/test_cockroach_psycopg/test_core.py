# pyright: reportArgumentType=false, reportIncompatibleMethodOverride=false
"""Unit tests for CockroachDB psycopg core module.

Tests cover:
- CockroachPsycopgRetryConfig slots class and factory method
- is_retryable_error() function for SQLSTATE 40001 detection
- calculate_backoff_seconds() exponential backoff with jitter
"""

from typing import TYPE_CHECKING, cast

import pytest

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgRetryConfig
from sqlspec.adapters.cockroach_psycopg.core import calculate_backoff_seconds, is_retryable_error
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.core import SQL
from sqlspec.driver import ExecutionResult

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_psycopg._typing import CockroachAsyncConnection, CockroachSyncConnection


def test_cockroach_psycopg_retry_config_default_values() -> None:
    """Default config should have sensible retry defaults."""
    config = CockroachPsycopgRetryConfig()
    assert config.max_retries == 10
    assert config.base_delay_ms == 50.0
    assert config.max_delay_ms == 5000.0
    assert config.enable_logging is True


def test_cockroach_psycopg_retry_config_custom_values() -> None:
    """Config should accept custom values."""
    config = CockroachPsycopgRetryConfig(max_retries=5, base_delay_ms=100.0, max_delay_ms=2000.0, enable_logging=False)
    assert config.max_retries == 5
    assert config.base_delay_ms == 100.0
    assert config.max_delay_ms == 2000.0
    assert config.enable_logging is False


def test_cockroach_psycopg_retry_config_from_features_with_empty_dict() -> None:
    """from_features with empty dict should return defaults."""
    config = CockroachPsycopgRetryConfig.from_features({})
    assert config.max_retries == 10
    assert config.base_delay_ms == 50.0
    assert config.max_delay_ms == 5000.0
    assert config.enable_logging is True


def test_cockroach_psycopg_retry_config_from_features_with_custom_values() -> None:
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


def test_cockroach_psycopg_retry_config_from_features_type_coercion() -> None:
    """from_features should coerce string values to appropriate types."""
    features = {"max_retries": "5", "retry_delay_base_ms": "100", "retry_delay_max_ms": "3000"}
    config = CockroachPsycopgRetryConfig.from_features(features)
    assert config.max_retries == 5
    assert config.base_delay_ms == 100.0
    assert config.max_delay_ms == 3000.0


def test_cockroach_psycopg_retry_config_slots_class_is_mutable() -> None:
    """Config should keep slots while allowing runtime updates."""
    config = CockroachPsycopgRetryConfig()
    config.max_retries = 5
    assert config.max_retries == 5
    assert CockroachPsycopgRetryConfig.__slots__ == ("base_delay_ms", "enable_logging", "max_delay_ms", "max_retries")


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
    config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
    delay = calculate_backoff_seconds(0, config)
    assert 0.0 <= delay <= 0.2


def test_calculate_backoff_seconds_exponential_growth() -> None:
    """Delays should grow exponentially with attempt number."""
    config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=10000.0)
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
    config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=500.0)
    delay = calculate_backoff_seconds(10, config)
    assert delay <= 0.5


def test_calculate_backoff_seconds_returns_seconds() -> None:
    """Delay should be returned in seconds, not milliseconds."""
    config = CockroachPsycopgRetryConfig(base_delay_ms=1000.0, max_delay_ms=5000.0)
    delay = calculate_backoff_seconds(0, config)
    assert delay <= 2.0


def test_calculate_backoff_seconds_jitter_variation() -> None:
    """Multiple calls should produce different delays due to jitter."""
    config = CockroachPsycopgRetryConfig(base_delay_ms=100.0, max_delay_ms=5000.0)
    delays = [calculate_backoff_seconds(1, config) for _ in range(20)]
    unique_delays = set(delays)
    assert len(unique_delays) > 1


def _sync_connection() -> "CockroachSyncConnection":
    return cast("CockroachSyncConnection", object())


def _async_connection() -> "CockroachAsyncConnection":
    return cast("CockroachAsyncConnection", object())


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


class _RecordingCockroachPsycopgSyncDriver(CockroachPsycopgSyncDriver):
    def __init__(self) -> None:
        super().__init__(connection=_sync_connection(), driver_features={"enable_auto_retry": False})
        self.calls: list[str] = []

    def _dispatch_execute_many_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = statement
        self.calls.append("many")
        return _execution_result("many")

    def _dispatch_execute_script_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = statement
        self.calls.append("script")
        return _execution_result("script")


class _RecordingCockroachPsycopgAsyncDriver(CockroachPsycopgAsyncDriver):
    def __init__(self) -> None:
        super().__init__(connection=_async_connection(), driver_features={"enable_auto_retry": False})
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


class _SyncRetryConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _AsyncRetryConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.autocommit_values: list[bool] = []

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value
        self.autocommit_values.append(value)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _RetryingCockroachPsycopgSyncDriver(CockroachPsycopgSyncDriver):
    def __init__(self, connection: _SyncRetryConnection) -> None:
        super().__init__(
            connection=cast("CockroachSyncConnection", connection),
            driver_features={
                "enable_auto_retry": True,
                "enable_retry_logging": False,
                "max_retries": 1,
                "retry_delay_base_ms": 0.0,
                "retry_delay_max_ms": 0.0,
            },
        )
        self.calls = 0

    def _connection_in_transaction(self) -> bool:
        return False

    def _dispatch_execute_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = cursor, statement
        self.calls += 1
        if self.calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return _execution_result("retried")


class _RetryingCockroachPsycopgAsyncDriver(CockroachPsycopgAsyncDriver):
    def __init__(self, connection: _AsyncRetryConnection) -> None:
        super().__init__(
            connection=cast("CockroachAsyncConnection", connection),
            driver_features={
                "enable_auto_retry": True,
                "enable_retry_logging": False,
                "max_retries": 1,
                "retry_delay_base_ms": 0.0,
                "retry_delay_max_ms": 0.0,
            },
        )
        self.calls = 0

    def _connection_in_transaction(self) -> bool:
        return False

    async def _dispatch_execute_impl(self, cursor: object, statement: SQL) -> ExecutionResult:
        _ = cursor, statement
        self.calls += 1
        if self.calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return _execution_result("retried")


def test_driver_cockroach_psycopg_sync_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgSyncDriver()
    result = driver.dispatch_execute_many(object(), SQL("SELECT 1"))
    assert result.cursor_result == "many"
    assert driver.calls == ["many"]


def test_driver_cockroach_psycopg_sync_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgSyncDriver()
    result = driver.dispatch_execute_script(object(), SQL("SELECT 1"))
    assert result.cursor_result == "script"
    assert driver.calls == ["script"]


@pytest.mark.anyio
async def test_driver_cockroach_psycopg_async_non_retry_execute_many_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgAsyncDriver()
    result = await driver.dispatch_execute_many(object(), SQL("SELECT 1"))
    assert result.cursor_result == "many"
    assert driver.calls == ["many"]


@pytest.mark.anyio
async def test_driver_cockroach_psycopg_async_non_retry_execute_script_uses_impl_wrapper() -> None:
    driver = _RecordingCockroachPsycopgAsyncDriver()
    result = await driver.dispatch_execute_script(object(), SQL("SELECT 1"))
    assert result.cursor_result == "script"
    assert driver.calls == ["script"]


def test_driver_cockroach_psycopg_sync_dispatch_execute_does_not_retry_inside_statement() -> None:
    connection = _SyncRetryConnection()
    driver = _RetryingCockroachPsycopgSyncDriver(connection)

    with pytest.raises(_RetryableCockroachError):
        driver.dispatch_execute(object(), SQL("SELECT 1"))

    assert driver.calls == 1
    assert connection.rollbacks == 0


def test_driver_cockroach_psycopg_sync_transaction_retry_replays_whole_callback() -> None:
    connection = _SyncRetryConnection()
    driver = _RetryingCockroachPsycopgSyncDriver(connection)
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return "ok"

    result = driver.run_transaction_with_retry(operation)

    assert result == "ok"
    assert calls == 2
    assert connection.rollbacks == 1
    assert connection.commits == 1


@pytest.mark.anyio
async def test_driver_cockroach_psycopg_async_dispatch_execute_does_not_retry_inside_statement() -> None:
    connection = _AsyncRetryConnection()
    driver = _RetryingCockroachPsycopgAsyncDriver(connection)

    with pytest.raises(_RetryableCockroachError):
        await driver.dispatch_execute(object(), SQL("SELECT 1"))

    assert driver.calls == 1
    assert connection.rollbacks == 0


@pytest.mark.anyio
async def test_driver_cockroach_psycopg_async_transaction_retry_replays_whole_callback() -> None:
    connection = _AsyncRetryConnection()
    driver = _RetryingCockroachPsycopgAsyncDriver(connection)
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
    assert connection.rollbacks == 1
    assert connection.commits == 1


def test_driver_cockroach_psycopg_sync_data_dictionary_uses_parent_slot() -> None:
    driver = CockroachPsycopgSyncDriver(connection=_sync_connection())
    assert isinstance(driver.data_dictionary, CockroachPsycopgSyncDataDictionary)


def test_driver_cockroach_psycopg_async_data_dictionary_uses_parent_slot() -> None:
    driver = CockroachPsycopgAsyncDriver(connection=_async_connection())
    assert isinstance(driver.data_dictionary, CockroachPsycopgAsyncDataDictionary)
