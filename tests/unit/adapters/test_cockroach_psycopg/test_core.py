# pyright: reportArgumentType=false
"""Unit tests for CockroachDB psycopg retry helpers."""

import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import errors as pg_errors
from psycopg import pq

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgRetryConfig
from sqlspec.adapters.cockroach_psycopg.core import (
    build_connection_config,
    build_statement_config,
    calculate_backoff_seconds,
    create_mapped_exception,
    is_retryable_error,
    resolve_follower_read_options,
)
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.exceptions import ConnectionTimeoutError, ReadOnlyTransactionError
from sqlspec.utils.routing import RoutingContext


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
    assert len(set(delays)) > 1


@pytest.mark.parametrize("pin", [False, True])
def test_primary_consistency_overrides_explicit_follower_bound(pin: bool) -> None:
    scope = RoutingContext.scope() if pin else RoutingContext.primary("orders")
    with scope:
        if pin:
            RoutingContext.mark_write("orders")
        assert resolve_follower_read_options(
            {}, follower_reads=True, staleness="'-10s'", read_only_session=True, bind_key="orders"
        ) == (False, None, True)


def test_follower_defaults_and_context_bounds() -> None:
    assert resolve_follower_read_options({}, follower_reads=True) == (True, "follower_read_timestamp()", False)
    with RoutingContext.replica("orders", max_staleness_seconds=3):
        assert resolve_follower_read_options({}, bind_key="orders") == (True, "'-3s'", False)
        assert resolve_follower_read_options({}, follower_reads=True, staleness="'-8s'", bind_key="orders") == (
            True,
            "'-8s'",
            False,
        )
    with RoutingContext.replica("orders", max_staleness_seconds=0):
        assert resolve_follower_read_options({}, follower_reads=True, bind_key="orders") == (False, None, False)


def test_native_read_only_exception_keeps_cause() -> None:
    class NativeReadOnlyError(Exception):
        sqlstate = "25006"

    native = NativeReadOnlyError("read-only transaction")
    mapped = create_mapped_exception(native)
    assert isinstance(mapped, ReadOnlyTransactionError)
    assert mapped.__cause__ is native


def test_create_mapped_exception_signature_parity() -> None:
    """CockroachDB psycopg create_mapped_exception exposes the standard logger keyword argument."""
    sig = inspect.signature(create_mapped_exception)
    assert list(sig.parameters.keys()) == ["error", "logger"]
    assert sig.parameters["logger"].kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["logger"].default is None

    mock_logger = MagicMock()
    mapped = create_mapped_exception(pg_errors.CannotConnectNow("cannot connect now"), logger=mock_logger)
    assert isinstance(mapped, ConnectionTimeoutError)


def test_build_connection_config_merges_managed_options() -> None:
    """Managed options replace existing copies while preserving unrelated options."""
    built = build_connection_config({
        "dsn": "postgresql://localhost:26257/defaultdb",
        "options": "--application_name=app -c statement_timeout=1000",
        "statement_timeout": 2500,
        "default_transaction_use_follower_reads": True,
    })
    assert built["conninfo"] == "postgresql://localhost:26257/defaultdb"
    assert built["options"] == (
        "--application_name=app -c statement_timeout=2500 -c default_transaction_use_follower_reads=on"
    )


def test_cockroach_psycopg_sync_load_from_storage_rejects_read_only_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync load_from_storage fails closed on read-only sessions even on idle autocommit connections."""
    conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    driver = CockroachPsycopgSyncDriver(
        conn,
        statement_config=build_statement_config(),
        driver_features={"enable_native_storage": True, "read_only_session": True},
    )
    execute_native = MagicMock()
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachPsycopgSyncDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/k")
        ),
    )

    with pytest.raises(ReadOnlyTransactionError):
        driver.load_from_storage("items", "s3://b/k.parquet", file_format="parquet")
    execute_native.assert_not_called()


@pytest.mark.anyio
async def test_cockroach_psycopg_async_load_from_storage_rejects_read_only_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async load_from_storage fails closed on read-only sessions even on idle autocommit connections."""
    conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    driver = CockroachPsycopgAsyncDriver(
        conn,
        statement_config=build_statement_config(),
        driver_features={"enable_native_storage": True, "read_only_session": True},
    )
    execute_native = AsyncMock()
    monkeypatch.setattr(CockroachPsycopgAsyncDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachPsycopgAsyncDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/k")
        ),
    )

    with pytest.raises(ReadOnlyTransactionError):
        await driver.load_from_storage("items", "s3://b/k.parquet", file_format="parquet")
    execute_native.assert_not_awaited()


@pytest.mark.parametrize(
    "sql", ["WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d", "SELECT nextval('items_seq')"]
)
@pytest.mark.parametrize("read_only_session", [False, True])
def test_cockroach_psycopg_sync_select_to_storage_guards_unsafe_selects(
    monkeypatch: pytest.MonkeyPatch, sql: str, read_only_session: bool
) -> None:
    """Unsafe SELECT statements bypass native EXPORT on sync driver and raise on read-only sessions."""
    conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    driver = CockroachPsycopgSyncDriver(
        conn,
        statement_config=build_statement_config(),
        driver_features={"enable_native_storage": True, "read_only_session": read_only_session},
    )
    execute_native = MagicMock()
    fallback_job = object()
    monkeypatch.setattr(CockroachPsycopgSyncDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachPsycopgSyncDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/out")
        ),
    )
    monkeypatch.setattr(PsycopgSyncDriver, "select_to_storage", MagicMock(return_value=fallback_job))

    if read_only_session:
        with pytest.raises(ReadOnlyTransactionError):
            driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
    else:
        job: Any = driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
        assert job is fallback_job
    execute_native.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql", ["WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d", "SELECT nextval('items_seq')"]
)
@pytest.mark.parametrize("read_only_session", [False, True])
async def test_cockroach_psycopg_async_select_to_storage_guards_unsafe_selects(
    monkeypatch: pytest.MonkeyPatch, sql: str, read_only_session: bool
) -> None:
    """Unsafe SELECT statements bypass native EXPORT on async driver and raise on read-only sessions."""
    conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    driver = CockroachPsycopgAsyncDriver(
        conn,
        statement_config=build_statement_config(),
        driver_features={"enable_native_storage": True, "read_only_session": read_only_session},
    )
    execute_native = AsyncMock()
    fallback_job = object()
    monkeypatch.setattr(CockroachPsycopgAsyncDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachPsycopgAsyncDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/out")
        ),
    )
    monkeypatch.setattr(PsycopgAsyncDriver, "select_to_storage", AsyncMock(return_value=fallback_job))

    if read_only_session:
        with pytest.raises(ReadOnlyTransactionError):
            await driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
    else:
        job: Any = await driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
        assert job is fallback_job
    execute_native.assert_not_awaited()
