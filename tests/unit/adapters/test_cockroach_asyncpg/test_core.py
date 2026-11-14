# pyright: reportArgumentType=false
"""Unit tests for CockroachDB AsyncPG retry helpers."""

import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.core import default_statement_config, register_json_codecs
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgRetryConfig
from sqlspec.adapters.cockroach_asyncpg.core import (
    calculate_backoff_seconds,
    create_mapped_exception,
    is_retryable_error,
    resolve_follower_read_options,
)
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver, CockroachAsyncpgExceptionHandler
from sqlspec.exceptions import DatabaseConnectionError, QueryTimeoutError, ReadOnlyTransactionError
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.serializers import from_json, to_json


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


@pytest.mark.anyio
@pytest.mark.parametrize("scalar", ["123", "null", "true", '{"a":1}', "[1, 2]", ""])
async def test_cockroach_asyncpg_json_codecs_encode_string_scalars_as_json_strings(scalar: str) -> None:
    """CockroachDB AsyncPG shared JSON codecs encode string scalars via the configured serializer."""
    connection = AsyncMock()
    await register_json_codecs(connection, to_json, from_json)
    json_encoder = connection.set_type_codec.call_args_list[0].kwargs["encoder"]
    jsonb_encoder = connection.set_type_codec.call_args_list[1].kwargs["encoder"]
    assert json_encoder(scalar) == to_json(scalar).encode("utf-8")
    assert jsonb_encoder(scalar) == b"\x01" + to_json(scalar).encode("utf-8")


def test_create_mapped_exception_signature_parity() -> None:
    """CockroachDB AsyncPG create_mapped_exception exposes the standard logger keyword argument."""
    sig = inspect.signature(create_mapped_exception)
    assert list(sig.parameters.keys()) == ["error", "logger"]
    assert sig.parameters["logger"].kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["logger"].default is None

    mock_logger = MagicMock()
    err = TimeoutError("timed out")
    mapped = create_mapped_exception(err, logger=mock_logger)
    assert isinstance(mapped, QueryTimeoutError)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("exc", "expected_cls"),
    [
        (TimeoutError("connect timed out"), QueryTimeoutError),
        (OSError("connection reset by peer"), DatabaseConnectionError),
        (asyncpg.InterfaceError("connection is closed"), DatabaseConnectionError),
    ],
)
async def test_cockroach_exception_handler_maps_transport_and_interface_errors(
    exc: BaseException, expected_cls: type[Exception]
) -> None:
    """CockroachAsyncpgExceptionHandler mirrors AsyncpgExceptionHandler transport and interface error checks."""
    handler = CockroachAsyncpgExceptionHandler()
    async with handler:
        raise exc
    assert isinstance(handler.pending_exception, expected_cls)


@pytest.mark.anyio
async def test_cockroach_asyncpg_load_from_storage_rejects_read_only_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """load_from_storage fails closed on read-only sessions even on idle autocommit connections."""
    conn = SimpleNamespace(is_in_transaction=lambda: False)
    driver = CockroachAsyncpgDriver(
        conn,
        statement_config=default_statement_config,
        driver_features={"enable_native_storage": True, "read_only_session": True},
    )
    execute_native = AsyncMock()
    monkeypatch.setattr(CockroachAsyncpgDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachAsyncpgDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/k")
        ),
    )

    with pytest.raises(ReadOnlyTransactionError):
        await driver.load_from_storage("items", "s3://b/k.parquet", file_format="parquet")
    execute_native.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql", ["WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d", "SELECT nextval('items_seq')"]
)
@pytest.mark.parametrize("read_only_session", [False, True])
async def test_cockroach_asyncpg_select_to_storage_guards_unsafe_selects(
    monkeypatch: pytest.MonkeyPatch, sql: str, read_only_session: bool
) -> None:
    """Unsafe SELECT statements bypass native EXPORT and raise on read-only sessions."""
    conn = SimpleNamespace(is_in_transaction=lambda: False)
    driver = CockroachAsyncpgDriver(
        conn,
        statement_config=default_statement_config,
        driver_features={"enable_native_storage": True, "read_only_session": read_only_session},
    )
    execute_native = AsyncMock()
    fallback_job = object()
    monkeypatch.setattr(CockroachAsyncpgDriver, "_execute_native_storage", execute_native)
    monkeypatch.setattr(
        CockroachAsyncpgDriver,
        "_storage_pipeline",
        lambda _self: SimpleNamespace(
            resolve_destination=lambda _target: SimpleNamespace(protocol="s3", uri="s3://b/out")
        ),
    )
    monkeypatch.setattr(AsyncpgDriver, "select_to_storage", AsyncMock(return_value=fallback_job))

    if read_only_session:
        with pytest.raises(ReadOnlyTransactionError):
            await driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
    else:
        job: Any = await driver.select_to_storage(sql, "s3://b/out", format_hint="parquet")
        assert job is fallback_job
    execute_native.assert_not_awaited()
