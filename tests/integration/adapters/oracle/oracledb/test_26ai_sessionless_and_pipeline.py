"""Native Oracle 26ai cross-checkout transactions and wire pipeline acceptance."""

import inspect
import os
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import oracledb
import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import StatementStack
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.exceptions import StackExecutionError, UniqueViolationError

pytestmark = pytest.mark.xdist_group("oracle")


async def _invoke(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    value = function(*args, **kwargs)
    return await value if inspect.isawaitable(value) else value


def _config(mode: str, service: OracleService) -> Any:
    return (OracleAsyncConfig if mode == "async" else OracleSyncConfig)(
        connection_config={
            "host": service.host,
            "port": service.port,
            "service_name": service.service_name,
            "user": service.user,
            "password": service.password,
            "min": 1,
            "max": 1,
        }
    )


@asynccontextmanager
async def _session(config: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
    if isinstance(config, OracleAsyncConfig):
        async with config.provide_session(**kwargs) as session:
            yield session
    else:
        with config.provide_session(**kwargs) as session:
            yield session


@asynccontextmanager
async def _transaction(driver: Any, transaction_id: bytes | None = None, **kwargs: Any) -> AsyncGenerator[bytes, None]:
    context = driver.sessionless_transaction(transaction_id, **kwargs)
    if isinstance(driver.connection, oracledb.AsyncConnection):
        async with context as txid:
            yield txid
    else:
        with context as txid:
            yield txid


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_sessionless_transaction_suspend_resume_commit_and_rollback(
    mode: str, oracle_26ai_service: OracleService
) -> None:
    config = _config(mode, oracle_26ai_service)
    table = f"sl_{uuid4().hex[:12]}"
    try:
        async with _session(config) as session:
            await _invoke(session.execute, f"CREATE TABLE {table} (id NUMBER PRIMARY KEY)")
            async with _transaction(session) as txid:
                await _invoke(session.execute, f"INSERT INTO {table} VALUES (:1)", (1,))
        async with _session(config) as observer:
            assert await _invoke(observer.select_value, f"SELECT COUNT(*) FROM {table}") == 0
        async with _session(config) as resumed:
            async with _transaction(resumed, txid, resume=True):
                await _invoke(resumed.commit)
        async with _session(config) as session:
            assert await _invoke(session.select_value, f"SELECT id FROM {table}") == 1
            with pytest.raises(ValueError, match="rollback"):
                async with _transaction(session):
                    await _invoke(session.execute, f"INSERT INTO {table} VALUES (:1)", (2,))
                    raise ValueError("rollback")
            assert await _invoke(session.select_value, f"SELECT COUNT(*) FROM {table}") == 1
            suspended = await _invoke(session.begin_sessionless_transaction)
            await _invoke(session.execute, f"INSERT INTO {table} VALUES (:1)", (3,), suspend_on_success=True)
            assert session.sessionless_transaction_id is None
            await _invoke(session.resume_sessionless_transaction, suspended)
            await _invoke(session.rollback)
    finally:
        async with _session(config) as session:
            await _invoke(session.execute, f"DROP TABLE {table} PURGE")
        await _invoke(config.close_pool)


async def test_native_pipeline_stops_before_nontransactional_sequence(oracle_26ai_service: OracleService) -> None:
    config = _config("async", oracle_26ai_service)
    table, sequence = f"pipe_{uuid4().hex[:12]}", f"seq_{uuid4().hex[:12]}"
    try:
        async with _session(config) as session:
            await session.execute(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, clob_col CLOB, blob_col BLOB)")
            await session.execute(f"CREATE SEQUENCE {sequence} START WITH 1 INCREMENT BY 1 NOCACHE")
            stack = (
                StatementStack()
                .push_execute(f"INSERT INTO {table} (id) VALUES (1)")
                .push_execute(f"INSERT INTO {table} (id) VALUES (1)")
                .push_execute(f"INSERT INTO {table} (id) VALUES ({sequence}.NEXTVAL + 100)")
            )
            with pytest.raises(StackExecutionError) as caught:
                await session.execute_stack(stack, continue_on_error=False)
            assert caught.value.operation_index == -1
            assert isinstance(caught.value.original_error, UniqueViolationError)
            assert await session.select_value(f"SELECT COUNT(*) FROM {table}") == 0
            assert await session.select_value(f"SELECT {sequence}.NEXTVAL FROM DUAL") == 1
            result = await session.execute_stack(stack, continue_on_error=True)
            assert len(result) == 3
            assert await session.select_value(f"SELECT COUNT(*) FROM {table}") == 2
            text, binary = "x" * 12000, b"a" * 6000
            lob_stack = (
                StatementStack()
                .push_execute(f"INSERT INTO {table} VALUES (:1,:2,:3)", (999, text, binary))
                .push_execute(f"SELECT clob_col, blob_col FROM {table} WHERE id = 999")
            )
            rows = await session.execute_stack(lob_stack)
            assert rows[1].result.data == [{"clob_col": text, "blob_col": binary}]
    finally:
        async with _session(config) as session:
            await session.execute(f"DROP TABLE {table} PURGE")
            await session.execute(f"DROP SEQUENCE {sequence}")
        await _invoke(config.close_pool)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_transaction_priority_checkout_and_driver(mode: str, oracle_26ai_service: OracleService) -> None:
    config = _config(mode, oracle_26ai_service)
    try:
        async with _session(config) as session:
            baseline = session.connection.transaction_priority
        async with _session(config, transaction_priority="HIGH") as session:
            assert session.connection.transaction_priority == oracledb.TransactionPriority.HIGH
            session.set_transaction_priority("LOW")
            assert session.connection.transaction_priority == oracledb.TransactionPriority.LOW
            await _invoke(session.execute, "ALTER SESSION SET TXN_PRIORITY = 'HIGH'")
            assert (
                await _invoke(session.select_value, "SELECT value FROM v$parameter WHERE name = 'txn_priority'")
                == "HIGH"
            )
        async with _session(config) as session:
            assert session.connection.transaction_priority == baseline
    finally:
        await _invoke(config.close_pool)


async def test_opentelemetry_trace_context_26ai_supported_vs_18c_unsupported(
    oracle_async_config: OracleAsyncConfig, oracle_18c_async_session: Any
) -> None:
    from opentelemetry.sdk.trace import TracerProvider

    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, TelemetryConfig, get_w3c_trace_context

    provider = TracerProvider()
    runtime = ObservabilityRuntime(
        ObservabilityConfig(telemetry=TelemetryConfig(enable_spans=True, provider_factory=lambda: provider))
    )
    config = oracle_async_config
    try:
        with provider.get_tracer(__name__).start_as_current_span("oracle-acceptance"):
            assert get_w3c_trace_context()[0] is not None
            oracle_18c_async_session.attach_observability(runtime)
            assert oracle_18c_async_session.connection.supports_opentelemetry_tracing is False
            assert await oracle_18c_async_session.select_value("SELECT 1 FROM DUAL") == 1
            async with _session(config) as session:
                session.attach_observability(runtime)
                supported = session.connection.supports_opentelemetry_tracing
                assert await session.select_value("SELECT 1 FROM DUAL") == 1
                if os.getenv("SQLSPEC_REQUIRE_ORACLE_OTEL"):
                    assert supported is True, session.connection.version
                elif not supported:
                    pytest.skip(f"Oracle {session.connection.version} does not advertise native OTel tracing")
    finally:
        await _invoke(config.close_pool)
        provider.shutdown()
