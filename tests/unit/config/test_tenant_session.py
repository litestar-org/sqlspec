"""Explicit tenant sessions preserve context across the complete session lifetime."""

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload


@pytest.fixture(
    params=[
        AsyncpgConfig,
        CockroachAsyncpgConfig,
        CockroachPsycopgAsyncConfig,
        CockroachPsycopgSyncConfig,
        MssqlPythonConfig,
        OracleAsyncConfig,
        OracleSyncConfig,
        PsqlpyConfig,
        PsycopgAsyncConfig,
        PsycopgSyncConfig,
        PymssqlConfig,
    ],
    ids=lambda cls: cls.__name__,
)
def tenant_config(request: pytest.FixtureRequest) -> Any:
    return request.param(
        connection_config={"pool_enabled": False} if request.param is MssqlPythonConfig else {},
        driver_features={"tenant_strategy": True},
    )


@pytest.mark.parametrize("failure", [None, "acquire", "prepare", "body", "release"])
async def test_explicit_tenant_covers_session_lifetime(
    tenant_config: Any, failure: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[str, TenantContextPayload | None]] = []
    payload = TenantContextPayload("tenant-a", {"region": "west"})
    error = RuntimeError("session failed")
    connection = MagicMock()
    connection.info.transaction_status = 0
    statement_config = StatementConfig(dialect="postgres")

    def record(stage: str) -> None:
        events.append((stage, TenantContext.get()))
        if stage == failure:
            raise error

    def acquire() -> Any:
        record("acquire")
        return connection

    def release(*args: Any, **kwargs: Any) -> None:
        record("release")

    def prepare(driver: Any) -> Any:
        record("prepare")
        assert driver.connection is connection
        assert driver.statement_config is statement_config
        return driver

    async def async_acquire() -> Any:
        return acquire()

    async def async_release(*args: Any, **kwargs: Any) -> None:
        release(*args, **kwargs)

    lease = SimpleNamespace(
        acquire=async_acquire if tenant_config.is_async else acquire,
        release=async_release if tenant_config.is_async else release,
    )
    monkeypatch.setattr(type(tenant_config), "_create_tenant_lease", lambda self, **kwargs: lease)
    monkeypatch.setattr(type(tenant_config), "_prepare_driver", lambda self, driver: prepare(driver))
    context = tenant_config.provide_session(tenant=payload, statement_config=statement_config)
    assert TenantContext.get() is None
    with TenantContext.scope("outer") as outer:
        try:
            if tenant_config.is_async:
                async with context:
                    record("body")
            else:
                with context:
                    record("body")
        except RuntimeError as caught:
            assert failure is not None
            assert caught is error
        else:
            assert failure is None
        assert TenantContext.get() is outer
    expected = ["acquire"]
    if failure != "acquire":
        expected.append("prepare")
        if failure != "prepare":
            expected.append("body")
        expected.append("release")
    assert events == [(stage, payload) for stage in expected]


@pytest.mark.parametrize("tenant", ["tenant-a", UUID(int=1), TenantContextPayload.bypass()])
@pytest.mark.parametrize("registry", [False, True])
async def test_explicit_tenant_normalizes_identity_and_supports_registry(
    tenant: str | UUID | TenantContextPayload, registry: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = AsyncpgConfig(driver_features={"tenant_strategy": True})
    seen: list[TenantContextPayload | None] = []

    async def acquire() -> Any:
        seen.append(TenantContext.get())
        return MagicMock()

    async def release(*args: Any, **kwargs: Any) -> None:
        seen.append(TenantContext.get())

    monkeypatch.setattr(
        config, "_create_tenant_lease", lambda **kwargs: SimpleNamespace(acquire=acquire, release=release)
    )
    context = SQLSpec().provide_session(config, tenant=tenant) if registry else config.provide_session(tenant=tenant)
    async with context:
        payload = TenantContext.get()
        assert payload == (tenant if isinstance(tenant, TenantContextPayload) else TenantContextPayload(tenant))
    assert seen == [payload, payload]
    assert TenantContext.get() is None


@pytest.mark.parametrize("phase", ["acquire", "body", "release"])
async def test_explicit_tenant_restores_after_cancellation(phase: str, monkeypatch: pytest.MonkeyPatch) -> None:
    config = AsyncpgConfig(driver_features={"tenant_strategy": True})
    entered = asyncio.Event()

    async def block(stage: str) -> None:
        assert TenantContext.get_tenant_id() == "tenant-a"
        if stage == phase:
            entered.set()
            await asyncio.Event().wait()

    async def acquire() -> Any:
        await block("acquire")
        return MagicMock()

    async def release(*args: Any, **kwargs: Any) -> None:
        await block("release")

    monkeypatch.setattr(
        config, "_create_tenant_lease", lambda **kwargs: SimpleNamespace(acquire=acquire, release=release)
    )

    async def run() -> None:
        with TenantContext.scope("outer") as outer:
            try:
                async with config.provide_session(tenant="tenant-a"):
                    await block("body")
            finally:
                assert TenantContext.get() is outer

    owner = asyncio.create_task(run())
    await entered.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner


@pytest.mark.parametrize(
    "config_type",
    [SqliteConfig, AiosqliteConfig, AdbcConfig, ArrowOdbcConfig, BigQueryConfig, SpannerSyncConfig, SpannerAsyncConfig],
)
async def test_explicit_tenant_requires_strategy(config_type: type[Any]) -> None:
    config = config_type()
    with TenantContext.scope("outer") as outer:
        with pytest.raises(ImproperConfigurationError, match="tenant_strategy"):
            if config.is_async:
                async with config.provide_session(tenant="tenant-a"):
                    pytest.fail("session acquired without a tenant strategy")
            else:
                with config.provide_session(tenant="tenant-a"):
                    pytest.fail("session acquired without a tenant strategy")
        assert TenantContext.get() is outer
