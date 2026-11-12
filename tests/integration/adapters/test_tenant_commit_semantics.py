"""Tenant leases keep each adapter's native session commit behavior."""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.utils.tenant import TenantContext


@dataclass(frozen=True)
class CommitCase:
    """One tenant-enabled adapter configuration and its SQL dialect details."""

    service: str
    build: Callable[[Any, bool], Any]
    text_type: str = "VARCHAR(32)"
    read_hint: str = ""
    setup_sql: tuple[str, ...] = ()


def _postgres(config_type: str) -> Callable[[Any, bool], Any]:
    def build(service: Any, tenant: bool) -> Any:
        from tests.integration.fixtures.postgres import _postgres_connection_config, _postgres_conninfo, _psqlpy_dsn

        features: dict[str, Any] = {"tenant_strategy": "rls"} if tenant else {}
        if config_type == "asyncpg":
            from sqlspec.adapters.asyncpg import AsyncpgConfig

            return AsyncpgConfig(connection_config=_postgres_connection_config(service), driver_features=features)
        if config_type == "psqlpy":
            from sqlspec.adapters.psqlpy import PsqlpyConfig

            return PsqlpyConfig(connection_config={"dsn": _psqlpy_dsn(service)}, driver_features=features)
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

        psycopg_type = PsycopgSyncConfig if config_type == "psycopg_sync" else PsycopgAsyncConfig
        return psycopg_type(connection_config={"conninfo": _postgres_conninfo(service)}, driver_features=features)

    return build


def _cockroach(config_type: str) -> Callable[[Any, bool], Any]:
    def build(service: Any, tenant: bool) -> Any:
        from tests.integration.fixtures.cockroach import _cockroach_asyncpg_connection_config, _cockroach_conninfo

        features: dict[str, Any] = {"tenant_strategy": "rls"} if tenant else {}
        if config_type == "asyncpg":
            from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

            return CockroachAsyncpgConfig(
                connection_config=_cockroach_asyncpg_connection_config(service), driver_features=features
            )
        from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig

        config_cls = CockroachPsycopgSyncConfig if config_type == "psycopg_sync" else CockroachPsycopgAsyncConfig
        return config_cls(connection_config={"conninfo": _cockroach_conninfo(service)}, driver_features=features)

    return build


def _oracle(asynchronous: bool) -> Callable[[Any, bool], Any]:
    def build(service: Any, tenant: bool) -> Any:
        from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
        from tests.integration.fixtures.oracle import _oracle_pool_params

        config_cls = OracleAsyncConfig if asynchronous else OracleSyncConfig
        features: dict[str, Any] = {"tenant_strategy": "identifier"} if tenant else {}
        return config_cls(
            connection_config={**_oracle_pool_params(service), "min": 1, "max": 2}, driver_features=features
        )

    return build


def _mssql(config_type: str) -> Callable[[Any, bool], Any]:
    def build(service: Any, tenant: bool) -> Any:
        from tests.integration.fixtures.mssql import _mssql_connection_config, _mssql_python_connection_config

        features: dict[str, Any] = {"tenant_strategy": "session_context"} if tenant else {}
        if config_type == "mssql_python":
            from sqlspec.adapters.mssql_python import MssqlPythonConfig

            return MssqlPythonConfig(
                connection_config=_mssql_python_connection_config(service), driver_features=features
            )
        from sqlspec.adapters.pymssql import PymssqlConfig

        return PymssqlConfig(connection_config=_mssql_connection_config(service), driver_features=features)

    return build


def _db2(asynchronous: bool) -> Callable[[Any, bool], Any]:
    def build(service: Any, tenant: bool) -> Any:
        from sqlspec.adapters.db2 import Db2AsyncConfig, Db2SyncConfig

        config_cls = Db2AsyncConfig if asynchronous else Db2SyncConfig
        features: dict[str, Any] = {"tenant_strategy": "rcac"} if tenant else {}
        connection_config = {
            "database": service.db_name,
            "hostname": service.host,
            "port": service.port,
            "protocol": "TCPIP",
            "user": service.username,
            "password": service.password,
        }
        return config_cls(connection_config=connection_config, driver_features=features)

    return build


_DB2_SETUP = ("CREATE OR REPLACE VARIABLE APP.CURRENT_TENANT_ID VARCHAR(256) DEFAULT NULL",)
_DB2 = pytest.mark.xdist_group("db2")
_POSTGRES = pytest.mark.xdist_group("postgres")
_COCKROACH = pytest.mark.xdist_group("cockroachdb")
_ORACLE = pytest.mark.xdist_group("oracle")
_MSSQL = pytest.mark.xdist_group("mssql")

CASES = [
    pytest.param(CommitCase("postgres_service", _postgres("asyncpg")), id="asyncpg", marks=_POSTGRES),
    pytest.param(CommitCase("postgres_service", _postgres("psqlpy")), id="psqlpy", marks=_POSTGRES),
    pytest.param(CommitCase("postgres_service", _postgres("psycopg_sync")), id="psycopg_sync", marks=_POSTGRES),
    pytest.param(CommitCase("postgres_service", _postgres("psycopg_async")), id="psycopg_async", marks=_POSTGRES),
    pytest.param(CommitCase("cockroachdb_service", _cockroach("asyncpg")), id="cockroach_asyncpg", marks=_COCKROACH),
    pytest.param(
        CommitCase("cockroachdb_service", _cockroach("psycopg_sync")), id="cockroach_psycopg_sync", marks=_COCKROACH
    ),
    pytest.param(
        CommitCase("cockroachdb_service", _cockroach("psycopg_async")), id="cockroach_psycopg_async", marks=_COCKROACH
    ),
    pytest.param(
        CommitCase("oracle_23ai_service", _oracle(False), text_type="VARCHAR2(32)"), id="oracledb_sync", marks=_ORACLE
    ),
    pytest.param(
        CommitCase("oracle_23ai_service", _oracle(True), text_type="VARCHAR2(32)"), id="oracledb_async", marks=_ORACLE
    ),
    pytest.param(
        CommitCase("mssql_service", _mssql("mssql_python"), read_hint=" WITH (READPAST)"),
        id="mssql_python",
        marks=_MSSQL,
    ),
    pytest.param(
        CommitCase("mssql_service", _mssql("pymssql"), read_hint=" WITH (READPAST)"), id="pymssql", marks=_MSSQL
    ),
    pytest.param(CommitCase("db2_service", _db2(False), setup_sql=_DB2_SETUP), id="db2_sync", marks=_DB2),
    pytest.param(CommitCase("db2_service", _db2(True), setup_sql=_DB2_SETUP), id="db2_async", marks=_DB2),
]


async def _invoke(function: Any, *args: Any, **kwargs: Any) -> Any:
    result = function(*args, **kwargs)
    return await result if isawaitable(result) else result


@asynccontextmanager
async def _session(config: Any) -> "AsyncIterator[Any]":
    context = config.provide_session()
    if isinstance(context, AbstractAsyncContextManager):
        async with context as session:
            yield session
    else:
        with context as session:
            yield session


async def _write(config: Any, table: str, marker: str, *, fail: bool, explicit: bool) -> None:
    with TenantContext.scope("tenant-a"):
        try:
            async with _session(config) as session:
                if explicit:
                    await _invoke(session.begin)
                await _invoke(session.execute, f"INSERT INTO {table} (marker) VALUES (:marker)", {"marker": marker})
                if fail:
                    msg = "application failure"
                    raise RuntimeError(msg)
        except RuntimeError as exc:
            if not fail:
                raise
            assert str(exc) == "application failure"


async def _persisted(case: CommitCase, service: Any, table: str, marker: str) -> bool:
    reader = case.build(service, False)
    try:
        async with _session(reader) as session:
            count = await _invoke(
                session.select_value,
                f"SELECT COUNT(*) FROM {table}{case.read_hint} WHERE marker = :marker",
                {"marker": marker},
            )
            await _invoke(session.rollback)
            return bool(count)
    finally:
        await _invoke(reader.close_pool)


@pytest.mark.parametrize("explicit", [True, False], ids=["explicit_begin", "implicit_transaction"])
@pytest.mark.parametrize("fail", [False, True], ids=["clean_exit", "error_exit"])
@pytest.mark.parametrize("case", CASES)
async def test_tenant_session_exit_matches_native_commit_behavior(
    case: CommitCase, fail: bool, explicit: bool, request: pytest.FixtureRequest
) -> None:
    service = request.getfixturevalue(case.service)
    table = f"tenant_commit_{uuid4().hex[:10]}"
    admin = case.build(service, False)
    try:
        async with _session(admin) as session:
            for statement in case.setup_sql:
                await _invoke(session.execute, statement)
            await _invoke(session.execute, f"CREATE TABLE {table} (marker {case.text_type})")
            await _invoke(session.commit)
        outcomes: dict[str, bool] = {}
        for mode in ("native", "tenant"):
            config = case.build(service, mode == "tenant")
            try:
                await _write(config, table, mode, fail=fail, explicit=explicit)
                outcomes[mode] = await _persisted(case, service, table, mode)
            finally:
                await _invoke(config.close_pool)
        assert outcomes["tenant"] == outcomes["native"]
        if fail and explicit:
            assert outcomes == {"native": False, "tenant": False}
    finally:
        async with _session(admin) as session:
            await _invoke(session.execute, f"DROP TABLE {table}")
            await _invoke(session.commit)
        await _invoke(admin.close_pool)
