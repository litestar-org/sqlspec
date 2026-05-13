"""Shared helpers for adapter contract matrix tests."""

import inspect
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.aiomysql import default_statement_config as aiomysql_statement_config
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncmy import default_statement_config as asyncmy_statement_config
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.mysqlconnector import default_statement_config as mysqlconnector_statement_config
from sqlspec.adapters.oracledb import OracleAsyncConfig, OraclePoolParams, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.adapters.pymysql import default_statement_config as pymysql_statement_config
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig

ASYNC_ADAPTERS = {
    "aiosqlite",
    "aiomysql",
    "asyncmy",
    "mysqlconnector-async",
    "asyncpg",
    "psqlpy",
    "psycopg-async",
    "oracle-async",
    "cockroach-asyncpg",
    "cockroach-psycopg-async",
}

SPANNER_LOCAL_SKIP = pytest.mark.skipif(
    os.getenv("CI") != "true" and os.getenv("SQLSPEC_ENABLE_SPANNER_TESTS") != "1",
    reason=("Spanner contract tests run by default in CI; local runs require SQLSPEC_ENABLE_SPANNER_TESTS=1"),
)


async def maybe_await(value: Any) -> Any:
    """Await inspectable values and return regular values unchanged."""
    if inspect.isawaitable(value):
        return await value
    return value


async def close_config(config: Any) -> None:
    """Close adapter config pools used by contract tests."""
    close_pool = getattr(config, "close_pool", None)
    if close_pool is not None:
        await maybe_await(close_pool())
    elif (connection := getattr(config, "connection_instance", None)) is not None:
        close = getattr(connection, "close", None)
        if close is not None:
            await maybe_await(close())

    if hasattr(config, "connection_instance"):
        config.connection_instance = None


@asynccontextmanager
async def provide_driver(adapter: str, config: Any, *, write: bool = False) -> AsyncGenerator[Any, None]:
    """Yield a sync or async driver for a contract adapter."""
    if adapter == "spanner" and write:
        with config.provide_write_session() as driver:
            yield driver
        return

    try:
        manager = config.provide_session()
        if adapter in ASYNC_ADAPTERS or hasattr(manager, "__aenter__"):
            async with manager as driver:
                yield driver
        else:
            with manager as driver:
                yield driver
    finally:
        await close_config(config)


def postgres_url(request: pytest.FixtureRequest, *, scheme: str = "postgresql") -> str:
    """Build a PostgreSQL URL from the shared service fixture."""
    service = request.getfixturevalue("postgres_service")
    return f"{scheme}://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


def cockroach_conninfo(request: pytest.FixtureRequest) -> str:
    """Build a CockroachDB psycopg connection string."""
    service = request.getfixturevalue("cockroachdb_service")
    return f"host={service.host} port={service.port} user=root dbname={service.database} sslmode=disable"


def _mysql_connection_config(request: pytest.FixtureRequest, adapter: str) -> dict[str, Any]:
    service = request.getfixturevalue("mysql_service")
    config: dict[str, Any] = {
        "host": service.host,
        "port": service.port,
        "user": service.user,
        "password": service.password,
        "autocommit": True,
    }
    if adapter == "aiomysql":
        config["db"] = service.db
    else:
        config["database"] = service.db
    if adapter in {"mysqlconnector-sync", "mysqlconnector-async"}:
        config["use_pure"] = True
    return config


def _oracle_pool_params(request: pytest.FixtureRequest, *, async_pool: bool = False) -> OraclePoolParams:
    service = request.getfixturevalue("oracle_23ai_service")
    params = OraclePoolParams(
        host=service.host,
        port=service.port,
        service_name=service.service_name,
        user=service.user,
        password=service.password,
    )
    if async_pool:
        params["min"] = 1
        params["max"] = 5
    return params


def _spanner_config(request: pytest.FixtureRequest, extension_config: dict[str, Any] | None) -> SpannerSyncConfig:
    service = request.getfixturevalue("spanner_service")
    connection = request.getfixturevalue("spanner_connection")
    instance = connection.instance(service.instance_name)
    if not instance.exists():
        config_name = f"{connection.project_name}/instanceConfigs/emulator-config"
        instance = connection.instance(service.instance_name, configuration_name=config_name)
        instance.create().result(300)

    database = instance.database(service.database_name)
    if not database.exists():
        database.create().result(300)

    return SpannerSyncConfig(
        connection_config={
            "project": service.project,
            "instance_id": service.instance_name,
            "database_id": service.database_name,
            "credentials": service.credentials,
            "client_options": {"api_endpoint": f"{service.host}:{service.port}"},
            "min_sessions": 1,
            "max_sessions": 5,
        },
        extension_config=extension_config,
    )


def make_config(
    adapter: str,
    request: pytest.FixtureRequest,
    tmp_path: Path,
    *,
    migration_config: dict[str, Any] | None = None,
    extension_config: dict[str, Any] | None = None,
) -> Any:
    """Create an adapter config for contract tests."""
    if adapter == "sqlite":
        return SqliteConfig(
            connection_config={"database": str(tmp_path / "contract.sqlite")},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "aiosqlite":
        return AiosqliteConfig(
            connection_config={"database": str(tmp_path / "contract-aiosqlite.sqlite")},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "adbc-postgres":
        return AdbcConfig(
            connection_config={
                "uri": postgres_url(request, scheme="postgres"),
                "driver_name": "adbc_driver_postgresql",
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "adbc-duckdb":
        return AdbcConfig(
            connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "duckdb":
        return DuckDBConfig(
            connection_config={"database": str(tmp_path / "contract.duckdb")},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "aiomysql":
        return AiomysqlConfig(
            connection_config=_mysql_connection_config(request, adapter),
            statement_config=aiomysql_statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "asyncmy":
        return AsyncmyConfig(
            connection_config=_mysql_connection_config(request, adapter),
            statement_config=asyncmy_statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "mysqlconnector-sync":
        return MysqlConnectorSyncConfig(
            connection_config=_mysql_connection_config(request, adapter),
            statement_config=mysqlconnector_statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "mysqlconnector-async":
        return MysqlConnectorAsyncConfig(
            connection_config=_mysql_connection_config(request, adapter),
            statement_config=mysqlconnector_statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "pymysql":
        return PyMysqlConfig(
            connection_config=_mysql_connection_config(request, adapter),
            statement_config=pymysql_statement_config,
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "asyncpg":
        service = request.getfixturevalue("postgres_service")
        return AsyncpgConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": service.user,
                "password": service.password,
                "database": service.database,
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "psqlpy":
        return PsqlpyConfig(
            connection_config={"dsn": postgres_url(request, scheme="postgres"), "max_db_pool_size": 5},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "psycopg-sync":
        return PsycopgSyncConfig(
            connection_config={"conninfo": postgres_url(request)},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "psycopg-async":
        return PsycopgAsyncConfig(
            connection_config={"conninfo": postgres_url(request)},
            pool_config={"min_size": 1},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "oracle-sync":
        return OracleSyncConfig(
            connection_config=_oracle_pool_params(request),
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "oracle-async":
        return OracleAsyncConfig(
            connection_config=_oracle_pool_params(request, async_pool=True),
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "spanner":
        return _spanner_config(request, extension_config)
    if adapter == "cockroach-asyncpg":
        service = request.getfixturevalue("cockroachdb_service")
        return CockroachAsyncpgConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": "root",
                "password": "",
                "database": service.database,
                "ssl": None,
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "cockroach-psycopg-sync":
        return CockroachPsycopgSyncConfig(
            connection_config={"conninfo": cockroach_conninfo(request)},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "cockroach-psycopg-async":
        return CockroachPsycopgAsyncConfig(
            connection_config={"conninfo": cockroach_conninfo(request)},
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "bigquery":
        service = request.getfixturevalue("bigquery_service")
        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials

        return BigQueryConfig(
            connection_config={
                "project": service.project,
                "dataset_id": f"`{service.project}`.`{service.dataset}`",
                "client_options": ClientOptions(api_endpoint=f"http://{service.host}:{service.port}"),
                "credentials": AnonymousCredentials(),
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    msg = f"Unhandled adapter contract config: {adapter}"
    raise ValueError(msg)
