"""Shared SQL Server integration fixtures."""

import contextlib
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymssql.litestar import PymssqlStore

__all__ = (
    "MSSQL_MIGRATION_LOGIN",
    "MSSQL_MIGRATION_PASSWORD",
    "arrow_odbc_mssql_config",
    "contract_mssql_python_store",
    "contract_pymssql_store",
    "ensure_mssql_migration_login",
    "events_config_mssql_python",
    "events_config_pymssql",
    "mssql_migration_connection_config",
    "mssql_python_config",
    "mssql_python_connection_config",
    "mssql_python_migration_connection_config",
    "pymssql_config",
    "pymssql_connection_config",
)

MSSQL_MIGRATION_LOGIN = "sqlspec_migrator"
MSSQL_MIGRATION_PASSWORD = "Password123!"

_STORE_EXTENSION_CONFIG: "dict[str, Any]" = {"litestar": {"session_table": "litestar_contract_sessions"}}


def _mssql_events_migration_config(tmp_path: Path, suffix: str) -> "dict[str, Any]":
    migrations = tmp_path / f"migrations_{suffix}"
    migrations.mkdir()
    return {
        "script_location": str(migrations),
        "include_extensions": ["events"],
        "version_table_name": f"ddl_migrations_{suffix}",
    }


def ensure_mssql_migration_login(mssql_service: "MSSQLService") -> None:
    """Ensure SQL Server migration login and database user exist with db_owner membership."""
    import pymssql

    conn = pymssql.connect(
        server=mssql_service.host,
        port=str(mssql_service.port),
        user=mssql_service.user,
        password=mssql_service.password,
        database=mssql_service.database,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '{MSSQL_MIGRATION_LOGIN}')
                BEGIN
                    CREATE LOGIN [{MSSQL_MIGRATION_LOGIN}] WITH PASSWORD = '{MSSQL_MIGRATION_PASSWORD}', CHECK_POLICY = OFF;
                END
                IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '{MSSQL_MIGRATION_LOGIN}')
                BEGIN
                    CREATE USER [{MSSQL_MIGRATION_LOGIN}] FOR LOGIN [{MSSQL_MIGRATION_LOGIN}];
                    ALTER ROLE [db_owner] ADD MEMBER [{MSSQL_MIGRATION_LOGIN}];
                END
                """
            )
    finally:
        conn.close()


def _mssql_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    return {
        "server": mssql_service.host,
        "port": mssql_service.port,
        "database": mssql_service.database,
        "user": mssql_service.user,
        "password": mssql_service.password,
    }


def _mssql_python_connection_config(mssql_service: "MSSQLService", *, autocommit: bool = True) -> "dict[str, Any]":
    connection_config = _mssql_connection_config(mssql_service)
    connection_config.update({
        "encrypt": False,
        "trust_server_certificate": True,
        "autocommit": autocommit,
        "pool_enabled": False,
    })
    return connection_config


def _arrow_odbc_connection_config(mssql_service: "MSSQLService") -> "dict[str, str]":
    return {"connection_string": mssql_service.connection_string}


@pytest.fixture(scope="session")
def mssql_python_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide shared mssql-python connection parameters."""
    return _mssql_python_connection_config(mssql_service, autocommit=False)


@pytest.fixture(scope="session")
def mssql_python_config(mssql_python_connection_config: "dict[str, Any]") -> "Generator[MssqlPythonConfig, None, None]":
    """Provide a session-scoped mssql-python configuration."""
    config = MssqlPythonConfig(connection_config=dict(mssql_python_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
def pymssql_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide shared pymssql connection parameters."""
    return _mssql_connection_config(mssql_service)


@pytest.fixture(scope="session")
def pymssql_config(pymssql_connection_config: "dict[str, Any]") -> "Generator[PymssqlConfig, None, None]":
    """Provide a session-scoped pymssql configuration."""
    config = PymssqlConfig(connection_config=dict(pymssql_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
def arrow_odbc_mssql_config(mssql_service: "MSSQLService") -> "Generator[ArrowOdbcConfig, None, None]":
    """Provide a session-scoped arrow-odbc SQL Server configuration."""
    config = ArrowOdbcConfig(
        connection_config=_arrow_odbc_connection_config(mssql_service),
        driver_features={"dbms_name": "Microsoft SQL Server"},
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def events_config_mssql_python(mssql_service: "MSSQLService", tmp_path: Path) -> "Callable[..., Any]":
    """Build mssql-python event-channel configs for contract tests."""

    def make(*, extension_config: "dict[str, Any]", suffix: str) -> MssqlPythonConfig:
        return MssqlPythonConfig(
            connection_config=_mssql_python_connection_config(mssql_service),
            migration_config=_mssql_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_pymssql(mssql_service: "MSSQLService", tmp_path: Path) -> "Callable[..., Any]":
    """Build pymssql event-channel configs for contract tests."""

    def make(*, extension_config: "dict[str, Any]", suffix: str) -> PymssqlConfig:
        return PymssqlConfig(
            connection_config=_mssql_connection_config(mssql_service),
            migration_config=_mssql_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
async def contract_mssql_python_store(mssql_service: "MSSQLService") -> "AsyncGenerator[MssqlPythonStore, None]":
    """Provide a ready mssql-python Litestar store for contract tests."""
    config = MssqlPythonConfig(
        connection_config=_mssql_python_connection_config(mssql_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = MssqlPythonStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_pymssql_store(mssql_service: "MSSQLService") -> "AsyncGenerator[PymssqlStore, None]":
    """Provide a ready pymssql Litestar store for contract tests."""
    config = PymssqlConfig(
        connection_config=_mssql_connection_config(mssql_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = PymssqlStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture(scope="session")
def mssql_migration_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide SQL Server connection parameters using the dedicated migration login."""
    ensure_mssql_migration_login(mssql_service)
    config = _mssql_connection_config(mssql_service)
    config["user"] = MSSQL_MIGRATION_LOGIN
    config["password"] = MSSQL_MIGRATION_PASSWORD
    return config


@pytest.fixture(scope="session")
def mssql_python_migration_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide mssql-python connection parameters using the dedicated migration login."""
    ensure_mssql_migration_login(mssql_service)
    config = _mssql_python_connection_config(mssql_service, autocommit=False)
    config["user"] = MSSQL_MIGRATION_LOGIN
    config["password"] = MSSQL_MIGRATION_PASSWORD
    return config
