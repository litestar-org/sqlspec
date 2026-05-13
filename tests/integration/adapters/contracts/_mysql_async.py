"""Helpers for MySQL-family async adapter contract tests."""

import inspect
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.aiomysql import default_statement_config as aiomysql_statement_config
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.asyncmy import default_statement_config as asyncmy_statement_config

MYSQL_ASYNC_ADAPTERS = [
    pytest.param("aiomysql", marks=pytest.mark.aiomysql, id="aiomysql"),
    pytest.param("asyncmy", marks=pytest.mark.asyncmy, id="asyncmy"),
]


def mysql_async_config_type(adapter: str) -> type[Any]:
    """Return the concrete config type for a MySQL async-family adapter."""
    if adapter == "aiomysql":
        return AiomysqlConfig
    return AsyncmyConfig


def mysql_async_driver_type(adapter: str) -> type[Any]:
    """Return the concrete driver type for a MySQL async-family adapter."""
    if adapter == "aiomysql":
        return AiomysqlDriver
    return AsyncmyDriver


def mysql_async_database_key(adapter: str) -> str:
    """Return the adapter-specific database-name connection key."""
    if adapter == "aiomysql":
        return "db"
    return "database"


def mysql_async_connection_config(
    adapter: str,
    *,
    host: str = "localhost",
    port: int = 3306,
    user: str = "test_user",
    password: str = "test_password",
    database: str = "test_db",
    **overrides: Any,
) -> dict[str, Any]:
    """Build an adapter-specific connection config without requiring a live service."""
    connection_config: dict[str, Any] = {"host": host, "port": port, "user": user, "password": password}
    connection_config[mysql_async_database_key(adapter)] = database
    connection_config.update(overrides)
    return connection_config


def mysql_async_config(
    adapter: str,
    mysql_service: MySQLService,
    *,
    autocommit: bool = True,
    minsize: int | None = None,
    maxsize: int | None = None,
    echo: bool | None = None,
    migration_config: dict[str, Any] | None = None,
    driver_features: dict[str, Any] | None = None,
    extension_config: dict[str, Any] | None = None,
) -> Any:
    """Build an async MySQL-family config for the requested adapter."""
    connection_config: dict[str, Any] = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "autocommit": autocommit,
    }
    if minsize is not None:
        connection_config["minsize"] = minsize
    if maxsize is not None:
        connection_config["maxsize"] = maxsize
    if echo is not None:
        connection_config["echo"] = echo

    connection_config[mysql_async_database_key(adapter)] = mysql_service.db

    if adapter == "aiomysql":
        return AiomysqlConfig(
            connection_config=connection_config,
            statement_config=aiomysql_statement_config,
            migration_config=migration_config,
            driver_features=driver_features,
            extension_config=extension_config,
        )

    return AsyncmyConfig(
        connection_config=connection_config,
        statement_config=asyncmy_statement_config,
        migration_config=migration_config,
        driver_features=driver_features,
        extension_config=extension_config,
    )


async def close_mysql_async_config(config: Any) -> None:
    """Close an async MySQL-family config without depending on the concrete pool type."""
    close_pool = getattr(config, "close_pool", None)
    if close_pool is None:
        return
    result = close_pool()
    if inspect.isawaitable(result):
        await result
