"""Native MySQL reader sessions retain read-only state across checkouts."""

import inspect
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from pytest_databases.docker.mysql import provide_mysql_service

from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.exceptions import ReadOnlyTransactionError

if TYPE_CHECKING:
    from pytest_databases import DockerService
    from pytest_databases.docker.mysql import MySQLService
    from pytest_databases.types import XdistIsolationLevel

pytestmark = [pytest.mark.integration, pytest.mark.mysql, pytest.mark.xdist_group("mysql")]


@pytest.fixture(scope="session")
def mysql_service(
    docker_service: "DockerService",
    mysql_user: str,
    mysql_password: str,
    mysql_root_password: str,
    mysql_database: str,
    xdist_mysql_isolation_level: "XdistIsolationLevel",
) -> Generator["MySQLService", None, None]:
    """Enable real GTID execution tracking for the causal-wait acceptance tests."""
    with provide_mysql_service(
        docker_service,
        image="mysql:8.4",
        name="mysql-routing-gtid",
        isolation_level=xdist_mysql_isolation_level,
        platform=None,
        user=mysql_user,
        password=mysql_password,
        root_password=mysql_root_password,
        database=mysql_database,
        command=["--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--server-id=1"],
    ) as service:
        yield service


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@asynccontextmanager
async def _session(config: Any, *, read_only: bool = False) -> AsyncGenerator[Any]:
    context = config.provide_read_session() if read_only else config.provide_session()
    if config.is_async:
        async with context as session:
            yield session
    else:
        with context as session:
            yield session


@pytest.mark.parametrize(
    ("config_type", "package"),
    [
        (AsyncmyConfig, "asyncmy"),
        (AiomysqlConfig, "aiomysql"),
        (PyMysqlConfig, "pymysql"),
        (MysqlConnectorSyncConfig, "mysql.connector"),
        (MysqlConnectorAsyncConfig, "mysql.connector"),
    ],
)
async def test_mysql_reader_checkout_persists_readonly_and_gtid_wait(
    mysql_service: "MySQLService", config_type: type[Any], package: str
) -> None:
    pytest.importorskip(package)
    options = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "database": mysql_service.db,
        "autocommit": True,
    }
    if config_type is MysqlConnectorSyncConfig:
        options.update(pool_size=1, pool_name="routing_" + uuid4().hex[:12], use_pure=True)
    config = config_type(connection_config=options, replicas=[{"host": mysql_service.host}])
    table = "reader_" + uuid4().hex[:12]
    try:
        async with _session(config) as writer:
            await _resolve(writer.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)"))
            await _resolve(writer.execute(f"INSERT INTO {table} VALUES (%s)", (1,)))
        for _ in range(2):
            async with _session(config, read_only=True) as reader:
                assert await _resolve(reader.select_value(f"SELECT COUNT(*) FROM {table}")) == 1
                with pytest.raises(ReadOnlyTransactionError):
                    await _resolve(reader.execute(f"INSERT INTO {table} VALUES (%s)", (2,)))
                assert (
                    await _resolve(reader.wait_for_gtid_set("11111111-1111-1111-1111-111111111111:1", timeout=0.1))
                    is False
                )
                applied = await _resolve(reader.select_value("SELECT @@GLOBAL.gtid_executed"))
                assert await _resolve(reader.wait_for_gtid_set(str(applied), timeout=0.1)) is True
                with pytest.raises(ValueError, match="GTID"):
                    await _resolve(reader.wait_for_gtid_set("", timeout=0))
        async with _session(config) as writer:
            await _resolve(writer.execute(f"INSERT INTO {table} VALUES (%s)", (2,)))
            assert await _resolve(writer.select_value(f"SELECT COUNT(*) FROM {table}")) == 2
    finally:
        async with _session(config) as writer:
            await _resolve(writer.execute(f"DROP TABLE IF EXISTS {table}"))
        await _resolve(config.close_pool())
