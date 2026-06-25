"""mysql-connector LOAD DATA LOCAL INFILE storage-bridge coverage (sync + async)."""

import tempfile
from collections.abc import AsyncGenerator, Generator

import pyarrow as pa
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.mysqlconnector]


@pytest.fixture
def mysqlconnector_sync_infile_config(
    mysql_service: "MySQLService",
) -> "Generator[MysqlConnectorSyncConfig, None, None]":
    config = MysqlConnectorSyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "allow_local_infile": True,
            "allow_local_infile_in_path": tempfile.gettempdir(),
        },
        driver_features={"enable_local_infile_bulk_load": True},
    )
    with config.provide_session() as driver:
        driver.execute("SET GLOBAL local_infile = 1")
    yield config
    with config.provide_session() as driver:
        driver.execute("SET GLOBAL local_infile = 0")


@pytest.fixture
async def mysqlconnector_async_infile_config(
    mysql_service: "MySQLService",
) -> "AsyncGenerator[MysqlConnectorAsyncConfig, None]":
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "allow_local_infile": True,
            "allow_local_infile_in_path": tempfile.gettempdir(),
        },
        driver_features={"enable_local_infile_bulk_load": True},
    )
    async with config.provide_session() as driver:
        await driver.execute("SET GLOBAL local_infile = 1")
    yield config
    async with config.provide_session() as driver:
        await driver.execute("SET GLOBAL local_infile = 0")


def test_mysqlconnector_sync_load_from_arrow_local_infile(
    mysqlconnector_sync_infile_config: "MysqlConnectorSyncConfig",
) -> None:
    with mysqlconnector_sync_infile_config.provide_session() as driver:
        driver.execute("DROP TABLE IF EXISTS mysqlconnector_infile_sync")
        driver.execute(
            "CREATE TABLE mysqlconnector_infile_sync (id INT PRIMARY KEY, name VARCHAR(255), note VARCHAR(255))"
        )
        arrow_table = pa.table({"id": [1, 2, 3], "name": ["alpha", "with\ttab", "gamma"], "note": ["ok", None, "x"]})

        job = driver.load_from_arrow("mysqlconnector_infile_sync", arrow_table)
        assert job.telemetry["rows_processed"] == 3

        rows = driver.select("SELECT id, name, note FROM mysqlconnector_infile_sync ORDER BY id")
        assert len(rows) == 3
        assert rows[1]["name"] == "with\ttab"
        assert rows[1]["note"] is None
        driver.execute("DROP TABLE IF EXISTS mysqlconnector_infile_sync")


async def test_mysqlconnector_async_load_from_arrow_local_infile(
    mysqlconnector_async_infile_config: "MysqlConnectorAsyncConfig",
) -> None:
    async with mysqlconnector_async_infile_config.provide_session() as driver:
        await driver.execute("DROP TABLE IF EXISTS mysqlconnector_infile_async")
        await driver.execute(
            "CREATE TABLE mysqlconnector_infile_async (id INT PRIMARY KEY, name VARCHAR(255), note VARCHAR(255))"
        )
        arrow_table = pa.table({"id": [1, 2, 3], "name": ["alpha", "with\ttab", "gamma"], "note": ["ok", None, "x"]})

        job = await driver.load_from_arrow("mysqlconnector_infile_async", arrow_table)
        assert job.telemetry["rows_processed"] == 3

        rows = await driver.select("SELECT id, name, note FROM mysqlconnector_infile_async ORDER BY id")
        assert len(rows) == 3
        assert rows[1]["name"] == "with\ttab"
        assert rows[1]["note"] is None
        await driver.execute("DROP TABLE IF EXISTS mysqlconnector_infile_async")
