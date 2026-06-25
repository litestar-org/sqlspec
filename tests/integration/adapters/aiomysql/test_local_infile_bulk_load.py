"""aiomysql LOAD DATA LOCAL INFILE storage-bridge coverage."""

from collections.abc import AsyncGenerator

import pyarrow as pa
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.aiomysql]


@pytest.fixture
async def aiomysql_infile_config(mysql_service: "MySQLService") -> "AsyncGenerator[AiomysqlConfig, None]":
    config = AiomysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "db": mysql_service.db,
            "autocommit": True,
            "enable_local_infile": True,
        },
        driver_features={"enable_local_infile_bulk_load": True},
    )
    async with config.provide_session() as driver:
        await driver.execute("SET GLOBAL local_infile = 1")
    yield config
    async with config.provide_session() as driver:
        await driver.execute("SET GLOBAL local_infile = 0")
    if config.connection_instance:
        await config.close_pool()


async def test_aiomysql_load_from_arrow_local_infile(aiomysql_infile_config: "AiomysqlConfig") -> None:
    async with aiomysql_infile_config.provide_session() as driver:
        await driver.execute("DROP TABLE IF EXISTS aiomysql_local_infile_test")
        await driver.execute(
            "CREATE TABLE aiomysql_local_infile_test (id INT PRIMARY KEY, name VARCHAR(255), note VARCHAR(255))"
        )
        arrow_table = pa.table({
            "id": [1, 2, 3],
            "name": ["alpha", "with\ttab", "gamma"],
            "note": ["ok", None, "back\\slash"],
        })

        job = await driver.load_from_arrow("aiomysql_local_infile_test", arrow_table)
        assert job.telemetry["rows_processed"] == 3

        rows = await driver.select("SELECT id, name, note FROM aiomysql_local_infile_test ORDER BY id")
        assert len(rows) == 3
        assert rows[1]["name"] == "with\ttab"
        assert rows[1]["note"] is None
        assert rows[2]["note"] == "back\\slash"

        await driver.execute("DROP TABLE IF EXISTS aiomysql_local_infile_test")
