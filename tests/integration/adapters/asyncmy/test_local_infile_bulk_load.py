"""Asyncmy LOAD DATA LOCAL INFILE storage-bridge coverage."""

from collections.abc import AsyncGenerator

import pyarrow as pa
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.asyncmy]


@pytest.fixture
async def asyncmy_infile_config(mysql_service: "MySQLService") -> "AsyncGenerator[AsyncmyConfig, None]":
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "local_infile": True,
            "allow_local_infile": True,
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


async def test_asyncmy_load_from_arrow_local_infile(asyncmy_infile_config: "AsyncmyConfig") -> None:
    async with asyncmy_infile_config.provide_session() as driver:
        await driver.execute("DROP TABLE IF EXISTS asyncmy_local_infile_test")
        await driver.execute(
            "CREATE TABLE asyncmy_local_infile_test (id INT PRIMARY KEY, name VARCHAR(255), note VARCHAR(255))"
        )
        arrow_table = pa.table({
            "id": [1, 2, 3],
            "name": ["alpha", "with\ttab", "gamma"],
            "note": ["ok", None, "back\\slash"],
        })

        job = await driver.load_from_arrow("asyncmy_local_infile_test", arrow_table)
        assert job.telemetry["rows_processed"] == 3

        rows = await driver.select("SELECT id, name, note FROM asyncmy_local_infile_test ORDER BY id")
        assert len(rows) == 3
        assert rows[1]["name"] == "with\ttab"
        assert rows[1]["note"] is None
        assert rows[2]["note"] == "back\\slash"

        await driver.execute("DROP TABLE IF EXISTS asyncmy_local_infile_test")
