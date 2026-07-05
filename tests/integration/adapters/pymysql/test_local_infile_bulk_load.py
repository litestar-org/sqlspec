"""PyMySQL LOAD DATA LOCAL INFILE storage-bridge coverage."""

from collections.abc import Generator

import pyarrow as pa
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


@pytest.fixture
def pymysql_infile_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "allow_local_infile": True,
            "local_infile": True,
        },
        driver_features={"enable_local_infile_bulk_load": True},
    )
    with config.provide_session() as driver:
        driver.execute("SET GLOBAL local_infile = 1")
    yield config
    with config.provide_session() as driver:
        driver.execute("SET GLOBAL local_infile = 0")
    if config.connection_instance:
        config.close_pool()


def test_pymysql_load_from_arrow_local_infile(pymysql_infile_config: "PyMysqlConfig") -> None:
    with pymysql_infile_config.provide_session() as driver:
        driver.execute("DROP TABLE IF EXISTS pymysql_local_infile_test")
        driver.execute(
            "CREATE TABLE pymysql_local_infile_test (id INT PRIMARY KEY, name VARCHAR(255), note VARCHAR(255))"
        )
        arrow_table = pa.table({
            "id": [1, 2, 3],
            "name": ["alpha", "with\ttab", "gamma"],
            "note": ["ok", None, "back\\slash"],
        })

        job = driver.load_from_arrow("pymysql_local_infile_test", arrow_table)
        assert job.telemetry["rows_processed"] == 3

        rows = driver.select("SELECT id, name, note FROM pymysql_local_infile_test ORDER BY id")
        assert len(rows) == 3
        assert rows[1]["name"] == "with\ttab"
        assert rows[1]["note"] is None
        assert rows[2]["note"] == "back\\slash"

        driver.execute("DROP TABLE IF EXISTS pymysql_local_infile_test")
