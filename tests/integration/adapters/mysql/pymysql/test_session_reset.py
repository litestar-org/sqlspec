"""Live check that reset_session_on_release clears session state on a reused PyMySQL connection."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


def test_pymysql_reset_clears_temporary_table_and_session_setting(mysql_service: MySQLService) -> None:
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
        },
        driver_features={"reset_session_on_release": True},
    )
    try:
        with config.provide_session() as session:
            session.execute("CREATE TEMPORARY TABLE reset_probe (id INT)")
            session.execute("SET SESSION max_execution_time = 7777")
            first_connection_id = session.select_value("SELECT CONNECTION_ID()")
        with config.provide_session() as session:
            assert session.select_value("SELECT CONNECTION_ID()") == first_connection_id
            assert session.select_value("SELECT @@session.max_execution_time") != 7777
            tables = session.select("SELECT table_name FROM information_schema.tables WHERE table_name = 'reset_probe'")
            assert list(tables) == []
    finally:
        config.close_pool()
