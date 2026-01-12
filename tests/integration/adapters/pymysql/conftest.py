"""Shared fixtures for PyMySQL integration tests."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver, default_statement_config


@pytest.fixture(scope="session")
def pymysql_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create PyMySQL config for testing."""
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        statement_config=default_statement_config,
    )
    yield config

    if config.connection_instance:
        config.close_pool()


@pytest.fixture
def pymysql_driver(pymysql_config: PyMysqlConfig) -> "Generator[PyMysqlDriver, None, None]":
    """Create PyMySQL driver instance for testing."""
    with pymysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
def pymysql_clean_driver(pymysql_config: PyMysqlConfig) -> "Generator[PyMysqlDriver, None, None]":
    """Create PyMySQL driver with clean database state."""
    with pymysql_config.provide_session() as driver:
        driver.execute("SET sql_notes = 0")
        cleanup_tables = [
            "test_table",
            "data_types_test",
            "user_profiles",
            "test_parameter_conversion",
            "transaction_test",
            "concurrent_test",
            "arrow_users",
            "arrow_table_test",
            "arrow_batch_test",
            "arrow_params_test",
            "arrow_empty_test",
            "arrow_null_test",
            "arrow_polars_test",
            "arrow_large_test",
            "arrow_types_test",
            "arrow_json_test",
        ]

        for table in cleanup_tables:
            driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        cleanup_procedures = ["test_procedure", "simple_procedure"]

        for proc in cleanup_procedures:
            driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        driver.execute("SET sql_notes = 1")

        yield driver

        driver.execute("SET sql_notes = 0")

        for table in cleanup_tables:
            driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        for proc in cleanup_procedures:
            driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        driver.execute("SET sql_notes = 1")
