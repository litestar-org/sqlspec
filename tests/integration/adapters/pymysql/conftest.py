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


@pytest.fixture(scope="function")
def pymysql_transaction_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create PyMySQL config for transaction testing (autocommit=False)."""
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
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
            "test_table_pymysql",
            "data_types_test_pymysql",
            "user_profiles_pymysql",
            "test_parameter_conversion_pymysql",
            "transaction_test_pymysql",
            "concurrent_test_pymysql",
            "arrow_users_pymysql",
            "arrow_table_test_pymysql",
            "arrow_batch_test_pymysql",
            "arrow_params_test_pymysql",
            "arrow_empty_test_pymysql",
            "arrow_null_test_pymysql",
            "arrow_polars_test_pymysql",
            "arrow_large_test_pymysql",
            "arrow_types_test_pymysql",
            "arrow_json_test_pymysql",
            "driver_feature_test_pymysql",
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
