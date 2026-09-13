"""Integration tests for SQL Server default schema introspection."""

from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.pymssql import PymssqlConfig

pytestmark = [pytest.mark.mssql, pytest.mark.xdist_group("mssql")]


def test_mssql_python_introspects_connection_default_schema(mssql_migration_connection_config: dict[str, Any]) -> None:
    """mssql-python data dictionary uses connection default schema when schema is omitted."""
    schema = f"introspect_{uuid4().hex[:8]}"
    table = f"tbl_{uuid4().hex[:8]}"
    conn_config = dict(mssql_migration_connection_config)
    conn_config.update({"encrypt": False, "trust_server_certificate": True, "autocommit": False, "pool_enabled": False})
    config = MssqlPythonConfig(connection_config=conn_config)
    try:
        with config.provide_session() as driver:
            driver.execute_script(f"CREATE SCHEMA [{schema}];")
            driver.execute_script(f"CREATE TABLE [{schema}].[{table}] (id INT);")
            driver.commit()

            assert driver.data_dictionary.resolve_connection_schema(driver, None) == "dbo"

            driver.set_migration_session_schema(schema)
            try:
                assert driver.data_dictionary.resolve_connection_schema(driver, None) == schema
                tables = driver.data_dictionary.get_tables(driver)
                table_names = [t["table_name"] for t in tables]
                assert table in table_names
            finally:
                driver.reset_migration_session_schema()
        with config.provide_session() as verify:
            assert verify.select_value("SELECT SCHEMA_NAME()") == "dbo"
    finally:
        with config.provide_session() as driver:
            driver.execute_script(f"DROP TABLE IF EXISTS [{schema}].[{table}];")
            driver.execute_script(f"DROP SCHEMA IF EXISTS [{schema}];")
            driver.commit()
        config.close_pool()


def test_pymssql_introspects_connection_default_schema(mssql_migration_connection_config: dict[str, Any]) -> None:
    """pymssql data dictionary uses connection default schema when schema is omitted."""
    schema = f"introspect_{uuid4().hex[:8]}"
    table = f"tbl_{uuid4().hex[:8]}"
    config = PymssqlConfig(connection_config=dict(mssql_migration_connection_config))
    try:
        with config.provide_session() as driver:
            driver.execute_script(f"CREATE SCHEMA [{schema}];")
            driver.execute_script(f"CREATE TABLE [{schema}].[{table}] (id INT);")
            driver.commit()

            assert driver.data_dictionary.resolve_connection_schema(driver, None) == "dbo"

            driver.set_migration_session_schema(schema)
            try:
                assert driver.data_dictionary.resolve_connection_schema(driver, None) == schema
                tables = driver.data_dictionary.get_tables(driver)
                table_names = [t["table_name"] for t in tables]
                assert table in table_names
            finally:
                driver.reset_migration_session_schema()
        with config.provide_session() as verify:
            assert verify.select_value("SELECT SCHEMA_NAME()") == "dbo"
    finally:
        with config.provide_session() as driver:
            driver.execute_script(f"DROP TABLE IF EXISTS [{schema}].[{table}];")
            driver.execute_script(f"DROP SCHEMA IF EXISTS [{schema}];")
            driver.commit()
        config.close_pool()
