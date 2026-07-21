"""Integration tests for mssql-python transaction persistence."""

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.mssql_python import MssqlPythonConfig

pytestmark = [pytest.mark.mssql_python, pytest.mark.xdist_group("mssql_python")]

_DROP_TABLE_SQL = "DROP TABLE IF EXISTS sqlspec_mssql_python_transactions"
_CREATE_TABLE_SQL = "CREATE TABLE sqlspec_mssql_python_transactions (id INT PRIMARY KEY, value VARCHAR(50) NOT NULL)"
_INSERT_SQL = "INSERT INTO sqlspec_mssql_python_transactions (id, value) VALUES (?, ?)"
_COUNT_SQL = "SELECT COUNT(*) FROM sqlspec_mssql_python_transactions WHERE id = ?"


def test_mssql_python_transactions_persist_and_restore_autocommit(mssql_service: MSSQLService) -> None:
    """Commit and rollback should use DBAPI-owned transactions visible across connections."""
    manual_config = MssqlPythonConfig(
        connection_config={
            "server": mssql_service.host,
            "port": mssql_service.port,
            "database": mssql_service.database,
            "user": mssql_service.user,
            "password": mssql_service.password,
            "encrypt": False,
            "trust_server_certificate": True,
            "autocommit": False,
            "pool_enabled": False,
        }
    )
    autocommit_config = MssqlPythonConfig(
        connection_config={
            "server": mssql_service.host,
            "port": mssql_service.port,
            "database": mssql_service.database,
            "user": mssql_service.user,
            "password": mssql_service.password,
            "encrypt": False,
            "trust_server_certificate": True,
            "autocommit": True,
            "pool_enabled": False,
        }
    )
    try:
        with manual_config.provide_session() as setup:
            setup.execute_script(_DROP_TABLE_SQL)
            setup.execute_script(_CREATE_TABLE_SQL)
            setup.commit()

        with manual_config.provide_session() as writer, manual_config.provide_session() as observer:
            writer.begin()
            writer.execute(_INSERT_SQL, (1, "committed"))
            writer.commit()

            assert observer.select_value(_COUNT_SQL, (1,)) == 1
            observer.commit()

            writer.begin()
            writer.execute(_INSERT_SQL, (2, "rolled-back"))
            writer.rollback()

            assert observer.select_value(_COUNT_SQL, (2,)) == 0
            observer.commit()

        with autocommit_config.provide_session() as writer, manual_config.provide_session() as observer:
            assert writer.connection.autocommit is True
            writer.begin()
            assert writer.connection.autocommit is False
            writer.execute(_INSERT_SQL, (3, "autocommit-restored"))
            writer.commit()
            assert writer.connection.autocommit is True

            assert observer.select_value(_COUNT_SQL, (3,)) == 1
            observer.commit()
    finally:
        try:
            with manual_config.provide_session() as cleanup:
                cleanup.execute_script(_DROP_TABLE_SQL)
                cleanup.commit()
        finally:
            manual_config.close_pool()
            autocommit_config.close_pool()
