"""Integration tests for MysqlConnector sync driver implementation."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.mysqlconnector import MysqlConnectorSyncConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector]


def test_mysqlconnector_sync_transactions(mysqlconnector_sync_transaction_config: "MysqlConnectorSyncConfig") -> None:
    """Test transaction management (begin, commit, rollback).

    Note: Uses a dedicated fixture with autocommit=False for proper transaction support.
    This test is currently skipped due to issues with mysql-connector's rollback behavior
    when using connection pooling.
    """
    with mysqlconnector_sync_transaction_config.provide_session() as driver:
        # Create table for transaction testing
        driver.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table_mysqlconnector_sync (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """)
        driver.execute_script("DELETE FROM test_table_mysqlconnector_sync")
        driver.commit()

        # Test commit
        driver.begin()
        driver.execute("INSERT INTO test_table_mysqlconnector_sync (name, value) VALUES (?, ?)", ("tx_user_1", 100))
        driver.commit()

        result = driver.execute(
            "SELECT COUNT(*) as count FROM test_table_mysqlconnector_sync WHERE name = ?", ("tx_user_1",)
        )
        assert result.get_data()[0]["count"] == 1

        # Test rollback
        driver.begin()
        driver.execute("INSERT INTO test_table_mysqlconnector_sync (name, value) VALUES (?, ?)", ("tx_user_2", 200))
        driver.rollback()

        result = driver.execute(
            "SELECT COUNT(*) as count FROM test_table_mysqlconnector_sync WHERE name = ?", ("tx_user_2",)
        )
        assert result.get_data()[0]["count"] == 0
