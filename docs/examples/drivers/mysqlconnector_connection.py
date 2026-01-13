from __future__ import annotations

import pytest

__all__ = ("test_mysqlconnector_connection",)


def test_mysqlconnector_connection() -> None:
    pytest.importorskip("mysql.connector")
    # start-example
    from sqlspec.adapters.mysqlconnector import MysqlConnectorSyncConfig

    config = MysqlConnectorSyncConfig(
        connection_config={"host": "localhost", "user": "app", "password": "secret", "database": "app_db"}
    )
    # end-example

    assert config.connection_config["database"] == "app_db"
