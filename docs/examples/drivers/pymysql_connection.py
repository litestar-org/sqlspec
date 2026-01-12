from __future__ import annotations

import pytest

__all__ = ("test_pymysql_connection",)


def test_pymysql_connection() -> None:
    pytest.importorskip("pymysql")
    # start-example
    from sqlspec.adapters.pymysql import PyMysqlConfig

    config = PyMysqlConfig(
        connection_config={"host": "localhost", "user": "app", "password": "secret", "database": "app_db"}
    )
    # end-example

    assert config.connection_config["host"] == "localhost"
