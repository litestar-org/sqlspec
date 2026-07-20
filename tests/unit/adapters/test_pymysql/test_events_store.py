# pyright: reportPrivateUsage=false
"""Unit tests for pymysql event queue store index DDL (DD-based existence)."""

import pytest


def test_mysql_events_index_check_no_information_schema_string() -> None:
    """The index-ensure SQL must not embed an information_schema probe."""
    pytest.importorskip("pymysql")
    from sqlspec.adapters.pymysql import PyMysqlConfig
    from sqlspec.adapters.pymysql.events.store import PyMysqlEventQueueStore

    store = PyMysqlEventQueueStore(PyMysqlConfig(connection_config={"host": "localhost", "database": "test"}))
    index_sql = store._index_ddl()

    assert index_sql is not None
    lowered = index_sql.lower()
    assert "information_schema" not in lowered
    assert "set @" not in lowered
    assert "prepare" not in lowered
    assert "ADD INDEX" in index_sql
    assert f"'{store._index_name()}'" not in index_sql
    assert store._index_existence_target() == (None, "sqlspec_event_queue")
