# pyright: reportPrivateUsage=false
"""Unit tests for aiomysql event queue store index DDL.

The MySQL-family event stores historically built a procedural
``information_schema.statistics`` existence probe with an f-string
interpolated index name (a parameterized-SQL-rule violation). The index
existence check now routes through ``driver.data_dictionary.get_indexes``
at migration time and the store emits a plain ``ALTER TABLE ... ADD INDEX``.
"""

from typing import Any

import pytest


def _aiomysql_store() -> Any:
    pytest.importorskip("aiomysql")
    from sqlspec.adapters.aiomysql import AiomysqlConfig
    from sqlspec.adapters.aiomysql.events.store import AiomysqlEventQueueStore

    config = AiomysqlConfig(connection_config={"host": "localhost", "db": "test"})
    return AiomysqlEventQueueStore(config)


def test_mysql_events_index_check_no_information_schema_string() -> None:
    """The index-ensure SQL must not embed an information_schema probe."""

    store = _aiomysql_store()
    index_sql = store._index_ddl()

    assert index_sql is not None
    lowered = index_sql.lower()
    assert "information_schema" not in lowered
    assert "information_schema.statistics" not in lowered
    assert "set @" not in lowered
    assert "prepare" not in lowered
    assert "execute" not in lowered

    index_name = store._index_name()
    assert f"'{index_name}'" not in index_sql
    assert "ADD INDEX" in index_sql
    assert index_name in index_sql


def test_mysql_events_index_existence_target_parses_table() -> None:
    """The store exposes the (schema, table) target for the DD existence check."""

    store = _aiomysql_store()

    assert store._index_existence_target() == (None, "sqlspec_event_queue")
