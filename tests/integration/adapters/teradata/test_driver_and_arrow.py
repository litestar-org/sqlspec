"""Live Teradata transaction, SQL generation, catalog, and Arrow acceptance."""

from collections.abc import Callable

import pytest

from sqlspec import sql
from sqlspec.adapters.teradata import TeradataConfig, TeradataDriver
from sqlspec.exceptions import NotNullViolationError, UniqueViolationError

pytestmark = [pytest.mark.teradata, pytest.mark.xdist_group("teradata")]


def test_parameterized_round_trip_and_error_mapping(
    teradata_session: TeradataDriver, teradata_table_factory: Callable[[str], str]
) -> None:
    table = teradata_table_factory("rows")
    teradata_session.execute_script(
        f"CREATE MULTISET TABLE {table} (id INTEGER NOT NULL, name VARCHAR(100) NOT NULL, UNIQUE(id))"
    )
    teradata_session.commit()
    teradata_session.execute_many(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [(1, "alpha"), (2, "beta")])
    teradata_session.commit()
    assert teradata_session.select_value(f"SELECT name FROM {table} WHERE id = ?", (1,)) == "alpha"
    with pytest.raises(UniqueViolationError):
        teradata_session.execute(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "duplicate"))
    teradata_session.rollback()
    with pytest.raises(NotNullViolationError):
        teradata_session.execute(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (3, None))
    teradata_session.rollback()


def test_tera_and_ansi_transactions_commit_and_rollback(
    teradata_config: TeradataConfig, teradata_ansi_config: TeradataConfig, teradata_table_factory: Callable[[str], str]
) -> None:
    table = teradata_table_factory("tx")
    with teradata_config.provide_session() as session:
        session.execute_script(f"CREATE MULTISET TABLE {table} (id INTEGER)")
        session.commit()
    for config in (teradata_config, teradata_ansi_config):
        with config.provide_session() as session:
            session.begin()
            session.execute(f"INSERT INTO {table} (id) VALUES (?)", (1,))
            session.rollback()
            assert session.select_value(f"SELECT COUNT(*) FROM {table}") == 0
            session.begin()
            session.execute(f"INSERT INTO {table} (id) VALUES (?)", (2,))
            session.commit()
            assert session.select_value(f"SELECT COUNT(*) FROM {table}") == 1
            session.execute_script(f"DELETE FROM {table}")
            session.commit()


def test_top_qualify_catalog_and_arrow(
    teradata_session: TeradataDriver, teradata_table_factory: Callable[[str], str]
) -> None:
    table = teradata_table_factory("arrow")
    teradata_session.execute_script(f"CREATE MULTISET TABLE {table} (id INTEGER, name VARCHAR(100))")
    teradata_session.commit()
    teradata_session.execute_many(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [(i, f"item-{i}") for i in range(8)])
    teradata_session.commit()
    query = sql.select("id").from_(table).order_by("id").limit(2).offset(3).build(dialect="teradata")
    assert [row["id"] for row in teradata_session.select(query.sql)] == [3, 4]
    assert teradata_session.select_to_arrow(f"SELECT id, name FROM {table} ORDER BY id").to_dict()[0] == {
        "id": 0,
        "name": "item-0",
    }
    columns = teradata_session.data_dictionary.get_columns(teradata_session, table)
    assert {column["column_name"].lower() for column in columns} == {"id", "name"}


def test_query_band_session_and_transaction(teradata_session: TeradataDriver) -> None:
    teradata_session.set_query_band({"application": "sqlspec-tests"})
    assert "application=sqlspec-tests;" in teradata_session.select_value("SELECT GetQueryBand()")
    teradata_session.begin()
    teradata_session.set_query_band({"operation": "acceptance"}, transaction=True)
    assert "operation=acceptance;" in teradata_session.select_value("SELECT GetQueryBand()")
    teradata_session.rollback()


def test_chunked_arrow_zero_rows_and_staged_upsert(
    teradata_session: TeradataDriver, teradata_table_factory: Callable[[str], str]
) -> None:
    pa = pytest.importorskip("pyarrow")
    table = teradata_table_factory("stage")
    teradata_session.execute_script(f"CREATE MULTISET TABLE {table} (id INTEGER, name VARCHAR(100))")
    teradata_session.commit()
    teradata_session.execute_many(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [(1, "alpha"), (2, "beta")])
    teradata_session.commit()
    empty = teradata_session.select_to_arrow(f"SELECT id, name FROM {table} WHERE 1=0", batch_size=1)
    assert empty.column_names == ["id", "name"]
    assert empty.to_dict() == []
    teradata_session.load_from_arrow(
        table, pa.table({"id": [2, 3], "name": ["updated", "gamma"]}), stage_table=True, conflict_keys=["id"]
    )
    assert teradata_session.select(f"SELECT id, name FROM {table} ORDER BY id") == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "updated"},
        {"id": 3, "name": "gamma"},
    ]


def test_opt_in_fastexport(
    teradata_connection_params: dict[str, object],
    teradata_session: TeradataDriver,
    teradata_table_factory: Callable[[str], str],
) -> None:
    table = teradata_table_factory("fast")
    teradata_session.execute_script(f"CREATE MULTISET TABLE {table} (id INTEGER)")
    teradata_session.commit()
    teradata_session.execute_many(f"INSERT INTO {table} (id) VALUES (?)", [(1,), (2,)])
    teradata_session.commit()
    config = TeradataConfig(connection_config=teradata_connection_params, driver_features={"enable_fastexport": True})
    try:
        with config.provide_session() as session:
            assert session.select_to_arrow(f"SELECT id FROM {table} ORDER BY id").to_dict() == [{"id": 1}, {"id": 2}]
    finally:
        config.close_pool()
