"""SAP SQL Anywhere driver round trips, transactions, and exception mapping."""

from collections.abc import Generator

import pytest

pytest.importorskip("sqlanydb")

from sqlspec.adapters.sqlanywhere import SqlanywhereConfig, SqlanywhereDriver
from sqlspec.exceptions import NotNullViolationError, UniqueViolationError
from sqlspec.storage._staging_sql import build_checkpoint_insert_sql, build_checkpoint_table_ddl
from sqlspec.utils.arrow_helpers import sql_type_for_category, sql_type_from_arrow_type

pytestmark = [pytest.mark.sqlanywhere, pytest.mark.xdist_group("sqlanywhere")]

_TABLE = "sqlspec_sa_items"


@pytest.fixture
def items_table(sqlanywhere_session: SqlanywhereDriver) -> Generator[str, None, None]:
    """Create a fresh items table and drop it after the test."""
    sqlanywhere_session.execute_script(f"DROP TABLE IF EXISTS {_TABLE}")
    sqlanywhere_session.execute_script(
        f"CREATE TABLE {_TABLE} (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL, note LONG VARCHAR NULL)"
    )
    sqlanywhere_session.commit()
    try:
        yield _TABLE
    finally:
        sqlanywhere_session.rollback()
        sqlanywhere_session.execute_script(f"DROP TABLE IF EXISTS {_TABLE}")
        sqlanywhere_session.commit()


def test_select_scalar(sqlanywhere_session: SqlanywhereDriver) -> None:
    assert sqlanywhere_session.select_value("SELECT 1") == 1


def test_parameterized_round_trip(sqlanywhere_session: SqlanywhereDriver, items_table: str) -> None:
    result = sqlanywhere_session.execute(
        f"INSERT INTO {items_table} (id, name, note) VALUES (?, ?, ?)", (1, "alpha", "x" * 5000)
    )
    assert result.rows_affected == 1
    sqlanywhere_session.execute_many(
        f"INSERT INTO {items_table} (id, name, note) VALUES (?, ?, ?)", [(2, "beta", None), (3, "gamma", "short")]
    )
    sqlanywhere_session.commit()

    rows = sqlanywhere_session.select(f"SELECT id, name, note FROM {items_table} ORDER BY id")
    assert [row["name"] for row in rows] == ["alpha", "beta", "gamma"]
    assert rows[0]["note"] == "x" * 5000
    assert rows[1]["note"] is None
    assert sqlanywhere_session.select_value(f"SELECT name FROM {items_table} WHERE id = ?", (3,)) == "gamma"


def test_rollback_discards_uncommitted_rows(sqlanywhere_session: SqlanywhereDriver, items_table: str) -> None:
    sqlanywhere_session.begin()
    sqlanywhere_session.execute(f"INSERT INTO {items_table} (id, name) VALUES (?, ?)", (10, "pending"))
    assert sqlanywhere_session.select_value(f"SELECT COUNT(*) FROM {items_table}") == 1
    sqlanywhere_session.rollback()
    assert sqlanywhere_session.select_value(f"SELECT COUNT(*) FROM {items_table}") == 0


def test_commit_is_visible_to_new_connection(
    sqlanywhere_config: SqlanywhereConfig, sqlanywhere_session: SqlanywhereDriver, items_table: str
) -> None:
    sqlanywhere_session.begin()
    sqlanywhere_session.execute(f"INSERT INTO {items_table} (id, name) VALUES (?, ?)", (20, "kept"))
    sqlanywhere_session.commit()

    connection = sqlanywhere_config.create_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT name FROM {items_table} WHERE id = 20")
        assert cursor.fetchall() == [("kept",)]
    finally:
        connection.close()


def test_unique_violation_is_mapped(sqlanywhere_session: SqlanywhereDriver, items_table: str) -> None:
    sqlanywhere_session.execute(f"INSERT INTO {items_table} (id, name) VALUES (?, ?)", (30, "first"))
    with pytest.raises(UniqueViolationError):
        sqlanywhere_session.execute(f"INSERT INTO {items_table} (id, name) VALUES (?, ?)", (30, "duplicate"))


def test_not_null_violation_is_mapped(sqlanywhere_session: SqlanywhereDriver, items_table: str) -> None:
    with pytest.raises(NotNullViolationError):
        sqlanywhere_session.execute(f"INSERT INTO {items_table} (id, name) VALUES (?, ?)", (40, None))


def test_data_dictionary_reports_version_and_tables(sqlanywhere_session: SqlanywhereDriver, items_table: str) -> None:
    dictionary = sqlanywhere_session.data_dictionary
    version = dictionary.get_version(sqlanywhere_session)
    assert version is not None
    assert version.major >= 16
    table_names = {table["table_name"].lower() for table in dictionary.get_tables(sqlanywhere_session)}
    assert items_table in table_names
    column_names = [
        column["column_name"].lower() for column in dictionary.get_columns(sqlanywhere_session, items_table)
    ]
    assert column_names == ["id", "name", "note"]


def test_staged_arrow_load_matches_custom_keys_and_rebuilds_indexes(
    sqlanywhere_session: SqlanywhereDriver, items_table: str
) -> None:
    pa = pytest.importorskip("pyarrow")
    driver = sqlanywhere_session
    driver.load_from_arrow(items_table, pa.table({"id": [1, 2], "name": ["alpha", "beta"], "note": ["old-a", "old-b"]}))
    driver.load_from_arrow(
        items_table,
        pa.table({"id": [20, 3], "name": ["beta", "gamma"], "note": ["updated", "new-c"]}),
        stage_table=True,
        conflict_keys=["name"],
        exclude_update_columns=["id"],
    )
    driver.load_from_arrow(
        items_table,
        pa.table({"id": [200, 4], "name": ["beta", "delta"], "note": ["ignored", "new-d"]}),
        stage_table=True,
        conflict_keys=["name"],
        on_conflict="nothing",
    )
    assert driver.select(f"SELECT id, name, note FROM {items_table} ORDER BY id") == [
        {"id": 1, "name": "alpha", "note": "old-a"},
        {"id": 2, "name": "beta", "note": "updated"},
        {"id": 3, "name": "gamma", "note": "new-c"},
        {"id": 4, "name": "delta", "note": "new-d"},
    ]
    index_name = "sqlspec_sa_items_name_index"
    driver.execute(f"CREATE INDEX {index_name} ON {items_table} (name)")
    with driver.without_indexes(items_table):
        assert index_name not in {
            index["index_name"] for index in driver.data_dictionary.get_indexes(driver, items_table)
        }
    assert index_name in {index["index_name"] for index in driver.data_dictionary.get_indexes(driver, items_table)}
    driver.commit()


def test_checkpoint_and_arrow_type_ddl_runs_on_sqlanywhere(sqlanywhere_session: SqlanywhereDriver) -> None:
    pa = pytest.importorskip("pyarrow")
    driver = sqlanywhere_session
    checkpoint_table = "sqlspec_sa_checkpoints"
    type_table = "sqlspec_sa_arrow_types"
    driver.execute(f"DROP TABLE IF EXISTS {checkpoint_table}")
    driver.execute(f"DROP TABLE IF EXISTS {type_table}")
    try:
        for _ in range(2):
            driver.execute(build_checkpoint_table_ddl("sqlanywhere", checkpoint_table))
        driver.execute(
            build_checkpoint_insert_sql("sqlanywhere", checkpoint_table),
            phase="daily",
            shard_uri="test.parquet",
            rows_loaded=3,
            bytes_processed=100,
            duration_s=0.25,
        )
        assert driver.select_value(f"SELECT COUNT(*) FROM {checkpoint_table} WHERE completed_at IS NOT NULL") == 1
        key_type = sql_type_for_category("key_text", "sqlanywhere")
        timestamp_type = sql_type_from_arrow_type(pa.timestamp("us"), "sqlanywhere")
        aware_type = sql_type_from_arrow_type(pa.timestamp("us", tz="UTC"), "sqlanywhere")
        decimal_type = sql_type_for_category("decimal", "sqlanywhere", precision=127, scale=4)
        driver.execute(
            f"CREATE TABLE {type_table} (key_value {key_type} NOT NULL PRIMARY KEY, "
            f"naive_value {timestamp_type}, aware_value {aware_type}, amount {decimal_type})"
        )
        columns = {column["column_name"]: column for column in driver.data_dictionary.get_columns(driver, type_table)}
        assert columns["key_value"]["max_length"] == 255
        assert columns["aware_value"]["data_type"].lower() == "timestamp with time zone"
        assert columns["amount"]["numeric_precision"] == 127
    finally:
        driver.rollback()
        driver.execute(f"DROP TABLE IF EXISTS {checkpoint_table}")
        driver.execute(f"DROP TABLE IF EXISTS {type_table}")
        driver.commit()
