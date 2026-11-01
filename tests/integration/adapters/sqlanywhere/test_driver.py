"""SAP SQL Anywhere driver round trips, transactions, and exception mapping."""

from collections.abc import Generator

import pytest

pytest.importorskip("sqlanydb")

from sqlspec.adapters.sqlanywhere import SqlanywhereConfig, SqlanywhereDriver
from sqlspec.exceptions import NotNullViolationError, UniqueViolationError

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
        assert cursor.fetchone()[0] == "kept"
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
