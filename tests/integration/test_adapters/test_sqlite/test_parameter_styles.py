"""Integration tests for SQLite parameter style handling."""

import math
from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("sqlite")


def test_qmark_parameter_style(sqlite_session: SqliteDriver) -> None:
    """Test qmark (?) parameter style - SQLite default."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    result = sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("qmark_test", 42))
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name = ? AND value = ?", ("qmark_test", 42)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "qmark_test"
    assert select_result.data[0]["value"] == 42


def test_named_colon_parameter_style(sqlite_session: SqliteDriver) -> None:
    """Test named colon (:name) parameter style."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    result = sqlite_session.execute(
        "INSERT INTO test_table (name, value) VALUES (:name, :value)", {"name": "named_test", "value": 123}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name = :target_name", {"target_name": "named_test"}
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "named_test"
    assert select_result.data[0]["value"] == 123


def test_mixed_parameter_scenarios(sqlite_session: SqliteDriver) -> None:
    """Test edge cases and mixed parameter scenarios."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    sql_obj = SQL("INSERT INTO test_table (name, value) VALUES (:name, :value)", name="sql_object_test", value=999)
    result = sqlite_session.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    verify_result = sqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("sql_object_test",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["name"] == "sql_object_test"
    assert verify_result.data[0]["value"] == 999


def test_parameter_type_coercion(sqlite_session: SqliteDriver) -> None:
    """Test parameter type coercion and handling."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    test_cases = [
        ("string_value", "test_string"),
        ("integer_value", 42),
        ("float_value", math.pi),
        ("boolean_value", True),
        ("none_value", None),
    ]

    for name, value in test_cases:
        result = sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", (name, value))
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

    select_result = sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 5

    boolean_row = next(row for row in select_result.data if row["name"] == "boolean_value")
    assert boolean_row["value"] == 1

    none_row = next(row for row in select_result.data if row["name"] == "none_value")
    assert none_row["value"] is None


def test_execute_many_parameter_styles(sqlite_session: SqliteDriver) -> None:
    """Test execute_many with different parameter styles."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    tuple_params: list[tuple[str, int]] = [("batch1", 10), ("batch2", 20), ("batch3", 30)]

    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", tuple_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    dict_params: list[dict[str, Any]] = [
        {"name": "dict1", "value": 100},
        {"name": "dict2", "value": 200},
        {"name": "dict3", "value": 300},
    ]

    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (:name, :value)", dict_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    count_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 6


def test_parameter_edge_cases(sqlite_session: SqliteDriver) -> None:
    """Test parameter handling edge cases."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 0

    result = sqlite_session.execute(
        "INSERT INTO test_table (name, value) VALUES (:param, :param)", {"param": "duplicate_param_test"}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = sqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("duplicate_param_test",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1


def test_parameter_escaping_and_sql_injection_protection(sqlite_session: SqliteDriver) -> None:
    """Test that parameters properly prevent SQL injection."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("safe_data", 42))

    malicious_input = "'; DROP TABLE test_table; --"

    result = sqlite_session.execute("SELECT * FROM test_table WHERE name = ?", (malicious_input,))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0

    count_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 1


@pytest.mark.parametrize(
    "sql_template,params,expected_count",
    [
        ("SELECT * FROM test_table WHERE value > ?", (25,), 2),
        ("SELECT * FROM test_table WHERE name LIKE ?", ("%test%",), 3),
        ("SELECT * FROM test_table WHERE value > :min_value", {"min_value": 25}, 2),
        ("SELECT * FROM test_table WHERE name = :target", {"target": "test1"}, 1),
    ],
)
def test_parameterized_query_patterns(
    sqlite_session: SqliteDriver, sql_template: str, params: Any, expected_count: int
) -> None:
    """Test various parameterized query patterns."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    test_data = [("test1", 10), ("test2", 20), ("test3", 30), ("other", 40)]
    sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    result = sqlite_session.execute(sql_template, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count


# ===== None Parameter Tests =====
# Tests consolidated from test_none_parameters.py


def test_sqlite_none_parameters() -> None:
    """Test that None values in named parameters are handled correctly by SQLite."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_values (
                id TEXT PRIMARY KEY,
                text_col TEXT,
                nullable_text TEXT,
                int_col INTEGER,
                nullable_int INTEGER,
                bool_col BOOLEAN,
                nullable_bool BOOLEAN,
                date_col DATE,
                nullable_date DATE
            )
        """)

        # Test INSERT with None values using named parameters
        test_id = str(uuid4())
        params = {
            "id": test_id,
            "text_col": "test_value",
            "nullable_text": None,  # None value
            "int_col": 42,
            "nullable_int": None,  # None value
            "bool_col": True,
            "nullable_bool": None,  # None value
            "date_col": date(2025, 1, 21).isoformat(),
            "nullable_date": None,  # None value
        }

        result = driver.execute(
            """
            INSERT INTO test_none_values (
                id, text_col, nullable_text, int_col, nullable_int,
                bool_col, nullable_bool, date_col, nullable_date
            )
            VALUES (
                :id, :text_col, :nullable_text, :int_col, :nullable_int,
                :bool_col, :nullable_bool, :date_col, :nullable_date
            )
        """,
            statement_config=None,
            **params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_none_values WHERE id = :id", id=test_id)

        assert select_result is not None
        assert select_result["id"] == test_id
        assert select_result["text_col"] == "test_value"
        assert select_result["nullable_text"] is None
        assert select_result["int_col"] == 42
        assert select_result["nullable_int"] is None
        # SQLite stores boolean as integer
        assert select_result["bool_col"] == 1  # True -> 1
        assert select_result["nullable_bool"] is None
        assert select_result["date_col"] is not None  # Date stored as string
        assert select_result["nullable_date"] is None


def test_sqlite_none_parameters_qmark_style() -> None:
    """Test None values with QMARK (?) parameter style - SQLite default."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_qmark (
                id INTEGER PRIMARY KEY,
                col1 TEXT,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Test INSERT with None values using positional parameters
        params = ("test_value", None, None)  # None in positions 1 and 2

        result = driver.execute("INSERT INTO test_none_qmark (col1, col2, col3) VALUES (?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_none_qmark WHERE col1 = ?", ("test_value",))

        assert select_result is not None
        assert select_result["col1"] == "test_value"
        assert select_result["col2"] is None
        assert select_result["col3"] is None


def test_sqlite_all_none_parameters() -> None:
    """Test when all parameter values are None."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_all_none (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                col1 TEXT,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Insert with all None values using named parameters
        params = {"col1": None, "col2": None, "col3": None}

        result = driver.execute(
            """
            INSERT INTO test_all_none (col1, col2, col3)
            VALUES (:col1, :col2, :col3)
        """,
            **params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_all_none WHERE id = last_insert_rowid()")

        assert select_result is not None
        assert select_result["id"] is not None  # Auto-generated
        assert select_result["col1"] is None
        assert select_result["col2"] is None
        assert select_result["col3"] is None


def test_sqlite_none_with_execute_many() -> None:
    """Test None values work correctly with execute_many."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_many (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)

        # Test execute_many with some None values
        params = [
            (1, "first", 10),
            (2, None, 20),  # None name
            (3, "third", None),  # None value
            (4, None, None),  # Both None
        ]

        result = driver.execute_many("INSERT INTO test_none_many (id, name, value) VALUES (?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 4

        # Verify all rows were inserted correctly
        select_result = driver.execute("SELECT * FROM test_none_many ORDER BY id")
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 4

        # Check specific None handling
        rows = select_result.data
        assert rows[0]["name"] == "first" and rows[0]["value"] == 10
        assert rows[1]["name"] is None and rows[1]["value"] == 20
        assert rows[2]["name"] == "third" and rows[2]["value"] is None
        assert rows[3]["name"] is None and rows[3]["value"] is None


def test_sqlite_none_in_where_clause() -> None:
    """Test None values in WHERE clauses work correctly."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_where (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT
            )
        """)

        # Insert test data
        test_data = [(1, "item1", "A"), (2, "item2", None), (3, "item3", "B"), (4, "item4", None)]
        driver.execute_many("INSERT INTO test_none_where (id, name, category) VALUES (?, ?, ?)", test_data)

        # Test WHERE with None parameter (should find NULL values)
        result = driver.execute("SELECT * FROM test_none_where WHERE category IS :category", {"category": None})

        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 2  # Two rows with NULL category

        # Verify the correct rows were found
        found_ids = {row["id"] for row in result.data}
        assert found_ids == {2, 4}
