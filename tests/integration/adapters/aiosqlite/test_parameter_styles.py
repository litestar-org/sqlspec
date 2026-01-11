"""Test that None values in parameters work correctly with AIOSQLite."""

import math
from datetime import date
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_aiosqlite_none_parameters() -> None:
    """Test that None values in named parameters are handled correctly by AIOSQLite."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
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

        result = await driver.execute(
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
            params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = await driver.select_one("SELECT * FROM test_none_values WHERE id = :id", id=test_id)

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


async def test_aiosqlite_none_parameters_qmark_style() -> None:
    """Test None values with QMARK (?) parameter style - AIOSQLite default."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
            CREATE TABLE test_none_qmark (
                id INTEGER PRIMARY KEY,
                col1 TEXT,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Test INSERT with None values using positional parameters
        params = ("test_value", None, None)  # None in positions 1 and 2

        result = await driver.execute("INSERT INTO test_none_qmark (col1, col2, col3) VALUES (?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = await driver.select_one("SELECT * FROM test_none_qmark WHERE col1 = ?", ("test_value",))

        assert select_result is not None
        assert select_result["col1"] == "test_value"
        assert select_result["col2"] is None
        assert select_result["col3"] is None


async def test_aiosqlite_all_none_parameters() -> None:
    """Test when all parameter values are None."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
            CREATE TABLE test_all_none (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                col1 TEXT,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Insert with all None values using named parameters
        params = {"col1": None, "col2": None, "col3": None}

        result = await driver.execute(
            """
            INSERT INTO test_all_none (col1, col2, col3)
            VALUES (:col1, :col2, :col3)
        """,
            **params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked - get the most recent row
        select_result = await driver.execute("SELECT * FROM test_all_none ORDER BY id DESC LIMIT 1")

        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        row = select_result.data[0]
        assert row["id"] is not None  # Auto-generated
        assert row["col1"] is None
        assert row["col2"] is None
        assert row["col3"] is None


async def test_aiosqlite_none_with_execute_many() -> None:
    """Test None values work correctly with execute_many."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
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

        result = await driver.execute_many("INSERT INTO test_none_many (id, name, value) VALUES (?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 4

        # Verify all rows were inserted correctly
        select_result = await driver.execute("SELECT * FROM test_none_many ORDER BY id")
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 4

        # Check specific None handling
        rows = select_result.data
        assert rows[0]["name"] == "first" and rows[0]["value"] == 10
        assert rows[1]["name"] is None and rows[1]["value"] == 20
        assert rows[2]["name"] == "third" and rows[2]["value"] is None
        assert rows[3]["name"] is None and rows[3]["value"] is None


async def test_aiosqlite_none_in_where_clause() -> None:
    """Test None values in WHERE clauses work correctly."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
            CREATE TABLE test_none_where (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT
            )
        """)

        # Insert test data
        test_data = [(1, "item1", "A"), (2, "item2", None), (3, "item3", "B"), (4, "item4", None)]
        await driver.execute_many("INSERT INTO test_none_where (id, name, category) VALUES (?, ?, ?)", test_data)

        # Test WHERE with None parameter using IS NULL comparison
        result = await driver.execute("SELECT * FROM test_none_where WHERE category IS NULL")

        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 2  # Two rows with NULL category

        # Verify the correct rows were found
        found_ids = {row["id"] for row in result.data}
        assert found_ids == {2, 4}

        # Test direct comparison with None parameter (should work with parameters)
        none_result = await driver.execute(
            "SELECT * FROM test_none_where WHERE category = ? OR ? IS NULL", (None, None)
        )

        # The second condition should be TRUE since None IS NULL
        assert isinstance(none_result, SQLResult)
        assert none_result.data is not None
        assert len(none_result.data) == 4  # All rows because condition is always true


async def test_aiosqlite_none_complex_parameter_scenarios() -> None:
    """Test complex scenarios with None parameters that might cause issues."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Create test table
        await driver.execute("""
            CREATE TABLE test_complex_none (
                id INTEGER,
                col1 TEXT,
                col2 INTEGER,
                col3 REAL,
                col4 BOOLEAN,
                col5 DATE,
                col6 TEXT  -- SQLite doesn't have native array type
            )
        """)

        # Test 1: Mix of None and complex values
        complex_params = {
            "id": 1,
            "col1": "complex_test",
            "col2": None,
            "col3": math.pi,
            "col4": None,
            "col5": date(2025, 1, 21).isoformat(),
            "col6": '["array", "with", "values"]',  # JSON string for array representation
        }

        result = await driver.execute(
            """
            INSERT INTO test_complex_none (id, col1, col2, col3, col4, col5, col6)
            VALUES (:id, :col1, :col2, :col3, :col4, :col5, :col6)
        """,
            complex_params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test 2: Correct parameter count with None values
        params_for_count_test = (2, "test2", None, None, None)  # 5 parameters for 5 placeholders

        # Should NOT raise a parameter count error
        await driver.execute(
            "INSERT INTO test_complex_none (id, col1, col2, col3, col4) VALUES (?, ?, ?, ?, ?)", params_for_count_test
        )

        # Test 3: Verify complex insert worked correctly
        verify_result = await driver.select_one("SELECT * FROM test_complex_none WHERE id = ?", (1,))

        assert verify_result is not None
        assert verify_result["col1"] == "complex_test"
        assert verify_result["col2"] is None
        assert abs(verify_result["col3"] - math.pi) < 0.00001
        assert verify_result["col4"] is None
        assert verify_result["col5"] is not None
        assert verify_result["col6"] == '["array", "with", "values"]'


async def test_aiosqlite_none_parameter_edge_cases() -> None:
    """Test edge cases that might reveal parameter handling bugs."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        # Test 1: Empty parameter list with None
        await driver.execute("CREATE TABLE test_edge (id INTEGER)")

        # Test 2: Single None parameter
        await driver.execute("CREATE TABLE test_single_none (id INTEGER, value TEXT)")
        await driver.execute("INSERT INTO test_single_none VALUES (1, ?)", (None,))

        result = await driver.select_one("SELECT * FROM test_single_none WHERE id = 1")
        assert result is not None
        assert result["value"] is None

        # Test 3: Multiple consecutive None parameters
        await driver.execute("CREATE TABLE test_consecutive_none (a INTEGER, b TEXT, c TEXT, d TEXT)")
        await driver.execute("INSERT INTO test_consecutive_none VALUES (?, ?, ?, ?)", (1, None, None, None))

        result = await driver.select_one("SELECT * FROM test_consecutive_none WHERE a = 1")
        assert result is not None
        assert result["b"] is None
        assert result["c"] is None
        assert result["d"] is None

        # Test 4: None at beginning, middle, and end positions
        await driver.execute("CREATE TABLE test_position_none (a TEXT, b TEXT, c TEXT)")
        test_cases = [
            (None, "middle", "end"),  # None at start
            ("start", None, "end"),  # None at middle
            ("start", "middle", None),  # None at end
            (None, None, "end"),  # Multiple None at start
            ("start", None, None),  # Multiple None at end
        ]

        for i, params in enumerate(test_cases):
            await driver.execute("INSERT INTO test_position_none VALUES (?, ?, ?)", params)

        # Verify all rows were inserted
        all_results = await driver.execute("SELECT COUNT(*) as count FROM test_position_none")
        assert isinstance(all_results, SQLResult)
        assert all_results.data is not None
        assert all_results.data[0]["count"] == 5


async def test_aiosqlite_parameter_count_mismatch_with_none() -> None:
    """Test that parameter count mismatches are properly detected even when None values are involved.

    This test verifies the bug mentioned in the original issue where parameter
    count mismatches might be missed when None values are present.
    """
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE test_param_count (col1 TEXT, col2 INTEGER)")

        # Test: Too many parameters - should raise an error
        with pytest.raises(Exception) as exc_info:
            await driver.execute(
                "INSERT INTO test_param_count (col1, col2) VALUES (?, ?)",  # 2 placeholders
                ("value1", None, "extra_param"),  # 3 parameters
            )

        # Should be a parameter count error or similar database error
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in ["mismatch", "parameter", "bind", "wrong", "incorrect"])

        # Test: Too few parameters - should raise an error
        with pytest.raises(Exception) as exc_info:
            await driver.execute(
                "INSERT INTO test_param_count (col1, col2) VALUES (?, ?)",  # 2 placeholders
                ("value1",),  # Only 1 parameter
            )

        # Should be a parameter count error or similar database error
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in ["mismatch", "parameter", "bind", "wrong", "incorrect"])

        # Test: Correct count with None should work fine
        result = await driver.execute("INSERT INTO test_param_count (col1, col2) VALUES (?, ?)", ("value1", None))
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1
