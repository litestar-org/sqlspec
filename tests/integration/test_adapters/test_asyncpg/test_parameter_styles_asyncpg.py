"""Test different parameter styles for AsyncPG drivers."""

import math
from collections.abc import AsyncGenerator
from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture(scope="function")
async def asyncpg_parameters_session(asyncpg_async_driver: AsyncpgDriver) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG session for parameter style testing."""

    try:
        await asyncpg_async_driver.execute_script(
            """
                DROP TABLE IF EXISTS test_parameters CASCADE;
                CREATE TABLE test_parameters (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    description TEXT
                );
                -- Insert all test data in one go
                INSERT INTO test_parameters (name, value, description) VALUES
                    ('test1', 100, 'First test'),
                    ('test2', 200, 'Second test'),
                    ('test3', 300, NULL),
                    ('alpha', 50, 'Alpha test'),
                    ('beta', 75, 'Beta test'),
                    ('gamma', 250, 'Gamma test');
            """
        )
        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("DROP TABLE IF EXISTS test_parameters")


@pytest.mark.parametrize("parameters,expected_count", [(("test1",), 1), (["test1"], 1)])
async def test_asyncpg_numeric_parameter_types(
    asyncpg_parameters_session: AsyncpgDriver, parameters: Any, expected_count: int
) -> None:
    """Test different parameter types with AsyncPG numeric style."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", parameters)

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == expected_count
    if expected_count > 0:
        assert result[0]["name"] == "test1"


async def test_asyncpg_numeric_parameter_style(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test PostgreSQL numeric parameter style with AsyncPG."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", ("test1",))

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test1"


async def test_asyncpg_multiple_parameters_numeric(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test queries with multiple parameters using numeric style."""
    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= $1 AND value <= $2 ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 3
    assert result[0]["value"] == 50
    assert result[1]["value"] == 75
    assert result[2]["value"] == 100


async def test_asyncpg_null_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test handling of NULL parameters on AsyncPG."""

    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test3"
    assert result[0]["description"] is None

    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("null_param_test", 400, None)
    )

    null_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = $1", ("null_param_test",)
    )
    assert len(null_result) == 1
    assert null_result[0]["description"] is None


async def test_asyncpg_parameter_escaping(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameter escaping prevents SQL injection."""

    malicious_input = "'; DROP TABLE test_parameters; --"

    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = $1", (malicious_input,)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 0

    count_result = await asyncpg_parameters_session.execute("SELECT COUNT(*) as count FROM test_parameters")
    assert count_result[0]["count"] >= 3


async def test_asyncpg_parameter_with_like(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with LIKE operations."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name LIKE $1", ("test%",))

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) >= 3

    specific_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name LIKE $1", ("test1%",)
    )
    assert len(specific_result) == 1
    assert specific_result[0]["name"] == "test1"


async def test_asyncpg_parameter_with_any_array(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL ANY and arrays."""

    await asyncpg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)",
        [("delta", 10, "Delta test"), ("epsilon", 20, "Epsilon test"), ("zeta", 30, "Zeta test")],
    )

    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = ANY($1) ORDER BY name", (["alpha", "beta", "test1"],)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 3
    assert result[0]["name"] == "alpha"
    assert result[1]["name"] == "beta"
    assert result[2]["name"] == "test1"


async def test_asyncpg_parameter_with_sql_object(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with SQL object."""

    sql_obj = SQL("SELECT * FROM test_parameters WHERE value > $1", [150])
    result = await asyncpg_parameters_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) >= 1
    assert all(row["value"] > 150 for row in result)


async def test_asyncpg_parameter_data_types(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test different parameter data types with AsyncPG."""

    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_types (
            id SERIAL PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val TEXT,
            bool_val BOOLEAN,
            array_val INTEGER[]
        )
    """)

    test_data = [
        (42, math.pi, "hello", True, [1, 2, 3]),
        (-100, -2.5, "world", False, [4, 5, 6]),
        (0, 0.0, "", None, []),
    ]

    for data in test_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_types (int_val, real_val, text_val, bool_val, array_val) VALUES ($1, $2, $3, $4, $5)",
            data,
        )

    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_types WHERE int_val = $1 AND real_val = $2", (42, math.pi)
    )

    assert len(result) == 1
    assert result[0]["text_val"] == "hello"
    assert result[0]["bool_val"] is True
    assert result[0]["array_val"] == [1, 2, 3]


async def test_asyncpg_parameter_edge_cases(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test edge cases for AsyncPG parameters."""

    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("", 999, "Empty name test")
    )

    empty_result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", ("",))
    assert len(empty_result) == 1
    assert empty_result[0]["value"] == 999

    long_string = "x" * 1000
    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("long_test", 1000, long_string)
    )

    long_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE description = $1", (long_string,)
    )
    assert len(long_result) == 1
    assert len(long_result[0]["description"]) == 1000


async def test_asyncpg_parameter_with_postgresql_functions(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL functions."""

    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE LENGTH(name) > $1 AND UPPER(name) LIKE $2", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result is not None

    assert len(result) >= 3

    math_result = await asyncpg_parameters_session.execute(
        "SELECT name, value, ROUND((value * $1::FLOAT)::NUMERIC, 2) as multiplied FROM test_parameters WHERE value >= $2",
        (1.5, 100),
    )
    assert len(math_result) >= 3

    for row in math_result:
        expected = round(row["value"] * 1.5, 2)
        multiplied_value = float(row["multiplied"])

        assert multiplied_value == expected


async def test_asyncpg_parameter_with_json(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL JSON operations."""

    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        );
        TRUNCATE TABLE test_json RESTART IDENTITY;
    """)

    json_data = [
        ("JSON 1", {"type": "test", "value": 100, "active": True}),
        ("JSON 2", {"type": "prod", "value": 200, "active": False}),
        ("JSON 3", {"type": "test", "value": 300, "tags": ["a", "b"]}),
    ]

    for name, metadata in json_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_json (name, metadata) VALUES ($1, $2)", (name, metadata)
        )

    result = await asyncpg_parameters_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json WHERE metadata->>'type' = $1",
        ("test",),
    )

    assert len(result) == 2
    assert all(row["type"] == "test" for row in result)


async def test_asyncpg_parameter_with_arrays(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL array operations."""

    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        );
        TRUNCATE TABLE test_arrays RESTART IDENTITY;
    """)

    array_data = [
        ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
        ("Array 2", ["tag3"], [40, 50]),
        ("Array 3", ["tag4", "tag5", "tag6"], [60]),
    ]

    for name, tags, scores in array_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_arrays (name, tags, scores) VALUES ($1, $2, $3)", (name, tags, scores)
        )

    result = await asyncpg_parameters_session.execute("SELECT name FROM test_arrays WHERE $1 = ANY(tags)", ("tag2",))

    assert len(result) == 1
    assert result[0]["name"] == "Array 1"

    length_result = await asyncpg_parameters_session.execute(
        "SELECT name FROM test_arrays WHERE array_length(scores, 1) > $1", (1,)
    )
    assert len(length_result) == 2


async def test_asyncpg_parameter_with_window_functions(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL window functions."""

    await asyncpg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)",
        [
            ("window1", 50, "Group A"),
            ("window2", 75, "Group A"),
            ("window3", 25, "Group B"),
            ("window4", 100, "Group B"),
        ],
    )

    result = await asyncpg_parameters_session.execute(
        """
        SELECT
            name,
            value,
            description,
            ROW_NUMBER() OVER (PARTITION BY description ORDER BY value) as row_num
        FROM test_parameters
        WHERE value > $1
        ORDER BY description, value
    """,
        (30,),
    )

    assert len(result) >= 4

    group_a_rows = [row for row in result if row["description"] == "Group A"]
    assert len(group_a_rows) == 2
    assert group_a_rows[0]["row_num"] == 1
    assert group_a_rows[1]["row_num"] == 2


async def test_asyncpg_none_values_in_named_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test that None values in named parameters are handled correctly."""
    await asyncpg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_none_values (
            id UUID PRIMARY KEY,
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
    test_id = uuid4()
    params = {
        "id": test_id,
        "text_col": "test_value",
        "nullable_text": None,  # None value
        "int_col": 42,
        "nullable_int": None,  # None value
        "bool_col": True,
        "nullable_bool": None,  # None value
        "date_col": date(2025, 1, 21),
        "nullable_date": None,  # None value
    }

    await asyncpg_parameters_session.execute(
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

    # Verify the insert worked
    result = await asyncpg_parameters_session.select_one("SELECT * FROM test_none_values WHERE id = :id", id=test_id)

    assert result is not None
    assert result["id"] == test_id
    assert result["text_col"] == "test_value"
    assert result["nullable_text"] is None
    assert result["int_col"] == 42
    assert result["nullable_int"] is None
    assert result["bool_col"] is True
    assert result["nullable_bool"] is None
    assert result["date_col"] is not None  # Date object
    assert result["nullable_date"] is None

    await asyncpg_parameters_session.execute("DROP TABLE test_none_values")


async def test_asyncpg_all_none_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test when all parameter values are None."""
    await asyncpg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_all_none (
            id SERIAL PRIMARY KEY,
            col1 TEXT,
            col2 INTEGER,
            col3 BOOLEAN
        )
    """)

    # Insert with all None values
    params = {"col1": None, "col2": None, "col3": None}

    result = await asyncpg_parameters_session.select_one(
        """
        INSERT INTO test_all_none (col1, col2, col3)
        VALUES (:col1, :col2, :col3)
        RETURNING id, col1, col2, col3
    """,
        **params,
    )

    assert result is not None
    assert result["id"] is not None  # Auto-generated
    assert result["col1"] is None
    assert result["col2"] is None
    assert result["col3"] is None

    await asyncpg_parameters_session.execute("DROP TABLE test_all_none")


async def test_asyncpg_jsonb_none_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test JSONB column None parameter handling comprehensively."""

    await asyncpg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_jsonb_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB,
            tags JSONB
        )
    """)

    # Test 1: Insert None values into JSONB columns
    result1 = await asyncpg_parameters_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4) RETURNING id, name, metadata, config, tags",
        ("test_none_jsonb", None, None, None),
    )

    assert isinstance(result1, SQLResult)
    assert result1 is not None
    assert len(result1) == 1
    assert result1[0]["name"] == "test_none_jsonb"
    assert result1[0]["metadata"] is None
    assert result1[0]["config"] is None
    assert result1[0]["tags"] is None

    # Test 2: Insert mixed JSON data and None values
    json_data = {"user_id": 123, "preferences": {"theme": "dark", "notifications": True}}
    complex_json = {"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}], "total": 2}

    result2 = await asyncpg_parameters_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4) RETURNING id, metadata, config, tags",
        ("test_mixed_jsonb", json_data, None, complex_json),
    )

    assert isinstance(result2, SQLResult)
    assert result2 is not None
    assert len(result2) == 1
    assert result2[0]["metadata"]["user_id"] == 123
    assert result2[0]["config"] is None
    assert result2[0]["tags"]["total"] == 2

    # Test 3: Query JSONB columns with None values using positional parameters
    result3 = await asyncpg_parameters_session.execute("SELECT * FROM test_jsonb_none WHERE metadata IS NULL")

    assert isinstance(result3, SQLResult)
    assert result3 is not None
    assert len(result3) == 1
    assert result3[0]["name"] == "test_none_jsonb"

    # Test 4: Query JSONB columns filtering by JSON content
    result4 = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_jsonb_none WHERE metadata->>'user_id' = $1", ("123",)
    )

    assert isinstance(result4, SQLResult)
    assert result4 is not None
    assert len(result4) == 1
    assert result4[0]["name"] == "test_mixed_jsonb"

    # Test 5: Insert using named parameters with JSONB and None
    params = {
        "name": "named_jsonb_test",
        "metadata": {"type": "test", "version": "1.0"},
        "config": None,
        "tags": ["tag1", "tag2", "tag3"],
    }

    result5 = await asyncpg_parameters_session.execute(
        """INSERT INTO test_jsonb_none (name, metadata, config, tags)
           VALUES (:name, :metadata, :config, :tags)
           RETURNING name, metadata, config, tags""",
        statement_config=None,
        **params,
    )

    assert isinstance(result5, SQLResult)
    assert result5 is not None
    assert len(result5) == 1
    assert result5[0]["name"] == "named_jsonb_test"

    assert result5[0]["metadata"]["type"] == "test"

    assert result5[0]["config"] is None

    assert result5[0]["tags"] == ["tag1", "tag2", "tag3"]

    # Test 6: Update JSONB columns with None values
    await asyncpg_parameters_session.execute(
        "UPDATE test_jsonb_none SET metadata = $1, config = $2 WHERE name = $3",
        (None, {"updated": True}, "named_jsonb_test"),
    )

    result6 = await asyncpg_parameters_session.execute(
        "SELECT metadata, config FROM test_jsonb_none WHERE name = $1", ("named_jsonb_test",)
    )

    assert isinstance(result6, SQLResult)
    assert result6 is not None
    assert len(result6) == 1
    assert result6[0]["metadata"] is None

    assert result6[0]["config"]["updated"] is True

    # Test 7: Test JSONB operations with None parameters
    result7 = await asyncpg_parameters_session.execute(
        "SELECT name FROM test_jsonb_none WHERE metadata IS NULL AND config IS NOT NULL"
    )

    assert isinstance(result7, SQLResult)
    assert result7 is not None
    assert len(result7) == 1
    assert result7[0]["name"] == "named_jsonb_test"

    # Test 8: Test COALESCE with JSONB and None values
    result8 = await asyncpg_parameters_session.execute(
        "SELECT name, COALESCE(metadata, $1::jsonb) as metadata_or_default FROM test_jsonb_none WHERE name = $2",
        ({"default": "value"}, "test_none_jsonb"),
    )

    assert isinstance(result8, SQLResult)
    assert result8 is not None
    assert len(result8) == 1
    assert result8[0]["metadata_or_default"]["default"] == "value"

    # Test 9: execute_many with JSONB None values
    batch_data = [
        ("batch1", {"batch": 1}, None, ["batch"]),
        ("batch2", None, {"config": "batch2"}, None),
        ("batch3", None, None, None),
    ]

    result9 = await asyncpg_parameters_session.execute_many(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4)", batch_data
    )

    assert isinstance(result9, SQLResult)
    assert result9.rows_affected == 3

    # Verify batch insert
    result10 = await asyncpg_parameters_session.execute(
        "SELECT name, metadata, config, tags FROM test_jsonb_none WHERE name LIKE 'batch%' ORDER BY name"
    )

    assert isinstance(result10, SQLResult)
    assert result10 is not None
    assert len(result10) == 3

    # Verify batch1
    assert result10[0]["name"] == "batch1"

    assert result10[0]["metadata"]["batch"] == 1
    assert result10[0]["config"] is None

    assert result10[0]["tags"] == ["batch"]

    # Verify batch2
    assert result10[1]["name"] == "batch2"
    assert result10[1]["metadata"] is None

    assert result10[1]["config"]["config"] == "batch2"

    assert result10[1]["tags"] is None

    # Verify batch3
    assert result10[2]["name"] == "batch3"
    assert result10[2]["metadata"] is None
    assert result10[2]["config"] is None
    assert result10[2]["tags"] is None

    await asyncpg_parameters_session.execute("DROP TABLE test_jsonb_none")
