"""Test PSQLPy parameter style handling."""

import datetime
import decimal
import math
from typing import Any, Literal

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")

ParamStyle = Literal["positional", "named", "mixed"]


@pytest.mark.parametrize(
    ("sql", "parameters", "style"),
    [
        pytest.param("SELECT $1::text as value", ("test_value",), "positional", id="positional_single"),
        pytest.param("SELECT $1::text as val1, $2::int as val2", ("test", 42), "positional", id="positional_multiple"),
        pytest.param("SELECT :value::text as value", {"value": "named_test"}, "named", id="named_single"),
        pytest.param(
            "SELECT :name::text as name, :age::int as age", {"name": "John", "age": 30}, "named", id="named_multiple"
        ),
        pytest.param(
            "SELECT :name::text as name, $2::int as age", {"name": "Mixed", "age": 25}, "mixed", id="mixed_style"
        ),
    ],
)
async def test_parameter_styles(psqlpy_session: PsqlpyDriver, sql: str, parameters: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    result = await psqlpy_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    if style == "positional":
        if "val1" in result.data[0]:
            assert result.data[0]["val1"] == "test"
            assert result.data[0]["val2"] == 42
        else:
            assert result.data[0]["value"] == "test_value"
    elif style == "named":
        if "name" in result.data[0]:
            assert result.data[0]["name"] == "John"
            assert result.data[0]["age"] == 30
        else:
            assert result.data[0]["value"] == "named_test"
    else:
        assert result.data[0]["name"] == "Mixed"


@pytest.mark.parametrize("param_count", [1, 5, 10, 20], ids=["single", "few", "medium", "many"])
async def test_many_parameters(psqlpy_session: PsqlpyDriver, param_count: int) -> None:
    """Test handling of many parameters."""

    placeholders = ", ".join(f"${i}::int as val{i}" for i in range(1, param_count + 1))
    sql = f"SELECT {placeholders}"
    parameters = tuple(range(param_count))

    result = await psqlpy_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    for i in range(param_count):
        assert result.data[0][f"val{i + 1}"] == i


async def test_parameter_types(psqlpy_session: PsqlpyDriver) -> None:
    """Test various parameter data types."""

    result = await psqlpy_session.execute(
        """
        SELECT
            $1::text as text_val,
            $2::int as int_val,
            $3::float as float_val,
            $4::bool as bool_val,
            $5::json as json_val
    """,
        ("string_value", 42, math.pi, True, {"key": "value"}),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["text_val"] == "string_value"
    assert row["int_val"] == 42
    assert abs(row["float_val"] - math.pi) < 0.001
    assert row["bool_val"] is True
    assert "key" in row["json_val"]


async def test_null_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """Test NULL parameter handling."""
    result = await psqlpy_session.execute("SELECT $1::text as val1, $2::int as val2", (None, None))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["val1"] is None
    assert result.data[0]["val2"] is None


async def test_parameters_in_crud_operations(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter handling in CRUD operations."""

    insert_result = await psqlpy_session.execute(
        "INSERT INTO test_table (name) VALUES ($1) RETURNING id", ("param_test",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.data is not None
    assert len(insert_result.data) == 1
    record_id = insert_result.data[0]["id"]

    select_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "param_test"

    update_result = await psqlpy_session.execute(
        "UPDATE test_table SET name = $1 WHERE id = $2", ("updated_param", record_id)
    )
    assert isinstance(update_result, SQLResult)

    verify_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["name"] == "updated_param"

    delete_result = await psqlpy_session.execute("DELETE FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(delete_result, SQLResult)


async def test_parameters_with_sql_object(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter handling with SQL objects."""

    sql_obj = SQL("INSERT INTO test_table (name) VALUES ($1) RETURNING id, name", ("sql_object_test",))

    result = await psqlpy_session.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "sql_object_test"
    assert result.data[0]["id"] is not None

    multi_sql = SQL("SELECT $1::text as msg, $2::int as num, $3::bool as flag", ("test", 123, False))
    multi_result = await psqlpy_session.execute(multi_sql)
    assert isinstance(multi_result, SQLResult)
    assert multi_result.data is not None
    assert multi_result.data[0]["msg"] == "test"
    assert multi_result.data[0]["num"] == 123
    assert multi_result.data[0]["flag"] is False


async def test_parameter_edge_cases(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter handling edge cases."""

    result1 = await psqlpy_session.execute("SELECT $1::text as empty_str", ("",))
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert result1.data[0]["empty_str"] == ""

    result2 = await psqlpy_session.execute("SELECT $1::int as zero_val", (0,))
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert result2.data[0]["zero_val"] == 0

    large_num = 9999999999
    result3 = await psqlpy_session.execute("SELECT $1::bigint as large_num", (large_num,))
    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert result3.data[0]["large_num"] == large_num


async def test_parameter_conversion_accuracy(psqlpy_session: PsqlpyDriver) -> None:
    """Test that parameter conversion maintains accuracy."""

    decimal_val = decimal.Decimal("123.456789")
    result1 = await psqlpy_session.execute("SELECT $1::float as decimal_val", (float(decimal_val),))
    assert isinstance(result1, SQLResult)
    assert result1.data is not None

    returned_val = result1.data[0]["decimal_val"]
    assert abs(float(returned_val) - float(decimal_val)) < 0.000001

    now = datetime.datetime.now()
    result2 = await psqlpy_session.execute("SELECT $1::timestamp as datetime_val", (now.isoformat(),))
    assert isinstance(result2, SQLResult)
    assert result2.data is not None

    assert result2.data[0]["datetime_val"] is not None


@pytest.mark.parametrize("batch_size", [1, 5, 10, 50], ids=["single", "small", "medium", "large"])
async def test_execute_many_parameter_handling(psqlpy_session: PsqlpyDriver, batch_size: int) -> None:
    """Test parameter handling in execute_many operations."""

    parameters_list = [(f"batch_item_{i}",) for i in range(batch_size)]

    result = await psqlpy_session.execute_many("INSERT INTO test_table (name) VALUES ($1)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == batch_size

    count_result = await psqlpy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == batch_size

    for i in range(batch_size):
        check_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE name = $1", (f"batch_item_{i}",))
        assert isinstance(check_result, SQLResult)
        assert check_result.data is not None
        assert len(check_result.data) == 1
        assert check_result.data[0]["name"] == f"batch_item_{i}"


async def test_comprehensive_none_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """Test comprehensive None parameter handling scenarios."""

    await psqlpy_session.execute("""
        CREATE TABLE IF NOT EXISTS test_none_comprehensive (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            nullable_text TEXT,
            int_col INTEGER,
            nullable_int INTEGER,
            bool_col BOOLEAN,
            nullable_bool BOOLEAN,
            json_col JSONB
        )
    """)

    # Test positional parameters with some None values
    result1 = await psqlpy_session.execute(
        """INSERT INTO test_none_comprehensive (text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool, json_col)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool""",
        ("test_value", None, 42, None, True, None, {"key": "value"}),
    )
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert len(result1.data) == 1

    row = result1.data[0]
    assert row["text_col"] == "test_value"
    assert row["nullable_text"] is None
    assert row["int_col"] == 42
    assert row["nullable_int"] is None
    assert row["bool_col"] is True
    assert row["nullable_bool"] is None

    # Test named parameters with some None values
    result2 = await psqlpy_session.execute(
        """INSERT INTO test_none_comprehensive (text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool, json_col)
           VALUES (:text_col, :nullable_text, :int_col, :nullable_int, :bool_col, :nullable_bool, :json_col)
           RETURNING id, text_col, nullable_text""",
        {
            "text_col": "named_test",
            "nullable_text": None,
            "int_col": 100,
            "nullable_int": None,
            "bool_col": False,
            "nullable_bool": None,
            "json_col": None,
        },
    )
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 1
    assert result2.data[0]["text_col"] == "named_test"
    assert result2.data[0]["nullable_text"] is None

    # Test all parameters being None
    result3 = await psqlpy_session.execute(
        "SELECT $1::text as val1, $2::int as val2, $3::bool as val3, $4::jsonb as val4", (None, None, None, None)
    )
    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert len(result3.data) == 1
    assert result3.data[0]["val1"] is None
    assert result3.data[0]["val2"] is None
    assert result3.data[0]["val3"] is None
    assert result3.data[0]["val4"] is None

    # Test None with named parameters (all None)
    result4 = await psqlpy_session.execute(
        "SELECT :val1::text as val1, :val2::int as val2, :val3::bool as val3",
        {"val1": None, "val2": None, "val3": None},
    )
    assert isinstance(result4, SQLResult)
    assert result4.data is not None
    assert len(result4.data) == 1
    assert result4.data[0]["val1"] is None
    assert result4.data[0]["val2"] is None
    assert result4.data[0]["val3"] is None


async def test_none_values_with_execute_many(psqlpy_session: PsqlpyDriver) -> None:
    """Test None values with execute_many operations."""

    await psqlpy_session.execute("""
        CREATE TABLE IF NOT EXISTS test_many_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            description TEXT,
            value INTEGER
        )
    """)

    # Test execute_many with mixed None and non-None values
    parameters_list = [
        ("item1", "description1", 100),
        ("item2", None, 200),  # None in middle
        (None, "description3", None),  # None in first and last
        ("item4", "description4", 400),
        (None, None, None),  # All None values
    ]

    result = await psqlpy_session.execute_many(
        "INSERT INTO test_many_none (name, description, value) VALUES ($1, $2, $3)", parameters_list
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 5

    # Verify the data was inserted correctly
    verify_result = await psqlpy_session.execute("SELECT name, description, value FROM test_many_none ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 5

    # Check specific None value handling
    assert verify_result.data[0]["name"] == "item1"
    assert verify_result.data[0]["description"] == "description1"
    assert verify_result.data[0]["value"] == 100

    assert verify_result.data[1]["name"] == "item2"
    assert verify_result.data[1]["description"] is None
    assert verify_result.data[1]["value"] == 200

    assert verify_result.data[2]["name"] is None
    assert verify_result.data[2]["description"] == "description3"
    assert verify_result.data[2]["value"] is None

    assert verify_result.data[4]["name"] is None
    assert verify_result.data[4]["description"] is None
    assert verify_result.data[4]["value"] is None


async def test_parameter_count_validation_with_none(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter count validation when using None values."""

    # Test correct parameter count with None values
    result1 = await psqlpy_session.execute(
        "SELECT $1::text as val1, $2::int as val2, $3::bool as val3", ("test", None, True)
    )
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert result1.data[0]["val1"] == "test"
    assert result1.data[0]["val2"] is None
    assert result1.data[0]["val3"] is True

    # Test named parameters with None - all parameters provided
    result2 = await psqlpy_session.execute(
        "SELECT :name::text as name, :age::int as age, :active::bool as active",
        {"name": None, "age": 25, "active": None},
    )
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert result2.data[0]["name"] is None
    assert result2.data[0]["age"] == 25
    assert result2.data[0]["active"] is None

    # Test that parameter count mismatch still raises errors (not affected by None values)
    with pytest.raises(Exception):  # Should raise some kind of database or parameter error
        await psqlpy_session.execute(
            "SELECT $1::text as val1, $2::int as val2",
            (None,),  # Missing second parameter
        )


async def test_none_parameters_in_where_clauses(psqlpy_session: PsqlpyDriver) -> None:
    """Test None parameters in WHERE clauses and comparisons."""

    await psqlpy_session.execute("""
        CREATE TABLE IF NOT EXISTS test_where_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            category TEXT,
            value INTEGER
        )
    """)

    # Insert test data
    await psqlpy_session.execute_many(
        "INSERT INTO test_where_none (name, category, value) VALUES ($1, $2, $3)",
        [("item1", "A", 100), ("item2", None, 200), ("item3", "B", None), (None, "C", 300)],
    )

    # Test WHERE with None parameter (should not match anything due to SQL NULL semantics)
    result1 = await psqlpy_session.execute("SELECT * FROM test_where_none WHERE category = $1", (None,))
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert len(result1.data) == 0  # None doesn't equal NULL in SQL

    # Test IS NULL with parameter (need to use IS NULL syntax)
    result2 = await psqlpy_session.execute("SELECT * FROM test_where_none WHERE category IS NULL")
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 1
    assert result2.data[0]["name"] == "item2"

    # Test COALESCE with None parameter
    result3 = await psqlpy_session.execute(
        "SELECT name, COALESCE(category, $1) as category FROM test_where_none WHERE name = $2",
        ("default_category", "item2"),
    )
    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert len(result3.data) == 1
    assert result3.data[0]["category"] == "default_category"


async def test_psqlpy_jsonb_none_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """Test JSONB column None parameter handling comprehensively."""

    await psqlpy_session.execute("""
        CREATE TABLE IF NOT EXISTS test_jsonb_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB,
            tags JSONB
        )
    """)

    # Test 1: Insert None values into JSONB columns using positional parameters
    result1 = await psqlpy_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4) RETURNING id, name, metadata, config, tags",
        ("test_none_jsonb", None, None, None),
    )

    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert len(result1.data) == 1
    assert result1.data[0]["name"] == "test_none_jsonb"
    assert result1.data[0]["metadata"] is None
    assert result1.data[0]["config"] is None
    assert result1.data[0]["tags"] is None

    # Test 2: Insert mixed JSON data and None values using positional parameters
    json_data = {"user_id": 123, "preferences": {"theme": "dark", "notifications": True}}
    complex_json = {"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}], "total": 2}

    result2 = await psqlpy_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4) RETURNING id, metadata, config, tags",
        ("test_mixed_jsonb", json_data, None, complex_json),
    )

    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 1
    # PSQLPy should parse JSONB back to dict objects
    assert isinstance(result2.data[0]["metadata"], dict)
    assert result2.data[0]["metadata"]["user_id"] == 123
    assert result2.data[0]["config"] is None
    assert isinstance(result2.data[0]["tags"], dict)
    assert result2.data[0]["tags"]["total"] == 2

    # Test 3: Query JSONB columns with None values
    result3 = await psqlpy_session.execute("SELECT * FROM test_jsonb_none WHERE metadata IS NULL")

    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert len(result3.data) == 1
    assert result3.data[0]["name"] == "test_none_jsonb"

    # Test 4: Query JSONB columns filtering by JSON content using positional parameters
    result4 = await psqlpy_session.execute("SELECT * FROM test_jsonb_none WHERE metadata->>'user_id' = $1", ("123",))

    assert isinstance(result4, SQLResult)
    assert result4.data is not None
    assert len(result4.data) == 1
    assert result4.data[0]["name"] == "test_mixed_jsonb"

    # Test 5: Insert using named parameters with JSONB and None
    # Note: psqlpy has known issues with Python list parameters as JSONB,
    # so we skip array testing and focus on dict/None combinations
    params = {"name": "named_jsonb_test", "metadata": {"type": "test", "version": "1.0"}, "config": None}

    result5 = await psqlpy_session.execute(
        """INSERT INTO test_jsonb_none (name, metadata, config)
           VALUES ($name, $metadata, $config)
           RETURNING name, metadata, config""",
        params,
    )

    assert isinstance(result5, SQLResult)
    assert result5.data is not None
    assert len(result5.data) == 1
    assert result5.data[0]["name"] == "named_jsonb_test"
    assert result5.data[0]["metadata"]["type"] == "test"
    assert result5.data[0]["config"] is None

    # Test 6: Update JSONB columns with None values using positional parameters
    await psqlpy_session.execute(
        "UPDATE test_jsonb_none SET metadata = $1, config = $2 WHERE name = $3",
        (None, {"updated": True}, "named_jsonb_test"),
    )

    result6 = await psqlpy_session.execute(
        "SELECT metadata, config FROM test_jsonb_none WHERE name = $1", ("named_jsonb_test",)
    )

    assert isinstance(result6, SQLResult)
    assert result6.data is not None
    assert len(result6.data) == 1
    assert result6.data[0]["metadata"] is None
    assert result6.data[0]["config"]["updated"] is True

    # Test 7: Update JSONB columns with None values using named parameters
    await psqlpy_session.execute(
        "UPDATE test_jsonb_none SET tags = $new_tags WHERE name = $target_name",
        {"new_tags": None, "target_name": "test_mixed_jsonb"},
    )

    result7 = await psqlpy_session.execute(
        "SELECT name, tags FROM test_jsonb_none WHERE name = $name", {"name": "test_mixed_jsonb"}
    )

    assert isinstance(result7, SQLResult)
    assert result7.data is not None
    assert len(result7.data) == 1
    assert result7.data[0]["tags"] is None

    # Test 8: Test JSONB operations with None parameters
    result8 = await psqlpy_session.execute(
        "SELECT name FROM test_jsonb_none WHERE metadata IS NULL AND config IS NOT NULL"
    )

    assert isinstance(result8, SQLResult)
    assert result8.data is not None
    assert len(result8.data) == 1
    assert result8.data[0]["name"] == "named_jsonb_test"

    # Test 9: Test COALESCE with JSONB and None values
    result9 = await psqlpy_session.execute(
        "SELECT name, COALESCE(metadata, $1::jsonb) as metadata_or_default FROM test_jsonb_none WHERE name = $2",
        ({"default": "value"}, "test_none_jsonb"),
    )

    assert isinstance(result9, SQLResult)
    assert result9.data is not None
    assert len(result9.data) == 1
    assert result9.data[0]["metadata_or_default"]["default"] == "value"

    # Test 10: execute_many with JSONB None values
    # Note: Using dict instead of array for tags to avoid psqlpy list expansion issues
    batch_data = [
        ("batch1", {"batch": 1}, None, {"tags": ["batch"]}),
        ("batch2", None, {"config": "batch2"}, None),
        ("batch3", None, None, None),
    ]

    result10 = await psqlpy_session.execute_many(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4)", batch_data
    )

    assert isinstance(result10, SQLResult)
    assert result10.rows_affected == 3

    # Verify batch insert
    result11 = await psqlpy_session.execute(
        "SELECT name, metadata, config, tags FROM test_jsonb_none WHERE name LIKE $1 ORDER BY name", ("batch%",)
    )

    assert isinstance(result11, SQLResult)
    assert result11.data is not None
    assert len(result11.data) == 3

    # Verify batch1
    assert result11.data[0]["name"] == "batch1"
    assert result11.data[0]["metadata"]["batch"] == 1
    assert result11.data[0]["config"] is None
    assert result11.data[0]["tags"]["tags"] == ["batch"]

    # Verify batch2
    assert result11.data[1]["name"] == "batch2"
    assert result11.data[1]["metadata"] is None
    assert result11.data[1]["config"]["config"] == "batch2"
    assert result11.data[1]["tags"] is None

    # Verify batch3
    assert result11.data[2]["name"] == "batch3"
    assert result11.data[2]["metadata"] is None
    assert result11.data[2]["config"] is None
    assert result11.data[2]["tags"] is None

    # Test 11: Test JSONB array operations with None values
    # Note: Using dicts instead of direct arrays to avoid psqlpy list expansion issues
    await psqlpy_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4)",
        ("array_test", {"numbers": [1, 2, 3]}, None, {"array": [None, "value", None]}),
    )

    result12 = await psqlpy_session.execute(
        "SELECT metadata, tags FROM test_jsonb_none WHERE name = $1", ("array_test",)
    )

    assert isinstance(result12, SQLResult)
    assert result12.data is not None
    assert len(result12.data) == 1
    assert result12.data[0]["metadata"]["numbers"] == [1, 2, 3]
    assert result12.data[0]["tags"]["array"] == [None, "value", None]

    # Test 12: Test checking for NULL JSONB fields (simplified to avoid path operator issues)
    result13 = await psqlpy_session.execute("SELECT name FROM test_jsonb_none WHERE metadata IS NULL OR config IS NULL")

    assert isinstance(result13, SQLResult)
    assert result13.data is not None
    # Should include multiple records with NULL values
    assert len(result13.data) >= 2

    # Test 13: Complex JSONB operations with mixed None and data
    result14 = await psqlpy_session.execute(
        """SELECT name,
                  metadata IS NULL as metadata_is_null,
                  config IS NULL as config_is_null,
                  tags IS NULL as tags_is_null
           FROM test_jsonb_none
           WHERE name IN (:name1, :name2, :name3)
           ORDER BY name""",
        {"name1": "test_none_jsonb", "name2": "named_jsonb_test", "name3": "batch3"},
    )

    assert isinstance(result14, SQLResult)
    assert result14.data is not None
    assert len(result14.data) == 3

    # Verify None status for each record
    batch3_row = next(r for r in result14.data if r["name"] == "batch3")
    assert batch3_row["metadata_is_null"] is True
    assert batch3_row["config_is_null"] is True
    assert batch3_row["tags_is_null"] is True

    named_test_row = next(r for r in result14.data if r["name"] == "named_jsonb_test")
    assert named_test_row["metadata_is_null"] is True  # Was set to None in earlier test
    assert named_test_row["config_is_null"] is False  # Has {"updated": True}
    assert named_test_row["tags_is_null"] is True  # Originally had array but no explicit reset

    # Test 14: Verify parameter count validation still works with JSONB None values
    with pytest.raises(Exception):  # Should raise some kind of parameter error
        await psqlpy_session.execute(
            "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES ($1, $2, $3, $4)",
            ("incomplete", None, None),  # Missing 4th parameter
        )
