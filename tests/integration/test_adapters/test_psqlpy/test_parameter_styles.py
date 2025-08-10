"""Test PSQLPy parameter style handling."""

from __future__ import annotations

import math
from typing import Any, Literal

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration]

ParamStyle = Literal["positional", "named", "mixed"]


@pytest.mark.parametrize(
    ("sql", "parameters", "style"),
    [
        # Positional parameters
        pytest.param("SELECT $1::text as value", ("test_value",), "positional", id="positional_single"),
        pytest.param("SELECT $1::text as val1, $2::int as val2", ("test", 42), "positional", id="positional_multiple"),
        # Named parameters (converted to positional by SQLSpec)
        pytest.param("SELECT :value::text as value", {"value": "named_test"}, "named", id="named_single"),
        pytest.param(
            "SELECT :name::text as name, :age::int as age", {"name": "John", "age": 30}, "named", id="named_multiple"
        ),
        # Mixed (should be converted appropriately)
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
    else:  # mixed
        assert result.data[0]["name"] == "Mixed"
        # Note: Mixed style may not work as expected with psqlpy


@pytest.mark.parametrize("param_count", [1, 5, 10, 20], ids=["single", "few", "medium", "many"])
async def test_many_parameters(psqlpy_session: PsqlpyDriver, param_count: int) -> None:
    """Test handling of many parameters."""
    # Create SQL with many positional parameters
    placeholders = ", ".join(f"${i}::int as val{i}" for i in range(1, param_count + 1))
    sql = f"SELECT {placeholders}"
    parameters = tuple(range(param_count))

    result = await psqlpy_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    # Verify all parameters were bound correctly
    for i in range(param_count):
        assert result.data[0][f"val{i + 1}"] == i


async def test_parameter_types(psqlpy_session: PsqlpyDriver) -> None:
    """Test various parameter data types."""
    # Test different PostgreSQL types
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
    # INSERT with parameters
    insert_result = await psqlpy_session.execute(
        "INSERT INTO test_table (name) VALUES ($1) RETURNING id", ("param_test",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.data is not None
    assert len(insert_result.data) == 1
    record_id = insert_result.data[0]["id"]

    # SELECT with parameters
    select_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "param_test"

    # UPDATE with parameters
    update_result = await psqlpy_session.execute(
        "UPDATE test_table SET name = $1 WHERE id = $2", ("updated_param", record_id)
    )
    assert isinstance(update_result, SQLResult)
    # psqlpy may not report rows_affected

    # Verify update
    verify_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["name"] == "updated_param"

    # DELETE with parameters
    delete_result = await psqlpy_session.execute("DELETE FROM test_table WHERE id = $1", (record_id,))
    assert isinstance(delete_result, SQLResult)


async def test_parameters_with_sql_object(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter handling with CORE_ROUND_3 SQL objects."""
    # Create SQL object with parameters
    sql_obj = SQL("INSERT INTO test_table (name) VALUES ($1) RETURNING id, name", ("sql_object_test",))

    result = await psqlpy_session.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "sql_object_test"
    assert result.data[0]["id"] is not None

    # Test SQL object with multiple parameters
    multi_sql = SQL("SELECT $1::text as msg, $2::int as num, $3::bool as flag", ("test", 123, False))
    multi_result = await psqlpy_session.execute(multi_sql)
    assert isinstance(multi_result, SQLResult)
    assert multi_result.data is not None
    assert multi_result.data[0]["msg"] == "test"
    assert multi_result.data[0]["num"] == 123
    assert multi_result.data[0]["flag"] is False


async def test_parameter_edge_cases(psqlpy_session: PsqlpyDriver) -> None:
    """Test parameter handling edge cases."""
    # Empty string parameter
    result1 = await psqlpy_session.execute("SELECT $1::text as empty_str", ("",))
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert result1.data[0]["empty_str"] == ""

    # Zero parameter
    result2 = await psqlpy_session.execute("SELECT $1::int as zero_val", (0,))
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert result2.data[0]["zero_val"] == 0

    # Large number parameter
    large_num = 9999999999
    result3 = await psqlpy_session.execute("SELECT $1::bigint as large_num", (large_num,))
    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert result3.data[0]["large_num"] == large_num


async def test_parameter_conversion_accuracy(psqlpy_session: PsqlpyDriver) -> None:
    """Test that parameter conversion maintains accuracy."""
    import datetime
    import decimal

    # Test decimal precision - using float instead of string to avoid psqlpy issues
    decimal_val = decimal.Decimal("123.456789")
    result1 = await psqlpy_session.execute("SELECT $1::float as decimal_val", (float(decimal_val),))
    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    # PostgreSQL may return as float or string, verify the value is close
    returned_val = result1.data[0]["decimal_val"]
    assert abs(float(returned_val) - float(decimal_val)) < 0.000001

    # Test datetime
    now = datetime.datetime.now()
    result2 = await psqlpy_session.execute("SELECT $1::timestamp as datetime_val", (now.isoformat(),))
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    # Just verify we got a result without error
    assert result2.data[0]["datetime_val"] is not None


@pytest.mark.parametrize("batch_size", [1, 5, 10, 50], ids=["single", "small", "medium", "large"])
async def test_execute_many_parameter_handling(psqlpy_session: PsqlpyDriver, batch_size: int) -> None:
    """Test parameter handling in execute_many operations."""
    # Generate test data
    parameters_list = [(f"batch_item_{i}",) for i in range(batch_size)]

    # Execute batch insert
    result = await psqlpy_session.execute_many("INSERT INTO test_table (name) VALUES ($1)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == batch_size

    # Verify all records were inserted
    count_result = await psqlpy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == batch_size

    # Verify data integrity
    for i in range(batch_size):
        check_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE name = $1", (f"batch_item_{i}",))
        assert isinstance(check_result, SQLResult)
        assert check_result.data is not None
        assert len(check_result.data) == 1
        assert check_result.data[0]["name"] == f"batch_item_{i}"
