"""PSQLPy-specific parameter variant coverage."""

import datetime
import decimal
import math
from typing import Any, Literal

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")

ParamStyle = Literal["positional", "named", "dollar_named"]


@pytest.mark.parametrize(
    ("query", "parameters", "style"),
    [
        pytest.param("SELECT $1::text AS value", ("test_value",), "positional", id="positional_single"),
        pytest.param(
            "SELECT $1::text AS val1, $2::int AS val2", ("test", 42), "positional", id="positional_multiple"
        ),
        pytest.param("SELECT :value::text AS value", {"value": "named_test"}, "named", id="colon_named"),
        pytest.param("SELECT $value::text AS value", {"value": "named_test"}, "dollar_named", id="dollar_named"),
    ],
)
async def test_psqlpy_native_parameter_styles(
    psqlpy_session: PsqlpyDriver, query: str, parameters: Any, style: ParamStyle
) -> None:
    """PSQLPy binds native numeric, colon-named, and dollar-named parameters."""
    result = await psqlpy_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    if style == "positional" and "val1" in result.get_data()[0]:
        assert result.get_data()[0] == {"val1": "test", "val2": 42}
    elif style == "positional":
        assert result.get_data()[0] == {"value": "test_value"}
    else:
        assert result.get_data()[0] == {"value": "named_test"}


@pytest.mark.parametrize("param_count", [1, 5, 10], ids=["single", "few", "many"])
async def test_psqlpy_many_native_numeric_parameters(psqlpy_session: PsqlpyDriver, param_count: int) -> None:
    """PSQLPy handles generated native numeric placeholder lists."""
    placeholders = ", ".join(f"${index}::int AS val{index}" for index in range(1, param_count + 1))
    result = await psqlpy_session.execute(f"SELECT {placeholders}", tuple(range(param_count)))

    assert result.get_data()[0] == {f"val{index + 1}": index for index in range(param_count)}


async def test_psqlpy_native_parameter_types(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy preserves native PostgreSQL scalar and JSON parameter types."""
    result = await psqlpy_session.execute(
        """
        SELECT
            $1::text AS text_val,
            $2::int AS int_val,
            $3::float AS float_val,
            $4::bool AS bool_val,
            $5::json AS json_val
        """,
        ("string_value", 42, math.pi, True, {"key": "value"}),
    )
    row = result.get_data()[0]

    assert row["text_val"] == "string_value"
    assert row["int_val"] == 42
    assert abs(row["float_val"] - math.pi) < 0.001
    assert row["bool_val"] is True
    assert row["json_val"]["key"] == "value"


async def test_psqlpy_native_parameters_with_sql_object(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy preserves native placeholders inside SQL objects."""
    result = await psqlpy_session.execute(SQL("SELECT $1::text AS message, $2::int AS number", ("test", 123)))

    assert result.get_data() == [{"message": "test", "number": 123}]


async def test_psqlpy_crud_with_native_numeric_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy native numeric parameters work through insert, select, update, and delete operations."""
    insert_result = await psqlpy_session.execute(
        "INSERT INTO test_table_psqlpy (name) VALUES ($1) RETURNING id", ("param_test",)
    )
    record_id = insert_result.get_data()[0]["id"]

    select_result = await psqlpy_session.execute("SELECT name FROM test_table_psqlpy WHERE id = $1", (record_id,))
    update_result = await psqlpy_session.execute(
        "UPDATE test_table_psqlpy SET name = $1 WHERE id = $2", ("updated_param", record_id)
    )
    verify_result = await psqlpy_session.execute("SELECT name FROM test_table_psqlpy WHERE id = $1", (record_id,))
    delete_result = await psqlpy_session.execute("DELETE FROM test_table_psqlpy WHERE id = $1", (record_id,))

    assert select_result.get_data() == [{"name": "param_test"}]
    assert isinstance(update_result, SQLResult)
    assert verify_result.get_data() == [{"name": "updated_param"}]
    assert isinstance(delete_result, SQLResult)


async def test_psqlpy_execute_rows_affected_deviation(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy single execute DML reports -1, while execute_many reports actual affected rows."""
    insert_result = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", ("single",))
    many_result = await psqlpy_session.execute_many(
        "INSERT INTO test_table_psqlpy (name) VALUES ($1)", [("batch1",), ("batch2",), ("batch3",)]
    )

    assert insert_result.rows_affected == -1
    assert many_result.rows_affected == 3


async def test_psqlpy_comprehensive_none_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy preserves None values across positional and named parameters."""
    await psqlpy_session.execute_script("""
        DROP TABLE IF EXISTS psqlpy_parameter_none_values;
        CREATE TABLE psqlpy_parameter_none_values (
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

    positional = await psqlpy_session.execute(
        """
        INSERT INTO psqlpy_parameter_none_values (
            text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool, json_col
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool
        """,
        ("test_value", None, 42, None, True, None, {"key": "value"}),
    )
    named = await psqlpy_session.execute(
        """
        INSERT INTO psqlpy_parameter_none_values (
            text_col, nullable_text, int_col, nullable_int, bool_col, nullable_bool, json_col
        )
        VALUES (:text_col, :nullable_text, :int_col, :nullable_int, :bool_col, :nullable_bool, :json_col)
        RETURNING text_col, nullable_text
        """,
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

    assert positional.get_data()[0]["nullable_text"] is None
    assert positional.get_data()[0]["nullable_int"] is None
    assert positional.get_data()[0]["nullable_bool"] is None
    assert named.get_data()[0] == {"text_col": "named_test", "nullable_text": None}


async def test_psqlpy_none_values_with_execute_many(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy preserves None values in execute_many payloads."""
    await psqlpy_session.execute_script("""
        DROP TABLE IF EXISTS psqlpy_parameter_many_none;
        CREATE TABLE psqlpy_parameter_many_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            description TEXT,
            value INTEGER
        )
    """)

    result = await psqlpy_session.execute_many(
        "INSERT INTO psqlpy_parameter_many_none (name, description, value) VALUES ($1, $2, $3)",
        [
            ("item1", "description1", 100),
            ("item2", None, 200),
            (None, "description3", None),
            ("item4", "description4", 400),
            (None, None, None),
        ],
    )
    rows = await psqlpy_session.execute("SELECT name, description, value FROM psqlpy_parameter_many_none ORDER BY id")

    assert result.rows_affected == 5
    assert rows.get_data()[1] == {"name": "item2", "description": None, "value": 200}
    assert rows.get_data()[2] == {"name": None, "description": "description3", "value": None}
    assert rows.get_data()[4] == {"name": None, "description": None, "value": None}


async def test_psqlpy_jsonb_none_parameters(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy JSONB parameters preserve dict values, arrays nested in dicts, and NULLs."""
    await psqlpy_session.execute_script("""
        DROP TABLE IF EXISTS psqlpy_parameter_jsonb;
        CREATE TABLE psqlpy_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB,
            tags JSONB
        )
    """)

    result = await psqlpy_session.execute(
        "INSERT INTO psqlpy_parameter_jsonb (name, metadata, config, tags) VALUES ($1, $2, $3, $4) "
        "RETURNING name, metadata, config, tags",
        ("json-test", {"user_id": 123}, None, {"tags": ["one", None]}),
    )
    named_result = await psqlpy_session.execute(
        "UPDATE psqlpy_parameter_jsonb SET metadata = $metadata, config = $config WHERE name = $name "
        "RETURNING metadata, config",
        {"metadata": None, "config": {"updated": True}, "name": "json-test"},
    )

    assert result.get_data()[0]["metadata"] == {"user_id": 123}
    assert result.get_data()[0]["config"] is None
    assert result.get_data()[0]["tags"] == {"tags": ["one", None]}
    assert named_result.get_data() == [{"metadata": None, "config": {"updated": True}}]


async def test_psqlpy_parameter_count_validation_with_none(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy still validates parameter counts when values include None."""
    correct = await psqlpy_session.execute(
        "SELECT $1::text AS val1, $2::int AS val2, $3::bool AS val3", ("test", None, True)
    )
    named = await psqlpy_session.execute(
        "SELECT :name::text AS name, :age::int AS age, :active::bool AS active",
        {"name": None, "age": 25, "active": None},
    )

    assert correct.get_data() == [{"val1": "test", "val2": None, "val3": True}]
    assert named.get_data() == [{"name": None, "age": 25, "active": None}]
    with pytest.raises(Exception):
        await psqlpy_session.execute("SELECT $1::text AS val1, $2::int AS val2", (None,))


async def test_psqlpy_parameter_conversion_accuracy(psqlpy_session: PsqlpyDriver) -> None:
    """PSQLPy numeric and timestamp parameter conversions retain useful precision."""
    decimal_val = decimal.Decimal("123.456789")
    now = datetime.datetime.now()

    decimal_result = await psqlpy_session.execute("SELECT $1::float AS decimal_val", (float(decimal_val),))
    timestamp_result = await psqlpy_session.execute("SELECT $1::timestamp AS datetime_val", (now.isoformat(),))

    assert abs(float(decimal_result.get_data()[0]["decimal_val"]) - float(decimal_val)) < 0.000001
    assert timestamp_result.get_data()[0]["datetime_val"] is not None
