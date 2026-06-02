"""AsyncPG-specific parameter variant coverage."""

import math
from collections.abc import AsyncGenerator
from datetime import date
from uuid import uuid4

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
async def asyncpg_parameters_session(asyncpg_async_driver: AsyncpgDriver) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create an AsyncPG session for native parameter variant tests."""
    await asyncpg_async_driver.execute_script("""
        DROP TABLE IF EXISTS asyncpg_parameter_items CASCADE;
        CREATE TABLE asyncpg_parameter_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0,
            description TEXT
        );
        INSERT INTO asyncpg_parameter_items (name, value, description) VALUES
            ('test1', 100, 'First test'),
            ('test2', 200, 'Second test'),
            ('test3', 300, NULL),
            ('alpha', 50, 'Alpha test'),
            ('beta', 75, 'Beta test');
    """)
    try:
        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("""
            DROP TABLE IF EXISTS asyncpg_parameter_items CASCADE;
            DROP TABLE IF EXISTS asyncpg_parameter_arrays CASCADE;
            DROP TABLE IF EXISTS asyncpg_parameter_jsonb CASCADE;
            DROP TABLE IF EXISTS asyncpg_parameter_none_values CASCADE;
        """)


@pytest.mark.parametrize("parameters", [("test1",), ["test1"]], ids=["tuple", "list"])
async def test_asyncpg_native_numeric_parameters_accept_sequence_types(
    asyncpg_parameters_session: AsyncpgDriver, parameters: tuple[str] | list[str]
) -> None:
    """AsyncPG accepts native numeric parameters from tuple and list inputs."""
    result = await asyncpg_parameters_session.execute(
        "SELECT name FROM asyncpg_parameter_items WHERE name = $1", parameters
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test1"}]


async def test_asyncpg_native_numeric_parameters_bind_multiple_values(
    asyncpg_parameters_session: AsyncpgDriver,
) -> None:
    """AsyncPG binds multiple native numeric parameters in order."""
    result = await asyncpg_parameters_session.execute(
        "SELECT name, value FROM asyncpg_parameter_items WHERE value >= $1 AND value <= $2 ORDER BY value",
        (50, 150),
    )

    assert result.get_data() == [
        {"name": "alpha", "value": 50},
        {"name": "beta", "value": 75},
        {"name": "test1", "value": 100},
    ]


async def test_asyncpg_native_parameters_with_sql_object(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """AsyncPG preserves native numeric placeholders inside SQL objects."""
    result = await asyncpg_parameters_session.execute(
        SQL("SELECT name FROM asyncpg_parameter_items WHERE value > $1 ORDER BY value", [150])
    )

    assert result.get_data() == [{"name": "test2"}, {"name": "test3"}]


async def test_asyncpg_array_parameters_with_any(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """AsyncPG binds Python lists to PostgreSQL arrays and ANY expressions."""
    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE asyncpg_parameter_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        );
    """)
    await asyncpg_parameters_session.execute_many(
        "INSERT INTO asyncpg_parameter_arrays (name, tags, scores) VALUES ($1, $2, $3)",
        [
            ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
            ("Array 2", ["tag3"], [40, 50]),
            ("Array 3", ["tag4", "tag5", "tag6"], [60]),
        ],
    )

    result = await asyncpg_parameters_session.execute(
        "SELECT name FROM asyncpg_parameter_arrays WHERE $1 = ANY(tags)", ("tag2",)
    )
    length_result = await asyncpg_parameters_session.execute(
        "SELECT name FROM asyncpg_parameter_arrays WHERE array_length(scores, 1) > $1 ORDER BY name", (1,)
    )

    assert result.get_data() == [{"name": "Array 1"}]
    assert length_result.get_data() == [{"name": "Array 1"}, {"name": "Array 2"}]


async def test_asyncpg_jsonb_dict_and_none_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """AsyncPG JSON codecs preserve dict values and JSONB NULL parameters."""
    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE asyncpg_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB
        );
    """)

    result = await asyncpg_parameters_session.execute(
        "INSERT INTO asyncpg_parameter_jsonb (name, metadata, config) VALUES ($1, $2, $3) "
        "RETURNING name, metadata, config",
        ("json-test", {"score": 100, "active": True}, None),
    )

    assert result.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]


async def test_asyncpg_named_none_values_preserve_postgres_types(
    asyncpg_parameters_session: AsyncpgDriver,
) -> None:
    """Named parameters preserve UUID, date, boolean, and NULL values."""
    await asyncpg_parameters_session.execute("""
        CREATE TABLE asyncpg_parameter_none_values (
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
    test_id = uuid4()

    await asyncpg_parameters_session.execute(
        """
        INSERT INTO asyncpg_parameter_none_values (
            id, text_col, nullable_text, int_col, nullable_int,
            bool_col, nullable_bool, date_col, nullable_date
        )
        VALUES (
            :id, :text_col, :nullable_text, :int_col, :nullable_int,
            :bool_col, :nullable_bool, :date_col, :nullable_date
        )
        """,
        statement_config=None,
        id=test_id,
        text_col="test_value",
        nullable_text=None,
        int_col=42,
        nullable_int=None,
        bool_col=True,
        nullable_bool=None,
        date_col=date(2025, 1, 21),
        nullable_date=None,
    )

    result = await asyncpg_parameters_session.select_one(
        "SELECT * FROM asyncpg_parameter_none_values WHERE id = :id", id=test_id
    )

    assert result is not None
    assert result["id"] == test_id
    assert result["nullable_text"] is None
    assert result["nullable_int"] is None
    assert result["bool_col"] is True
    assert result["nullable_bool"] is None
    assert result["date_col"] is not None
    assert result["nullable_date"] is None


async def test_asyncpg_parameter_count_mismatch_with_none_raises(
    asyncpg_parameters_session: AsyncpgDriver,
) -> None:
    """Missing native numeric parameters still fail when provided values include None."""
    with pytest.raises(Exception):
        await asyncpg_parameters_session.execute("SELECT $1::text AS first, $2::int AS second", (None,))


async def test_asyncpg_numeric_parameters_with_postgresql_functions(
    asyncpg_parameters_session: AsyncpgDriver,
) -> None:
    """AsyncPG native parameters work inside PostgreSQL function expressions."""
    result = await asyncpg_parameters_session.execute(
        "SELECT name, value, ROUND((value * $1::FLOAT)::NUMERIC, 2) AS multiplied "
        "FROM asyncpg_parameter_items WHERE value >= $2 ORDER BY value",
        (1.5, 100),
    )

    assert [row["name"] for row in result] == ["test1", "test2", "test3"]
    for row in result:
        assert float(row["multiplied"]) == round(row["value"] * 1.5, 2)


async def test_asyncpg_numeric_float_parameter_round_trip(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """AsyncPG preserves floating point parameter values within PostgreSQL casts."""
    result = await asyncpg_parameters_session.execute("SELECT $1::float AS value", (math.pi,))

    assert abs(result.get_data()[0]["value"] - math.pi) < 0.001
