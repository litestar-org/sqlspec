"""CockroachDB AsyncPG-specific parameter variant coverage."""

import math
from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("cockroachdb")


@pytest.fixture
async def cockroach_asyncpg_parameters_session(
    cockroach_asyncpg_driver: CockroachAsyncpgDriver,
) -> AsyncGenerator[CockroachAsyncpgDriver, None]:
    """Create a CockroachDB AsyncPG session for native parameter variants."""
    await cockroach_asyncpg_driver.execute_script("""
        DROP TABLE IF EXISTS cockroach_asyncpg_parameter_items CASCADE;
        CREATE TABLE cockroach_asyncpg_parameter_items (
            id SERIAL PRIMARY KEY,
            name STRING NOT NULL,
            value INT DEFAULT 0,
            description STRING
        );
    """)
    await cockroach_asyncpg_driver.execute_many(
        "INSERT INTO cockroach_asyncpg_parameter_items (name, value, description) VALUES ($1, $2, $3)",
        [
            ("test1", 100, "First test"),
            ("test2", 200, "Second test"),
            ("test3", 300, None),
            ("alpha", 50, "Alpha test"),
            ("beta", 75, "Beta test"),
        ],
    )
    try:
        yield cockroach_asyncpg_driver
    finally:
        await cockroach_asyncpg_driver.execute_script("""
            DROP TABLE IF EXISTS cockroach_asyncpg_parameter_items CASCADE;
            DROP TABLE IF EXISTS cockroach_asyncpg_parameter_jsonb CASCADE;
            DROP TABLE IF EXISTS cockroach_asyncpg_parameter_many CASCADE;
        """)


@pytest.mark.parametrize("parameters", [("test1",), ["test1"]], ids=["tuple", "list"])
async def test_cockroach_asyncpg_native_numeric_parameters_accept_sequence_types(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver, parameters: tuple[str] | list[str]
) -> None:
    """Cockroach AsyncPG accepts native numeric parameters from tuple and list inputs."""
    result = await cockroach_asyncpg_parameters_session.execute(
        "SELECT name FROM cockroach_asyncpg_parameter_items WHERE name = $1", parameters
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test1"}]


async def test_cockroach_asyncpg_native_numeric_parameters_bind_multiple_values(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG binds native numeric parameters in order."""
    result = await cockroach_asyncpg_parameters_session.execute(
        """
        SELECT name, value
        FROM cockroach_asyncpg_parameter_items
        WHERE value >= $1 AND value <= $2
        ORDER BY value
        """,
        (50, 150),
    )

    assert result.get_data() == [
        {"name": "alpha", "value": 50},
        {"name": "beta", "value": 75},
        {"name": "test1", "value": 100},
    ]


async def test_cockroach_asyncpg_native_parameters_with_sql_object(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG preserves native numeric placeholders inside SQL objects."""
    result = await cockroach_asyncpg_parameters_session.execute(
        SQL("SELECT name FROM cockroach_asyncpg_parameter_items WHERE value > $1 ORDER BY value", [150])
    )

    assert result.get_data() == [{"name": "test2"}, {"name": "test3"}]


async def test_cockroach_asyncpg_serial_returning_id_with_native_parameters(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach SERIAL keys round-trip through native numeric parameters."""
    insert_result = await cockroach_asyncpg_parameters_session.execute(
        """
        INSERT INTO cockroach_asyncpg_parameter_items (name, value, description)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        ("serial-native", 500, "Inserted with native numeric parameters"),
    )
    record_id = insert_result.get_data()[0]["id"]

    result = await cockroach_asyncpg_parameters_session.execute(
        "SELECT name, value FROM cockroach_asyncpg_parameter_items WHERE id = $1", (record_id,)
    )

    assert isinstance(record_id, int)
    assert result.get_data() == [{"name": "serial-native", "value": 500}]


async def test_cockroach_asyncpg_execute_many_with_native_parameters_preserves_none(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG preserves NULL values in native numeric execute_many payloads."""
    await cockroach_asyncpg_parameters_session.execute_script("""
        CREATE TABLE cockroach_asyncpg_parameter_many (
            id INT PRIMARY KEY,
            name STRING,
            value INT
        );
    """)

    result = await cockroach_asyncpg_parameters_session.execute_many(
        "INSERT INTO cockroach_asyncpg_parameter_many (id, name, value) VALUES ($1, $2, $3)",
        [(1, "first", 10), (2, None, 20), (3, "third", None), (4, None, None)],
    )
    rows = await cockroach_asyncpg_parameters_session.execute(
        "SELECT id, name, value FROM cockroach_asyncpg_parameter_many ORDER BY id"
    )

    assert result.rows_affected == 4
    assert rows.get_data() == [
        {"id": 1, "name": "first", "value": 10},
        {"id": 2, "name": None, "value": 20},
        {"id": 3, "name": "third", "value": None},
        {"id": 4, "name": None, "value": None},
    ]


async def test_cockroach_asyncpg_jsonb_dict_and_none_parameters(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG JSONB codecs preserve dict values and NULL parameters."""
    await cockroach_asyncpg_parameters_session.execute_script("""
        CREATE TABLE cockroach_asyncpg_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name STRING,
            metadata JSONB,
            config JSONB
        );
    """)

    result = await cockroach_asyncpg_parameters_session.execute(
        """
        INSERT INTO cockroach_asyncpg_parameter_jsonb (name, metadata, config)
        VALUES ($1, $2, $3)
        RETURNING name, metadata, config
        """,
        ("json-test", {"score": 100, "active": True}, None),
    )

    assert result.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]


async def test_cockroach_asyncpg_parameter_count_mismatch_with_none_raises(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG still rejects missing native numeric parameters when values include None."""
    with pytest.raises(Exception):
        await cockroach_asyncpg_parameters_session.execute(
            "SELECT $1::STRING AS first, $2::INT AS second",
            (None,),
        )


async def test_cockroach_asyncpg_numeric_float_parameter_round_trip(
    cockroach_asyncpg_parameters_session: CockroachAsyncpgDriver,
) -> None:
    """Cockroach AsyncPG preserves floating point values within native numeric casts."""
    result = await cockroach_asyncpg_parameters_session.execute("SELECT $1::FLOAT AS value", (math.pi,))

    assert abs(result.get_data()[0]["value"] - math.pi) < 0.001
