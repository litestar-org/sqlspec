"""CockroachDB Psycopg-specific parameter variant coverage."""

import json
from collections.abc import AsyncGenerator, Generator

import pytest

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("cockroachdb")


@pytest.fixture
def cockroach_psycopg_sync_parameters_session(
    cockroach_sync_driver: CockroachPsycopgSyncDriver,
) -> Generator[CockroachPsycopgSyncDriver, None, None]:
    """Create a CockroachDB psycopg sync session for native parameter variants."""
    cockroach_sync_driver.execute_script("""
        DROP TABLE IF EXISTS cockroach_psycopg_sync_parameter_items CASCADE;
        CREATE TABLE cockroach_psycopg_sync_parameter_items (
            id SERIAL PRIMARY KEY,
            name STRING NOT NULL,
            value INT DEFAULT 0,
            description STRING
        );
    """)
    cockroach_sync_driver.execute_many(
        "INSERT INTO cockroach_psycopg_sync_parameter_items (name, value, description) VALUES (%s, %s, %s)",
        [
            ("test1", 100, "First test"),
            ("test2", 200, "Second test"),
            ("test3", 300, None),
            ("alpha", 50, "Alpha test"),
            ("beta", 75, "Beta test"),
        ],
    )
    try:
        yield cockroach_sync_driver
    finally:
        cockroach_sync_driver.execute_script("""
            DROP TABLE IF EXISTS cockroach_psycopg_sync_parameter_items CASCADE;
            DROP TABLE IF EXISTS cockroach_psycopg_sync_parameter_jsonb CASCADE;
            DROP TABLE IF EXISTS cockroach_psycopg_sync_parameter_many CASCADE;
        """)


@pytest.fixture
async def cockroach_psycopg_async_parameters_session(
    cockroach_async_driver: CockroachPsycopgAsyncDriver,
) -> AsyncGenerator[CockroachPsycopgAsyncDriver, None]:
    """Create a CockroachDB psycopg async session for native parameter variants."""
    await cockroach_async_driver.execute_script("""
        DROP TABLE IF EXISTS cockroach_psycopg_async_parameter_items CASCADE;
        CREATE TABLE cockroach_psycopg_async_parameter_items (
            id SERIAL PRIMARY KEY,
            name STRING NOT NULL,
            value INT DEFAULT 0,
            description STRING
        );
    """)
    await cockroach_async_driver.execute_many(
        "INSERT INTO cockroach_psycopg_async_parameter_items (name, value, description) VALUES (%s, %s, %s)",
        [
            ("test1", 100, "First test"),
            ("test2", 200, "Second test"),
            ("test3", 300, None),
            ("alpha", 50, "Alpha test"),
            ("beta", 75, "Beta test"),
        ],
    )
    try:
        yield cockroach_async_driver
    finally:
        await cockroach_async_driver.execute_script("""
            DROP TABLE IF EXISTS cockroach_psycopg_async_parameter_items CASCADE;
            DROP TABLE IF EXISTS cockroach_psycopg_async_parameter_jsonb CASCADE;
            DROP TABLE IF EXISTS cockroach_psycopg_async_parameter_many CASCADE;
        """)


@pytest.mark.parametrize(
    ("query", "parameters"),
    [
        ("SELECT name FROM cockroach_psycopg_sync_parameter_items WHERE name = %s", ("test1",)),
        ("SELECT name FROM cockroach_psycopg_sync_parameter_items WHERE name = %(name)s", {"name": "test1"}),
    ],
    ids=["positional_pyformat", "named_pyformat"],
)
def test_cockroach_psycopg_sync_native_pyformat_parameter_styles(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
    query: str,
    parameters: tuple[str] | dict[str, str],
) -> None:
    """Cockroach psycopg sync accepts native positional and named pyformat placeholders."""
    result = cockroach_psycopg_sync_parameters_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test1"}]


def test_cockroach_psycopg_sync_pyformat_parameters_with_sql_object(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
) -> None:
    """Cockroach psycopg sync preserves native pyformat placeholders inside SQL objects."""
    result = cockroach_psycopg_sync_parameters_session.execute(
        SQL("SELECT name FROM cockroach_psycopg_sync_parameter_items WHERE value > %s ORDER BY value", [150])
    )

    assert result.get_data() == [{"name": "test2"}, {"name": "test3"}]


def test_cockroach_psycopg_sync_serial_returning_id_with_pyformat_parameters(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
) -> None:
    """Cockroach SERIAL keys round-trip through sync native pyformat parameters."""
    insert_result = cockroach_psycopg_sync_parameters_session.execute(
        """
        INSERT INTO cockroach_psycopg_sync_parameter_items (name, value, description)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        ("serial-native", 500, "Inserted with native pyformat parameters"),
    )
    record_id = insert_result.get_data()[0]["id"]

    result = cockroach_psycopg_sync_parameters_session.execute(
        "SELECT name, value FROM cockroach_psycopg_sync_parameter_items WHERE id = %s", (record_id,)
    )

    assert isinstance(record_id, int)
    assert result.get_data() == [{"name": "serial-native", "value": 500}]


def test_cockroach_psycopg_sync_execute_many_with_pyformat_preserves_none(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
) -> None:
    """Cockroach psycopg sync preserves NULL values in native pyformat execute_many payloads."""
    cockroach_psycopg_sync_parameters_session.execute("""
        CREATE TABLE cockroach_psycopg_sync_parameter_many (
            id INT PRIMARY KEY,
            name STRING,
            value INT
        )
    """)

    result = cockroach_psycopg_sync_parameters_session.execute_many(
        "INSERT INTO cockroach_psycopg_sync_parameter_many (id, name, value) VALUES (%s, %s, %s)",
        [(1, "first", 10), (2, None, 20), (3, "third", None), (4, None, None)],
    )
    rows = cockroach_psycopg_sync_parameters_session.execute(
        "SELECT id, name, value FROM cockroach_psycopg_sync_parameter_many ORDER BY id"
    ).get_data()

    assert result.rows_affected == 4
    assert rows == [
        {"id": 1, "name": "first", "value": 10},
        {"id": 2, "name": None, "value": 20},
        {"id": 3, "name": "third", "value": None},
        {"id": 4, "name": None, "value": None},
    ]


def test_cockroach_psycopg_sync_jsonb_serialized_parameters(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
) -> None:
    """Cockroach psycopg sync JSONB parameters accept serialized payloads and return decoded values."""
    cockroach_psycopg_sync_parameters_session.execute_script("""
        CREATE TABLE cockroach_psycopg_sync_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name STRING,
            metadata JSONB,
            config JSONB
        );
    """)

    result = cockroach_psycopg_sync_parameters_session.execute(
        """
        INSERT INTO cockroach_psycopg_sync_parameter_jsonb (name, metadata, config)
        VALUES (%s, %s, %s)
        RETURNING name, metadata, config
        """,
        ("json-test", json.dumps({"score": 100, "active": True}), None),
    )

    assert result.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]


def test_cockroach_psycopg_sync_parameter_count_mismatch_with_none_raises(
    cockroach_psycopg_sync_parameters_session: CockroachPsycopgSyncDriver,
) -> None:
    """Cockroach psycopg sync still rejects native pyformat parameter count mismatches."""
    with pytest.raises(Exception):
        cockroach_psycopg_sync_parameters_session.execute("SELECT %s::STRING AS first, %s::INT AS second", (None,))


@pytest.mark.parametrize(
    ("query", "parameters"),
    [
        ("SELECT name FROM cockroach_psycopg_async_parameter_items WHERE name = %s", ("test1",)),
        ("SELECT name FROM cockroach_psycopg_async_parameter_items WHERE name = %(name)s", {"name": "test1"}),
    ],
    ids=["positional_pyformat", "named_pyformat"],
)
@pytest.mark.anyio
async def test_cockroach_psycopg_async_native_pyformat_parameter_styles(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
    query: str,
    parameters: tuple[str] | dict[str, str],
) -> None:
    """Cockroach psycopg async accepts native positional and named pyformat placeholders."""
    result = await cockroach_psycopg_async_parameters_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test1"}]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_pyformat_parameters_with_sql_object(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
) -> None:
    """Cockroach psycopg async preserves native pyformat placeholders inside SQL objects."""
    result = await cockroach_psycopg_async_parameters_session.execute(
        SQL("SELECT name FROM cockroach_psycopg_async_parameter_items WHERE value > %s ORDER BY value", [150])
    )

    assert result.get_data() == [{"name": "test2"}, {"name": "test3"}]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_serial_returning_id_with_pyformat_parameters(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
) -> None:
    """Cockroach SERIAL keys round-trip through async native pyformat parameters."""
    insert_result = await cockroach_psycopg_async_parameters_session.execute(
        """
        INSERT INTO cockroach_psycopg_async_parameter_items (name, value, description)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        ("serial-native", 500, "Inserted with native pyformat parameters"),
    )
    record_id = insert_result.get_data()[0]["id"]

    result = await cockroach_psycopg_async_parameters_session.execute(
        "SELECT name, value FROM cockroach_psycopg_async_parameter_items WHERE id = %s", (record_id,)
    )

    assert isinstance(record_id, int)
    assert result.get_data() == [{"name": "serial-native", "value": 500}]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_execute_many_with_pyformat_preserves_none(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
) -> None:
    """Cockroach psycopg async preserves NULL values in native pyformat execute_many payloads."""
    await cockroach_psycopg_async_parameters_session.execute("""
        CREATE TABLE cockroach_psycopg_async_parameter_many (
            id INT PRIMARY KEY,
            name STRING,
            value INT
        )
    """)

    result = await cockroach_psycopg_async_parameters_session.execute_many(
        "INSERT INTO cockroach_psycopg_async_parameter_many (id, name, value) VALUES (%s, %s, %s)",
        [(1, "first", 10), (2, None, 20), (3, "third", None), (4, None, None)],
    )
    rows = await cockroach_psycopg_async_parameters_session.execute(
        "SELECT id, name, value FROM cockroach_psycopg_async_parameter_many ORDER BY id"
    )

    assert result.rows_affected == 4
    assert rows.get_data() == [
        {"id": 1, "name": "first", "value": 10},
        {"id": 2, "name": None, "value": 20},
        {"id": 3, "name": "third", "value": None},
        {"id": 4, "name": None, "value": None},
    ]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_jsonb_serialized_parameters(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
) -> None:
    """Cockroach psycopg async JSONB parameters accept serialized payloads and return decoded values."""
    await cockroach_psycopg_async_parameters_session.execute_script("""
        CREATE TABLE cockroach_psycopg_async_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name STRING,
            metadata JSONB,
            config JSONB
        );
    """)

    result = await cockroach_psycopg_async_parameters_session.execute(
        """
        INSERT INTO cockroach_psycopg_async_parameter_jsonb (name, metadata, config)
        VALUES (%s, %s, %s)
        RETURNING name, metadata, config
        """,
        ("json-test", json.dumps({"score": 100, "active": True}), None),
    )

    assert result.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]


@pytest.mark.anyio
async def test_cockroach_psycopg_async_parameter_count_mismatch_with_none_raises(
    cockroach_psycopg_async_parameters_session: CockroachPsycopgAsyncDriver,
) -> None:
    """Cockroach psycopg async still rejects native pyformat parameter count mismatches."""
    with pytest.raises(Exception):
        await cockroach_psycopg_async_parameters_session.execute(
            "SELECT %s::STRING AS first, %s::INT AS second", (None,)
        )
