"""Psycopg-specific parameter variant coverage."""

import json
import math
from collections.abc import Generator
from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_parameters_session(postgres_service: PostgresService) -> Generator[PsycopgSyncDriver, None, None]:
    """Create a Psycopg sync session for native parameter variant tests."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": (
                f"postgresql://{postgres_service.user}:{postgres_service.password}"
                f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            ),
            "autocommit": True,
            "min_size": 1,
            "max_size": 5,
        }
    )

    try:
        with config.provide_session() as session:
            session.execute_script("""
                DROP TABLE IF EXISTS psycopg_parameter_items CASCADE;
                CREATE TABLE psycopg_parameter_items (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    description TEXT
                );
            """)
            session.execute_many(
                "INSERT INTO psycopg_parameter_items (name, value, description) VALUES (%s, %s, %s)",
                [
                    ("test1", 100, "First test"),
                    ("test2", 200, "Second test"),
                    ("test3", 300, None),
                    ("alpha", 50, "Alpha test"),
                    ("beta", 75, "Beta test"),
                ],
            )
            yield session
            session.execute_script("""
                DROP TABLE IF EXISTS psycopg_parameter_items CASCADE;
                DROP TABLE IF EXISTS psycopg_parameter_arrays CASCADE;
                DROP TABLE IF EXISTS psycopg_parameter_jsonb CASCADE;
                DROP TABLE IF EXISTS psycopg_parameter_none_values CASCADE;
                DROP TABLE IF EXISTS psycopg_parameter_none_many CASCADE;
                DROP TABLE IF EXISTS psycopg_parameter_count CASCADE;
            """)
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("query", "parameters"),
    [
        ("SELECT name FROM psycopg_parameter_items WHERE name = %s", ("test1",)),
        ("SELECT name FROM psycopg_parameter_items WHERE name = %(name)s", {"name": "test1"}),
    ],
    ids=["pyformat_positional", "pyformat_named"],
)
def test_psycopg_native_pyformat_parameter_styles(
    psycopg_parameters_session: PsycopgSyncDriver, query: str, parameters: Any
) -> None:
    """Psycopg accepts native positional and named pyformat placeholders."""
    result = psycopg_parameters_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test1"}]


def test_psycopg_native_pyformat_parameters_with_sql_object(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg preserves pyformat placeholders inside SQL objects."""
    result = psycopg_parameters_session.execute(
        SQL("SELECT name FROM psycopg_parameter_items WHERE value > %s ORDER BY value", [150])
    )

    assert result.get_data() == [{"name": "test2"}, {"name": "test3"}]


def test_psycopg_array_parameters_with_any(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg binds Python lists to PostgreSQL arrays and ANY expressions."""
    psycopg_parameters_session.execute_script("""
        CREATE TABLE psycopg_parameter_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        );
    """)
    psycopg_parameters_session.execute_many(
        "INSERT INTO psycopg_parameter_arrays (name, tags, scores) VALUES (%s, %s, %s)",
        [
            ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
            ("Array 2", ["tag3"], [40, 50]),
            ("Array 3", ["tag4", "tag5", "tag6"], [60]),
        ],
    )

    result = psycopg_parameters_session.execute(
        "SELECT name FROM psycopg_parameter_arrays WHERE %s = ANY(tags)", ("tag2",)
    )
    named_result = psycopg_parameters_session.execute(
        "SELECT name FROM psycopg_parameter_arrays WHERE array_length(scores, 1) > %(min_length)s ORDER BY name",
        {"min_length": 1},
    )

    assert result.get_data() == [{"name": "Array 1"}]
    assert named_result.get_data() == [{"name": "Array 1"}, {"name": "Array 2"}]


def test_psycopg_jsonb_serialized_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg JSONB parameters accept serialized payloads and return decoded values."""
    psycopg_parameters_session.execute_script("""
        CREATE TABLE psycopg_parameter_jsonb (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB
        );
    """)

    result = psycopg_parameters_session.execute(
        "INSERT INTO psycopg_parameter_jsonb (name, metadata, config) VALUES (%s, %s, %s) "
        "RETURNING name, metadata, config",
        ("json-test", json.dumps({"score": 100, "active": True}), None),
    )

    assert result.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]


def test_psycopg_named_none_values_preserve_postgres_types(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Named pyformat parameters preserve UUID, date, boolean, and NULL values."""
    psycopg_parameters_session.execute("""
        CREATE TABLE psycopg_parameter_none_values (
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

    psycopg_parameters_session.execute(
        """
        INSERT INTO psycopg_parameter_none_values (
            id, text_col, nullable_text, int_col, nullable_int,
            bool_col, nullable_bool, date_col, nullable_date
        )
        VALUES (
            %(id)s, %(text_col)s, %(nullable_text)s, %(int_col)s, %(nullable_int)s,
            %(bool_col)s, %(nullable_bool)s, %(date_col)s, %(nullable_date)s
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

    result = psycopg_parameters_session.select_one(
        "SELECT * FROM psycopg_parameter_none_values WHERE id = %(id)s", id=test_id
    )

    assert result is not None
    assert result["id"] == test_id
    assert result["nullable_text"] is None
    assert result["nullable_int"] is None
    assert result["bool_col"] is True
    assert result["nullable_bool"] is None
    assert result["date_col"] is not None
    assert result["nullable_date"] is None


def test_psycopg_execute_many_with_none_values(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg preserves None values in execute_many payloads."""
    psycopg_parameters_session.execute("""
        CREATE TABLE psycopg_parameter_none_many (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    """)

    result = psycopg_parameters_session.execute_many(
        "INSERT INTO psycopg_parameter_none_many (id, name, value) VALUES (%s, %s, %s)",
        [(1, "first", 10), (2, None, 20), (3, "third", None), (4, None, None)],
    )
    rows = psycopg_parameters_session.execute(
        "SELECT id, name, value FROM psycopg_parameter_none_many ORDER BY id"
    ).get_data()

    assert result.rows_affected == 4
    assert rows == [
        {"id": 1, "name": "first", "value": 10},
        {"id": 2, "name": None, "value": 20},
        {"id": 3, "name": "third", "value": None},
        {"id": 4, "name": None, "value": None},
    ]


def test_psycopg_parameter_count_mismatch_with_none_raises(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Parameter count mismatches still fail when provided values include None."""
    psycopg_parameters_session.execute("CREATE TABLE psycopg_parameter_count (col1 TEXT, col2 INTEGER)")

    with pytest.raises(Exception):
        psycopg_parameters_session.execute(
            "INSERT INTO psycopg_parameter_count (col1, col2) VALUES (%s, %s)", ("value1", None, "extra_param")
        )
    with pytest.raises(Exception):
        psycopg_parameters_session.execute(
            "INSERT INTO psycopg_parameter_count (col1, col2) VALUES (%s, %s)", ("value1",)
        )


def test_psycopg_pyformat_parameters_with_postgresql_functions(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg native parameters work inside PostgreSQL function expressions."""
    result = psycopg_parameters_session.execute(
        "SELECT name, value, ROUND(CAST(value * %(multiplier)s AS NUMERIC), 2) AS multiplied "
        "FROM psycopg_parameter_items WHERE value >= %(min_value)s ORDER BY value",
        {"multiplier": 1.5, "min_value": 100},
    )

    assert [row["name"] for row in result.get_data()] == ["test1", "test2", "test3"]
    for row in result.get_data():
        assert row["multiplied"] == round(row["value"] * 1.5, 2)


def test_psycopg_float_parameter_round_trip(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Psycopg preserves floating point parameter values within PostgreSQL casts."""
    result = psycopg_parameters_session.execute("SELECT %s::float AS value", (math.pi,))

    assert abs(result.get_data()[0]["value"] - math.pi) < 0.001
