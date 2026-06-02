# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""ADBC-specific parameter variant coverage.

Shared adapter contracts own generic binding cases. This module keeps behavior
that is tied to ADBC's native backends, null-literal pruning, and Arrow path.
"""

from collections.abc import Generator
from datetime import date

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core import SQL, SQLResult
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import PYARROW_INSTALLED
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

pytestmark = pytest.mark.adbc


@pytest.fixture
def adbc_postgres_parameter_session(adbc_postgresql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create a PostgreSQL-backed ADBC session for native parameter variants."""
    with adbc_postgresql_config.provide_session() as session:
        session.execute_script("""
            DROP TABLE IF EXISTS adbc_parameter_items CASCADE;
            DROP TABLE IF EXISTS adbc_parameter_jsonb CASCADE;
            CREATE TABLE adbc_parameter_items (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                active BOOLEAN,
                created_date DATE
            );
            CREATE TABLE adbc_parameter_jsonb (
                id INTEGER PRIMARY KEY,
                metadata JSONB,
                config JSONB
            );
        """)
        try:
            yield session
        finally:
            try:
                session.execute_script("""
                    DROP TABLE IF EXISTS adbc_parameter_jsonb CASCADE;
                    DROP TABLE IF EXISTS adbc_parameter_items CASCADE;
                """)
            except Exception:
                try:
                    session.execute("ROLLBACK")
                    session.execute_script("""
                        DROP TABLE IF EXISTS adbc_parameter_jsonb CASCADE;
                        DROP TABLE IF EXISTS adbc_parameter_items CASCADE;
                    """)
                except Exception:
                    pass


def _first_row(result: SQLResult) -> dict[str, object]:
    rows = result.get_data()
    assert len(rows) == 1
    return rows[0]


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_numeric_parameters_prune_null_literals(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """ADBC PostgreSQL keeps native numeric placeholders while pruning NULL parameters."""
    result = adbc_postgres_parameter_session.execute(
        """
        INSERT INTO adbc_parameter_items (id, name, value, active, created_date)
        VALUES ($1, $2, $3, $4, $5)
        """,
        (1, None, 42, True, date(2025, 1, 21)),
    )
    row = _first_row(
        adbc_postgres_parameter_session.execute(
            "SELECT id, name, value, active, created_date FROM adbc_parameter_items WHERE id = $1",
            (1,),
        )
    )

    assert result.rows_affected in (-1, 0, 1)
    assert row["id"] == 1
    assert row["name"] is None
    assert row["value"] == 42
    assert row["active"] is True
    assert row["created_date"] is not None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_numeric_parameters_with_sql_object(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """ADBC preserves PostgreSQL numeric placeholders inside SQL objects."""
    adbc_postgres_parameter_session.execute_many(
        "INSERT INTO adbc_parameter_items (id, name, value) VALUES ($1, $2, $3)",
        [(1, "low", 10), (2, "mid", 20), (3, "high", 30)],
    )

    result = adbc_postgres_parameter_session.execute(
        SQL("SELECT name, value FROM adbc_parameter_items WHERE value >= $1 ORDER BY value", [20])
    )

    assert result.get_data() == [{"name": "mid", "value": 20}, {"name": "high", "value": 30}]


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_repeated_null_numeric_parameter(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """Repeated PostgreSQL numeric references still bind one pruned NULL parameter."""
    adbc_postgres_parameter_session.execute_many(
        "INSERT INTO adbc_parameter_items (id, name, value) VALUES ($1, $2, $3)",
        [(1, "named", 10), (2, None, 20)],
    )

    result = adbc_postgres_parameter_session.execute(
        """
        SELECT id, name FROM adbc_parameter_items
        WHERE name = $1 OR ($1 IS NULL AND name IS NULL)
        ORDER BY id
        """,
        (None,),
    )

    assert result.get_data() == [{"id": 2, "name": None}]


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_returning_clause_with_null_parameter(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """PostgreSQL-backed ADBC returns rows from RETURNING with pruned NULL parameters."""
    result = adbc_postgres_parameter_session.execute(
        """
        INSERT INTO adbc_parameter_items (id, name, value, active)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, value, active
        """,
        (10, None, 200, None),
    )

    assert result.get_data() == [{"id": 10, "name": None, "value": 200, "active": None}]


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_jsonb_cast_parameters_serialize_dict_and_null(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """ADBC PostgreSQL applies cast-aware JSONB parameter preparation."""
    result = adbc_postgres_parameter_session.execute(
        """
        INSERT INTO adbc_parameter_jsonb (id, metadata, config)
        VALUES ($1, $2::jsonb, $3::jsonb)
        RETURNING metadata ->> 'score' AS score, metadata ->> 'active' AS active, config
        """,
        (20, {"score": 100, "active": True}, None),
    )
    row = _first_row(result)

    assert row["score"] == "100"
    assert row["active"] == "true"
    assert row["config"] is None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgres_parameter_count_mismatch_with_pruned_null_raises(
    adbc_postgres_parameter_session: AdbcDriver,
) -> None:
    """ADBC validates parameter counts before NULL pruning can hide extras."""
    with pytest.raises(SQLSpecError) as exc_info:
        adbc_postgres_parameter_session.execute(
            "INSERT INTO adbc_parameter_items (id, name) VALUES ($1, $2)",
            (30, None, "extra"),
        )

    assert "parameter count mismatch" in str(exc_info.value).lower()


@pytest.mark.xdist_group("sqlite")
@xfail_if_driver_missing
def test_adbc_sqlite_qmark_parameters_preserve_nulls_and_boolean_values() -> None:
    """SQLite-backed ADBC keeps qmark placeholders and SQLite DDL semantics."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        with config.provide_session() as session:
            session.execute("""
                CREATE TABLE adbc_sqlite_parameter_items (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER,
                    active BOOLEAN
                )
            """)
            result = session.execute(
                "INSERT INTO adbc_sqlite_parameter_items (id, name, value, active) VALUES (?, ?, ?, ?)",
                (1, None, None, True),
            )
            row = _first_row(
                session.execute("SELECT name, value, active FROM adbc_sqlite_parameter_items WHERE id = ?", (1,))
            )

            assert result.rows_affected in (-1, 0, 1)
            assert row["name"] is None
            assert row["value"] is None
            assert row["active"] in (True, 1)
    finally:
        config.close_pool()


@pytest.mark.xdist_group("duckdb")
@xfail_if_driver_missing
def test_adbc_duckdb_numeric_parameters_with_backend_ddl() -> None:
    """DuckDB-backed ADBC accepts numeric placeholders with DuckDB table DDL."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        with config.provide_session() as session:
            session.execute("""
                CREATE TABLE adbc_duckdb_parameter_items (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    value INTEGER
                )
            """)
            for row in [(1, "low", 10), (2, "mid", 20), (3, "high", 30)]:
                session.execute("INSERT INTO adbc_duckdb_parameter_items (id, name, value) VALUES (?, ?, ?)", row)

            result = session.execute(
                "SELECT name, value FROM adbc_duckdb_parameter_items WHERE value >= $1 ORDER BY value",
                (20,),
            )

            assert result.get_data() == [{"name": "mid", "value": 20}, {"name": "high", "value": 30}]
    finally:
        config.close_pool()


@pytest.mark.xdist_group("duckdb")
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
@xfail_if_driver_missing
def test_adbc_duckdb_qmark_parameters_feed_native_arrow_result() -> None:
    """DuckDB-backed ADBC binds qmark parameters on the native Arrow path."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        with config.provide_session() as session:
            session.execute("""
                CREATE TABLE adbc_duckdb_arrow_parameters (
                    id INTEGER,
                    name VARCHAR,
                    value INTEGER
                )
            """)
            session.execute(
                """
                INSERT INTO adbc_duckdb_arrow_parameters VALUES
                    (1, 'low', 10),
                    (2, 'mid', 20),
                    (3, 'high', 30)
                """
            )

            result = session.select_to_arrow(
                "SELECT name, value FROM adbc_duckdb_arrow_parameters WHERE value > ? ORDER BY value",
                (10,),
            )
            frame = result.to_pandas()

            assert list(frame["name"]) == ["mid", "high"]
            assert list(frame["value"]) == [20, 30]
    finally:
        config.close_pool()
