"""Integration tests for psycopg driver implementation."""

from collections.abc import Generator
from typing import Any, cast

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLResult, StatementStack
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig, PsycopgSyncDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_session(psycopg_sync_config: "PsycopgSyncConfig") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a psycopg session with test table."""

    with psycopg_sync_config.provide_session() as session:
        session.execute_script("DROP TABLE IF EXISTS test_table_psycopg_sync")
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_psycopg_sync (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )

        session.commit()
        session.begin()
        yield session

        try:
            session.rollback()
        except Exception:
            pass

        try:
            session.execute_script("DROP TABLE IF EXISTS test_table_psycopg_sync")
        except Exception:
            try:
                session.connection.rollback()
            except Exception:
                pass

        try:
            session.execute_script("DROP TABLE IF EXISTS test_table_psycopg_sync")
        except Exception:
            pass


async def test_psycopg_async_connection(psycopg_async_config: "PsycopgAsyncConfig") -> None:
    """Test async connection components."""
    async with await psycopg_async_config.create_connection() as conn:
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS id")
            result = cast("tuple[Any, ...]", await cur.fetchone())
            assert result[0] == 1

    async with psycopg_async_config.provide_connection() as conn:
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS value")
            result = cast("tuple[Any, ...]", await cur.fetchone())
            assert result[0] == 1


def test_psycopg_sync_connection(postgres_service: "PostgresService") -> None:
    """Test sync connection components."""
    conninfo = (
        f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} "
        f"password={postgres_service.password} dbname={postgres_service.database}"
    )
    sync_config = PsycopgSyncConfig(connection_config={"conninfo": conninfo})
    try:
        with sync_config.create_connection() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as id")
                result = cast("tuple[Any, ...]", cur.fetchone())
                assert result[0] == 1
    finally:
        sync_config.close_pool()

    another_config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgres://{postgres_service.user}:{postgres_service.password}@"
            f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        with another_config.provide_connection() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS id")
                result = cast("tuple[Any, ...]", cur.fetchone())
                assert result[0] == 1
    finally:
        another_config.close_pool()


def test_psycopg_prepare_threshold_routes_to_pooled_connection(postgres_service: "PostgresService") -> None:
    """Connection prepare_threshold should control server-side prepared statements."""
    conninfo = (
        f"postgres://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    query = "SELECT %s::int AS value"

    def prepared_statement_count(prepare_threshold: int | None) -> int:
        config = PsycopgSyncConfig(
            connection_config={
                "conninfo": conninfo,
                "min_size": 1,
                "max_size": 1,
                "prepare_threshold": prepare_threshold,
            }
        )
        try:
            with config.provide_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DEALLOCATE ALL", prepare=False)
                    cur.execute(query, (1,))
                    cur.fetchone()
                    cur.execute("SELECT COUNT(*) FROM pg_prepared_statements", prepare=False)
                    row = cast("tuple[int]", cur.fetchone())
                    return row[0]
        finally:
            config.close_pool()

    assert prepared_statement_count(0) >= 1
    assert prepared_statement_count(None) == 0


def test_psycopg_statement_stack_continue_on_error(psycopg_session: "PsycopgSyncDriver") -> None:
    """Pipeline execution should continue when instructed to handle errors."""

    psycopg_session.execute("DELETE FROM test_table_psycopg_sync")
    psycopg_session.commit()

    stack = (
        StatementStack()
        .push_execute(
            "INSERT INTO test_table_psycopg_sync (id, name, value) VALUES (%s, %s, %s)", (1, "sync-initial", 10)
        )
        .push_execute(  # duplicate PK triggers error
            "INSERT INTO test_table_psycopg_sync (id, name, value) VALUES (%s, %s, %s)", (1, "sync-duplicate", 20)
        )
        .push_execute(
            "INSERT INTO test_table_psycopg_sync (id, name, value) VALUES (%s, %s, %s)", (2, "sync-success-final", 30)
        )
    )

    results = psycopg_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None
    assert results[0].error is None
    assert results[2].error is None

    verify = psycopg_session.execute("SELECT COUNT(*) AS total FROM test_table_psycopg_sync")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


def test_psycopg_postgresql_specific_features(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL-specific features with psycopg."""

    returning_result = psycopg_session.execute(
        "INSERT INTO test_table_psycopg_sync (name, value) VALUES (%s, %s) RETURNING id, name", "returning_test", 999
    )
    assert isinstance(returning_result, SQLResult)
    assert returning_result.data is not None
    assert len(returning_result.data) == 1
    assert returning_result.get_data()[0]["name"] == "returning_test"

    psycopg_session.execute_many(
        "INSERT INTO test_table_psycopg_sync (name, value) VALUES (%s, %s)",
        [("window1", 10), ("window2", 20), ("window3", 30)],
    )

    window_result = psycopg_session.execute("""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            LAG(value) OVER (ORDER BY value) as prev_value
        FROM test_table_psycopg_sync
        WHERE name LIKE 'window%'
        ORDER BY value
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 3
    assert window_result.get_data()[0]["row_num"] == 1
    assert window_result.get_data()[0]["prev_value"] is None


def test_psycopg_json_operations(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL JSON operations with psycopg."""

    psycopg_session.execute_script("""
        CREATE TABLE IF NOT EXISTS json_test_psycopg_sync (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
        DELETE FROM json_test_psycopg_sync;
    """)

    json_data = {"name": "test", "age": 30, "tags": ["postgres", "json"]}
    psycopg_session.execute("INSERT INTO json_test_psycopg_sync (data) VALUES (%s)", (json_data,))

    json_result = psycopg_session.execute(
        "SELECT data->>'name' as name, data->>'age' as age FROM json_test_psycopg_sync"
    )
    assert isinstance(json_result, SQLResult)
    assert json_result.data is not None
    assert json_result.get_data()[0]["name"] == "test"
    assert json_result.get_data()[0]["age"] == "30"

    psycopg_session.execute_script("DROP TABLE json_test_psycopg_sync")


@pytest.mark.integration
def test_extensions_not_enabled_on_standard_postgres(psycopg_sync_config: "PsycopgSyncConfig") -> None:
    """Verify pgvector and paradedb extensions are not detected on standard postgres.

    Standard PostgreSQL does not have the 'vector' or 'pg_search' extensions installed,
    so the driver should detect this and keep the default 'postgres' dialect.
    """
    with psycopg_sync_config.provide_session() as session:
        session.execute("SELECT 1")

    assert psycopg_sync_config._pgvector_available is False  # pyright: ignore[reportPrivateUsage]
    assert psycopg_sync_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert psycopg_sync_config.statement_config.dialect == "postgres"
