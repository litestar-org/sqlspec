"""Integration tests for psycopg async driver statement-stack pipelines."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import StatementStack
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
async def psycopg_async_session(postgres_service: "PostgresService") -> AsyncGenerator[PsycopgAsyncDriver, None]:
    """Create a psycopg async session with test table."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "autocommit": True,
        }
    )

    pool = await config.create_pool()
    config.connection_instance = pool

    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table_psycopg_async (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0
                )
            """)
            yield session

            try:
                await session.execute_script("DROP TABLE IF EXISTS test_table_psycopg_async")
            except Exception:
                pass
    finally:
        await config.close_pool()


async def test_psycopg_async_statement_stack_pipeline(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """Validate that StatementStack leverages async pipeline mode."""

    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")

    stack = (
        StatementStack()
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (1, "async-stack-one", 50)
        )
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (2, "async-stack-two", 60)
        )
        .push_execute("SELECT COUNT(*) AS total FROM test_table_psycopg_async WHERE name LIKE %s", ("async-stack-%",))
    )

    results = await psycopg_async_session.execute_stack(stack)

    assert len(results) == 3
    verify = await psycopg_async_session.execute(
        "SELECT COUNT(*) AS total FROM test_table_psycopg_async WHERE name LIKE %s", ("async-stack-%",)
    )
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


async def test_psycopg_async_statement_stack_continue_on_error(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """Ensure async pipeline honors continue-on-error semantics."""

    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")

    stack = (
        StatementStack()
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (1, "async-stack-initial", 15)
        )
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)",
            (1, "async-stack-duplicate", 25),
        )
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (2, "async-stack-final", 35)
        )
    )

    results = await psycopg_async_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = await psycopg_async_session.execute("SELECT COUNT(*) AS total FROM test_table_psycopg_async")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2
