"""Integration tests for psycopg async driver statement-stack pipelines and COPY."""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import StatementStack
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver
from sqlspec.exceptions import SQLSpecError

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


async def test_psycopg_async_statement_stack_fail_fast_reports_operation_index(
    psycopg_async_session: PsycopgAsyncDriver,
) -> None:
    """Async native pipeline fail-fast must surface StackExecutionError with the failing op index."""
    from sqlspec.exceptions import StackExecutionError

    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (1, "first", 10))
        .push_execute("INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (1, "dup", 20))
        .push_execute("INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", (2, "third", 30))
    )

    with pytest.raises(StackExecutionError) as excinfo:
        await psycopg_async_session.execute_stack(stack)

    assert excinfo.value.operation_index == 1


async def test_psycopg_async_statement_stack_accepts_every_parameter_shape(
    psycopg_async_session: PsycopgAsyncDriver,
) -> None:
    """Tuple, list, scalar, named and parameterless statements all run through the native pipeline."""
    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")

    insert = "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)"
    stack = (
        StatementStack()
        .push_execute(insert, (1, "tuple", 10))
        .push_execute(insert, [2, "list", 20])
        .push_execute(insert, 3, "scalars", 30)
        .push_execute(
            "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (:id, :name, :value)",
            id=4,
            name="named",
            value=40,
        )
        .push_execute("INSERT INTO test_table_psycopg_async (id, name, value) VALUES (5, 'literal', 50)")
        .push_execute("SELECT name FROM test_table_psycopg_async ORDER BY id")
    )

    results = await psycopg_async_session.execute_stack(stack)

    assert [result.rows_affected for result in results[:5]] == [1, 1, 1, 1, 1]
    assert results[5].get_result().get_data() == [
        {"name": "tuple"},
        {"name": "list"},
        {"name": "scalars"},
        {"name": "named"},
        {"name": "literal"},
    ]


async def test_psycopg_async_copy_to_stdout_returns_rows(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """COPY TO STDOUT issued through execute returns the exported text."""
    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")
    await psycopg_async_session.execute_many(
        "INSERT INTO test_table_psycopg_async (id, name, value) VALUES (%s, %s, %s)", [(1, "a", 10), (2, "b", 20)]
    )

    result = await psycopg_async_session.execute(
        "COPY (SELECT id, name, value FROM test_table_psycopg_async ORDER BY id) TO STDOUT"
    )

    assert result.get_data() == [{"copy_output": "1\ta\t10\n2\tb\t20\n"}]


async def test_psycopg_async_copy_from_stdin_reports_rejected_data(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """Data the server rejects when COPY finishes raises a mapped error and leaves the session usable."""
    await psycopg_async_session.execute_script("DELETE FROM test_table_psycopg_async")

    with pytest.raises(SQLSpecError):
        await psycopg_async_session.execute(
            "COPY test_table_psycopg_async (id, name, value) FROM STDIN", "not-a-number\ta\t1\n"
        )

    verify = await psycopg_async_session.execute("SELECT COUNT(*) AS total FROM test_table_psycopg_async")
    assert verify.get_data()[0]["total"] == 0


async def test_psycopg_async_statement_stack_cancellation_finishes_and_keeps_session_usable(
    psycopg_async_session: PsycopgAsyncDriver,
) -> None:
    """Cancelling a running pipeline ends the call promptly and the session works again after rollback."""
    stack = StatementStack().push_execute("SELECT pg_sleep(%s)", (30,))
    task = asyncio.ensure_future(psycopg_async_session.execute_stack(stack))
    await asyncio.sleep(0.5)

    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=15)

    assert task in done
    assert task.cancelled()
    await psycopg_async_session.rollback()
    verify = await psycopg_async_session.execute("SELECT 1 AS value")
    assert verify.get_data() == [{"value": 1}]
