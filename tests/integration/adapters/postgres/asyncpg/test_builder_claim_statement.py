"""Integration tests for builder claim statements and VALUES CTE updates on PostgreSQL asyncpg."""

from collections.abc import AsyncGenerator

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.adapters.asyncpg import AsyncpgDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
async def asyncpg_tasks_session(asyncpg_async_driver: AsyncpgDriver) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create a tasks table with three pending rows for claim testing."""
    await asyncpg_async_driver.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test_builder_tasks (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        TRUNCATE test_builder_tasks;
        INSERT INTO test_builder_tasks (id, status) VALUES
            ('task-1', 'pending'),
            ('task-2', 'pending'),
            ('task-3', 'pending');
        """
    )
    try:
        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("DROP TABLE IF EXISTS test_builder_tasks")


async def test_claim_one_row(asyncpg_tasks_session: AsyncpgDriver) -> None:
    """Test claiming exactly one row using UPDATE FROM with a subquery, FOR UPDATE SKIP LOCKED, and RETURNING."""
    subquery = (
        sql
        .select("id")
        .from_("test_builder_tasks")
        .where_eq("status", "pending")
        .order_by("id")
        .limit(1)
        .for_update(skip_locked=True)
    )
    claim_query = (
        sql
        .update("test_builder_tasks")
        .set(status="processing")
        .from_(subquery, alias="sub")
        .where("test_builder_tasks.id = sub.id")
        .returning(sql.column("id", table="test_builder_tasks"))
    )

    result = await asyncpg_tasks_session.execute(claim_query)
    claimed_rows = result.data
    assert len(claimed_rows) == 1
    assert claimed_rows[0][0] == "task-1"

    all_rows = (await asyncpg_tasks_session.execute("SELECT id, status FROM test_builder_tasks ORDER BY id")).data
    assert all_rows == [("task-1", "processing"), ("task-2", "pending"), ("task-3", "pending")]


async def test_values_cte_bulk_update(asyncpg_tasks_session: AsyncpgDriver) -> None:
    """Test bulk updating rows using a sql.values() Common Table Expression."""
    val_cte = sql.values([("task-1", "completed"), ("task-2", "failed")], alias="v", columns=["id", "status"])
    bulk_update = (
        sql
        .update("test_builder_tasks")
        .with_cte("v", val_cte)
        .set(status=exp.column("status", table="v"))
        .from_("v")
        .where("test_builder_tasks.id = v.id")
    )

    await asyncpg_tasks_session.execute(bulk_update)
    all_rows = (await asyncpg_tasks_session.execute("SELECT id, status FROM test_builder_tasks ORDER BY id")).data
    assert all_rows == [("task-1", "completed"), ("task-2", "failed"), ("task-3", "pending")]


@pytest.mark.parametrize("source_alias", [None, "original"])
async def test_select_from_values_with_alias_override(
    asyncpg_async_driver: AsyncpgDriver, source_alias: str | None
) -> None:
    source = sql.values([("first", "alice"), ("second", "bob")], alias=source_alias, columns=["id", "name"])
    query = sql.select("renamed.id", "renamed.name").from_(source, alias="renamed").order_by("renamed.id")

    assert (await asyncpg_async_driver.execute(query)).data == [("first", "alice"), ("second", "bob")]


async def test_select_from_values_cte_preserves_column_names(asyncpg_async_driver: AsyncpgDriver) -> None:
    source = sql.values([("first", "alice"), ("second", "bob")], columns=["id", "name"])
    query = sql.select("id", "name").from_("v").with_cte("v", source).order_by("id")

    assert (await asyncpg_async_driver.execute(query)).data == [("first", "alice"), ("second", "bob")]
