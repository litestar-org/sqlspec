"""Integration tests for builder claim statements and VALUES CTE updates on SQLite."""

from collections.abc import Generator

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture
def sqlite_tasks_session() -> Generator[SqliteDriver, None, None]:
    """Provide a fresh SQLite in-memory session with a tasks table and three pending rows."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute_script("""
                CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL
                );
                INSERT INTO tasks (id, status) VALUES (1, 'pending'), (2, 'pending'), (3, 'pending');
            """)
            yield driver
    finally:
        config.close_pool()


def test_claim_one_row(sqlite_tasks_session: SqliteDriver) -> None:
    """Test claiming exactly one row using UPDATE FROM with a subquery and RETURNING."""
    subquery = sql.select("id").from_("tasks").where_eq("status", "pending").limit(1)
    claim_query = (
        sql.update("tasks")
        .set(status="processing")
        .from_(subquery, alias="sub")
        .where("tasks.id = sub.id")
        .returning("id")
    )

    result = sqlite_tasks_session.execute(claim_query)
    claimed_rows = result.data
    assert len(claimed_rows) == 1
    claimed_id = claimed_rows[0][0]
    assert claimed_id == 1

    all_rows = sqlite_tasks_session.execute("SELECT id, status FROM tasks ORDER BY id").data
    assert all_rows == [(1, "processing"), (2, "pending"), (3, "pending")]


def test_values_cte_bulk_update(sqlite_tasks_session: SqliteDriver) -> None:
    """Test bulk updating rows using a sql.values() Common Table Expression."""
    val_cte = sql.values([(1, "completed"), (2, "failed")], alias="v", columns=["id", "status"])
    bulk_update = (
        sql.update("tasks")
        .with_cte("v", val_cte)
        .set(status=exp.column("status", table="v"))
        .from_("v")
        .where("tasks.id = v.id")
    )

    sqlite_tasks_session.execute(bulk_update)
    all_rows = sqlite_tasks_session.execute("SELECT id, status FROM tasks ORDER BY id").data
    assert all_rows == [(1, "completed"), (2, "failed"), (3, "pending")]
