"""Integration tests for CockroachDB asyncpg driver implementation."""

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver

pytestmark = pytest.mark.xdist_group("cockroachdb")


@pytest.fixture
async def cockroach_asyncpg_session(cockroach_asyncpg_driver: CockroachAsyncpgDriver) -> CockroachAsyncpgDriver:
    """Prepare test table for asyncpg driver."""
    await cockroach_asyncpg_driver.execute_script("DROP TABLE IF EXISTS test_table")
    await cockroach_asyncpg_driver.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT PRIMARY KEY DEFAULT unique_rowid(),
            name STRING NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return cockroach_asyncpg_driver


async def test_cockroach_asyncpg_basic_crud(cockroach_asyncpg_session: CockroachAsyncpgDriver) -> None:
    """Test basic CRUD operations on Cockroach asyncpg driver."""
    insert_result = await cockroach_asyncpg_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", "test_user", 42
    )
    assert insert_result.num_rows == 1

    select_result = await cockroach_asyncpg_session.execute(
        "SELECT name, value FROM test_table WHERE name = $1", "test_user"
    )
    data = select_result.get_data()
    assert data[0]["name"] == "test_user"
    assert data[0]["value"] == 42

    update_result = await cockroach_asyncpg_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2", 100, "test_user"
    )
    assert update_result.num_rows == 1

    delete_result = await cockroach_asyncpg_session.execute("DELETE FROM test_table WHERE name = $1", "test_user")
    assert delete_result.num_rows == 1
