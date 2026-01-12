"""Integration tests for CockroachDB psycopg driver implementations."""

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver

pytestmark = pytest.mark.xdist_group("cockroachdb")


@pytest.fixture
def cockroach_sync_session(cockroach_sync_driver: CockroachPsycopgSyncDriver) -> CockroachPsycopgSyncDriver:
    """Prepare test table for sync driver."""
    cockroach_sync_driver.execute_script("DROP TABLE IF EXISTS test_table")
    cockroach_sync_driver.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT PRIMARY KEY DEFAULT unique_rowid(),
            name STRING NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return cockroach_sync_driver


@pytest.fixture
async def cockroach_async_session(cockroach_async_driver: CockroachPsycopgAsyncDriver) -> CockroachPsycopgAsyncDriver:
    """Prepare test table for async driver."""
    await cockroach_async_driver.execute_script("DROP TABLE IF EXISTS test_table")
    await cockroach_async_driver.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT PRIMARY KEY DEFAULT unique_rowid(),
            name STRING NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return cockroach_async_driver


def test_cockroach_sync_basic_crud(cockroach_sync_session: CockroachPsycopgSyncDriver) -> None:
    """Test basic CRUD operations on Cockroach sync driver."""
    insert_result = cockroach_sync_session.execute(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", "test_user", 42
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = cockroach_sync_session.execute("SELECT name, value FROM test_table WHERE name = %s", "test_user")
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "test_user"
    assert select_result.data[0]["value"] == 42

    update_result = cockroach_sync_session.execute("UPDATE test_table SET value = %s WHERE name = %s", 100, "test_user")
    assert update_result.rows_affected == 1

    delete_result = cockroach_sync_session.execute("DELETE FROM test_table WHERE name = %s", "test_user")
    assert delete_result.rows_affected == 1


async def test_cockroach_async_basic_crud(cockroach_async_session: CockroachPsycopgAsyncDriver) -> None:
    """Test basic CRUD operations on Cockroach async driver."""
    insert_result = await cockroach_async_session.execute(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", "test_user", 42
    )
    assert insert_result.num_rows == 1

    select_result = await cockroach_async_session.execute(
        "SELECT name, value FROM test_table WHERE name = %s", "test_user"
    )
    data = select_result.get_data()
    assert data[0]["name"] == "test_user"
    assert data[0]["value"] == 42

    update_result = await cockroach_async_session.execute(
        "UPDATE test_table SET value = %s WHERE name = %s", 100, "test_user"
    )
    assert update_result.num_rows == 1

    delete_result = await cockroach_async_session.execute("DELETE FROM test_table WHERE name = %s", "test_user")
    assert delete_result.num_rows == 1
