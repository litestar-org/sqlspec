"""Integration tests for CockroachDB asyncpg driver implementation."""

from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig, CockroachAsyncpgDriver

if TYPE_CHECKING:
    from pytest_databases.docker.cockroachdb import CockroachDBService

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


async def test_cockroach_asyncpg_on_connection_create_hook(cockroachdb_service: "CockroachDBService") -> None:
    """Test on_connection_create callback is invoked for each connection."""
    hook_call_count = 0

    async def connection_hook(conn: Any) -> None:
        nonlocal hook_call_count
        hook_call_count += 1

    config = CockroachAsyncpgConfig(
        connection_config={
            "host": cockroachdb_service.host,
            "port": cockroachdb_service.port,
            "user": "root",
            "password": "",
            "database": cockroachdb_service.database,
            "ssl": None,
            "min_size": 1,
            "max_size": 2,
        },
        driver_features={"on_connection_create": connection_hook},
    )

    try:
        async with config.provide_session() as session:
            await session.execute("SELECT 1")
        assert hook_call_count >= 1, "Hook should be called at least once"
    finally:
        await config.close_pool()
