import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsync, PsycopgAsyncPool, PsycopgSync, PsycopgSyncPool


@pytest.mark.asyncio
async def test_async_driver(postgres_service: PostgresService) -> None:
    """Test async driver components."""
    adapter = PsycopgAsync(
        pool_config=PsycopgAsyncPool(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}"
        )
    )

    # Test provide_session
    async with adapter.provide_session() as session:
        assert session is not None

        # Test execute_script
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
        """
        await session.execute_script(create_table_sql)

        try:
            # Test insert_update_delete
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING id, name
            """
            result = await session.insert_update_delete_returning(insert_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"
            assert result["id"] is not None

            # Test select
            select_sql = "SELECT id, name FROM test_table"
            results = await session.select(select_sql)
            assert results is not None
            assert isinstance(results, list)
            assert len(results) == 1
            assert results[0]["name"] == "test_name"

            # Test select_one
            select_one_sql = "SELECT id, name FROM test_table WHERE name = :name"
            result = await session.select_one(select_one_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"

        finally:
            # Clean up
            await session.execute_script("DROP TABLE IF EXISTS test_table", {})


def test_sync_driver(postgres_service: PostgresService) -> None:
    """Test sync driver components."""
    adapter = PsycopgSync(
        pool_config=PsycopgSyncPool(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        )
    )

    # Test provide_session
    with adapter.provide_session() as session:
        assert session is not None

        # Test execute_script
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
        """
        session.execute_script(create_table_sql)

        try:
            # Test insert_update_delete
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING id, name
            """
            result = session.insert_update_delete_returning(insert_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"
            assert result["id"] is not None

            # Test select
            select_sql = "SELECT id, name FROM test_table"
            results = session.select(select_sql)
            assert len(results) == 1
            assert results[0]["name"] == "test_name"

            # Test select_one
            select_one_sql = "SELECT id, name FROM test_table WHERE name = :name"
            result = session.select_one(select_one_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"

        finally:
            # Clean up
            session.execute_script("DROP TABLE IF EXISTS test_table", {})
