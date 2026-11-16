"""Live check that reset_session_on_release clears session state on a reused asyncpg connection."""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")


async def test_asyncpg_reset_clears_temporary_table_and_setting(postgres_service: "PostgresService") -> None:
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
            "min_size": 1,
            "max_size": 1,
        },
        driver_features={"reset_session_on_release": True},
    )
    try:
        async with config.provide_session() as session:
            await session.execute("CREATE TEMPORARY TABLE reset_probe (id int)")
            await session.execute("SET application_name = 'probe'")
            first_pid = await session.select_value("SELECT pg_backend_pid()")
        async with config.provide_session() as session:
            assert await session.select_value("SELECT pg_backend_pid()") == first_pid
            assert await session.select_value("SELECT to_regclass('pg_temp.reset_probe')") is None
            assert await session.select_value("SHOW application_name") != "probe"
    finally:
        await config.close_pool()
