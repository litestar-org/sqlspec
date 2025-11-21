# start-example
from typing import Any

from asyncpg import Connection, Pool
from litestar import Litestar, get
from litestar.testing import AsyncTestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("client", "get_user", "health_check", "stats", "test_stub")


# Inject database session
@get("/users/{user_id:int}")
async def get_user(user_id: int, db_session: AsyncDriverAdapterBase) -> dict:
    result = await db_session.execute("SELECT id, name, email FROM users_fi2 WHERE id = $1", user_id)
    return result.one()


# Inject connection pool
@get("/health")
async def health_check(db_pool: Pool) -> dict[str, Any]:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval("SELECT 1")
        return {"status": "healthy" if result == 1 else "unhealthy"}


# Inject raw connection
@get("/stats")
async def stats(db_connection: Connection) -> dict[str, Any]:
    result = await db_connection.fetchval("SELECT COUNT(*) FROM users_fi2")
    return {"user_count": result}


# end-example


async def test_stub(postgres_service: PostgresService) -> None:
    spec = SQLSpec()
    config = AsyncpgConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )
    spec.add_config(config)

    sqlspec_plugin = SQLSpecPlugin(sqlspec=spec)
    async with spec.provide_session(config) as session:
        await session.execute("""CREATE TABLE users_fi2(id integer primary key, name text, email text)""")
        await session.execute("""insert into users_fi2(id, name, email) values (1, 'Alice', 'alice@example.com')""")
    app = Litestar(route_handlers=[health_check, stats, get_user], plugins=[sqlspec_plugin], debug=True)
    async with AsyncTestClient(app) as client:
        response = await client.get("/health")
        assert response.json() == {"status": "healthy"}
        response = await client.get("/stats")
        assert response.json() == {"user_count": 1}
        response = await client.get("/users/1")
        assert response.json() == {"email": "alice@example.com", "id": 1, "name": "Alice"}
