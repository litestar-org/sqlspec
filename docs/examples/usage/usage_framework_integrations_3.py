from litestar import Litestar, post
from litestar.testing import AsyncTestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("create_user", "test_stub")


# start-example
spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {"commit_mode": "manual"}  # Default
        },
    )
)


@post("/users")
async def create_user(data: dict[str, str], db_session: AsyncDriverAdapterBase) -> dict:
    async with db_session.begin_transaction():
        result = await db_session.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", data["name"], data["email"]
        )
        r = result.one()
        print(r)
        return r


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
        await session.execute("""CREATE TABLE users_fi3(id integer primary key, name text, email text)""")
        await session.execute("""insert into users_fi3(id, name, email) values (1, 'Alice', 'alice@example.com')""")
    app = Litestar(route_handlers=[create_user], plugins=[sqlspec_plugin], debug=True)
    async with AsyncTestClient(app) as client:
        response = await client.post("/users", json={"name": "toto", "email": "toto@example.com"})
        assert response.json() == {"id": 1, "name": "toto", "email": "toto@example.com"}
