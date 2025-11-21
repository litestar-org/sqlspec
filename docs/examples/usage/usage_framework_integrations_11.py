from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, FastAPI
from litestar.testing import AsyncTestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

__all__ = ("get_db_session", "get_user", "test_stub")


async def test_stub(postgres_service: PostgresService) -> None:
    # start-example
    app = FastAPI()

    async def get_db_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
        async with spec.provide_session(config) as session:
            yield session

    # Use in route handlers
    @app.get("/users/{user_id}")
    async def get_user(user_id: int, db: Annotated[AsyncDriverAdapterBase, Depends(get_db_session)]) -> dict:
        result = await db.execute("SELECT id, name, email FROM users_fi11 WHERE id = $1", user_id)
        return result.one()

    # end-example
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
    async with spec.provide_session(config) as session:
        await session.execute("""CREATE TABLE users_fi11(id integer primary key, name text, email text)""")
        await session.execute("""INSERT INTO users_fi11(id,name, email) VALUES (1,'toto','toto@example.com')""")

    async with AsyncTestClient(app) as client:
        response = await client.get("/users/1")
        assert response.json() == {"id": 1, "name": "toto", "email": "toto@example.com"}
