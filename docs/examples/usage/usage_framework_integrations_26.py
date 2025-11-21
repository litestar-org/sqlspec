# start-example
__all__ = ("UserRepository", "get_user", "test_stub")

# Good: Separate repository layer
from collections.abc import AsyncGenerator
from typing import Annotated, Any

from fastapi import Depends, FastAPI
from litestar.testing import AsyncTestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec import AsyncDriverAdapterBase, SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def test_stub(postgres_service: PostgresService) -> None:

    app = FastAPI()
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
    async with spec.provide_session(config) as session:
        await session.execute("""create table users_fi26(id integer primary key, name text, email text)""")
        await session.execute("""insert into users_fi26(id,name, email) values (1,'dora','dora@example.com')""")

    async def get_db() -> AsyncGenerator[AsyncDriverAdapterBase]:
        async with spec.provide_session(config) as session:
            yield session

    class UserRepository:
        def __init__(self, db: AsyncDriverAdapterBase) -> None:
            self.db = db

        async def get_user(self, user_id: int) -> dict[str, Any]:
            result = await self.db.execute("SELECT * FROM users_fi26 WHERE id = $1", user_id)
            return result.one()

    # Use in handlers
    @app.get("/users/{user_id}")
    async def get_user(user_id: int, db: Annotated[AsyncDriverAdapterBase, Depends(get_db)]) -> dict[str, Any]:
        repo = UserRepository(db)
        return await repo.get_user(user_id)

    # end-example

    async with AsyncTestClient(app) as client:
        response = await client.get("/users/1")
        assert response.json() == {"id": 1, "name": "dora", "email": "dora@example.com"}
