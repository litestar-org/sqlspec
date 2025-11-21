# start-example
from litestar.testing import AsyncTestClient

__all__ = ("create_user", "test_stub")

from collections.abc import AsyncGenerator
from typing import Annotated, Any

from fastapi import Depends, FastAPI
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

    async def get_db_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
        async with spec.provide_session(config) as session:
            yield session

    @app.post("/users")
    async def create_user(
        user_data: dict[str, str], db: Annotated[AsyncDriverAdapterBase, Depends(get_db_session)]
    ) -> dict[str, Any]:
        async with db.begin_transaction():
            result = await db.execute(
                "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", user_data["name"], user_data["email"]
            )

            user_id = result.scalar()

            # Additional operations in same transaction
            await db.execute("INSERT INTO audit_log (action, user_id) VALUES ($1, $2)", "user_created", user_id)

            return result.one()

    # end-example

    async with AsyncTestClient(app) as client:
        await client.post("/users", json={"name": "bernard", "email": "bernard@example.com"})
