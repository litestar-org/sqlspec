from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, FastAPI

from sqlspec.driver import AsyncDriverAdapterBase

__all__ = ("get_db_session", "get_user", "test_stub")


# start-example
app = FastAPI()


async def get_db_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
    async with spec.provide_session(config) as session:
        yield session


# Use in route handlers
@app.get("/users/{user_id}")
async def get_user(user_id: int, db: Annotated[AsyncDriverAdapterBase, Depends(get_db_session)]) -> dict:
    result = await db.execute("SELECT id, name, email FROM users WHERE id = $1", user_id)
    return result.one()


# end-example


def test_stub() -> None:
    assert True
