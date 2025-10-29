from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig


class User(BaseModel):
    id: int
    name: str
    email: str


async def test_quickstart_5() -> None:
    db_manager = SQLSpec()
    db = db_manager.add_config(
        AsyncpgConfig(
            pool_config={
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "database": "mydb",
            }
        )
    )

    async with db_manager.provide_session(db) as session:
        # PostgreSQL uses $1, $2 for parameters instead of ?
        user = await session.select_one("SELECT * FROM users WHERE id = $1", 1, schema_type=User)
        print(f"User: {user.name}")
