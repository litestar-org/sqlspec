# start-example
from litestar import post
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {"commit_mode": "manual"}  # Default
        }
    )
)

@post("/users")
async def create_user(
    data: dict,
    db_session: AsyncDriverAdapterBase
) -> dict:
    async with db_session.begin_transaction():
        result = await db_session.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
            data["name"],
            data["email"]
        )
        return result.one()
# end-example

def test_stub():
    assert True
