# start-example
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {
                "connection_key": "database",    # Default: "db_connection"
                "pool_key": "db_pool",           # Default: "db_pool"
                "session_key": "session",        # Default: "db_session"
            }
        }
    )
)

@get("/users")
async def list_users(session: AsyncDriverAdapterBase) -> list:
    result = await session.execute("SELECT * FROM users")
    return result.all()
# end-example

def test_stub():
    assert True
