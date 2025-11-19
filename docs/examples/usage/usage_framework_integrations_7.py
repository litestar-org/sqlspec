# start-example
from litestar import Litestar, get

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("generate_report", "test_stub")


spec = SQLSpec()

# Main database
main_db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/main"},
        extension_config={"litestar": {"session_key": "main_db", "connection_key": "main_db_connection"}},
    )
)

# Analytics database
analytics_db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/analytics"},
        extension_config={"litestar": {"session_key": "analytics_db", "connection_key": "analytics_connection"}},
    )
)

# Create single plugin with all configs
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])


# Use in handlers
@get("/report")
async def generate_report(main_db: AsyncDriverAdapterBase, analytics_db: AsyncDriverAdapterBase) -> dict:
    users = await main_db.execute("SELECT COUNT(*) FROM users")
    events = await analytics_db.execute("SELECT COUNT(*) FROM events")
    return {"total_users": users.scalar(), "total_events": events.scalar()}


# end-example


def test_stub() -> None:
    assert True
