# start-example
from litestar import Litestar
from litestar.middleware.session.server_side import ServerSideSessionConfig

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("test_stub",)


# Configure database with session support
spec = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/db"},
    extension_config={"litestar": {"session_table": "litestar_sessions"}},
    migration_config={"script_location": "migrations", "include_extensions": ["litestar"]},
)
db = spec.add_config(config)

# Create session store using adapter-specific class
store = AsyncpgStore(config)

# Configure Litestar with plugin and session middleware
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)], middleware=[ServerSideSessionConfig(store=store).middleware])
# end-example


def test_stub() -> None:
    assert app is not None
