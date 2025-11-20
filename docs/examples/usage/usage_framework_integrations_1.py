__all__ = ("index", "test_app_exists")
# start-example
from litestar import Litestar, get

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

# Configure database and create plugin
spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/mydb", "min_size": 10, "max_size": 20}))
sqlspec_plugin = SQLSpecPlugin(sqlspec=spec)


@get("/")
async def index() -> str:
    return "integrated"


# Create Litestar app
app = Litestar(route_handlers=[index], plugins=[sqlspec_plugin])
# end-example


def test_app_exists() -> None:
    assert app is not None
