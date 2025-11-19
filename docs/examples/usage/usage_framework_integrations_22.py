# start-example
__all__ = ("test_stub",)


# Prefer Litestar plugin over manual setup
from litestar import Litestar

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])
# end-example


def test_stub() -> None:
    assert app is not None
