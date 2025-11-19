# start-example
__all__ = ("test_stub",)


from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={"litestar": {"commit_mode": "autocommit_include_redirect"}},
    )
)
# end-example


def test_stub() -> None:
    assert True
