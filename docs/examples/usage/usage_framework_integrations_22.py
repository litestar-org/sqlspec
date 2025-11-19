# start-example
__all__ = ("test_stub", )


# Prefer Litestar plugin over manual setup
spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])
# end-example


def test_stub() -> None:
    assert app is not None
