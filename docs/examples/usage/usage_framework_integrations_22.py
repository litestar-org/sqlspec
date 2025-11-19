# start-example
# Prefer Litestar plugin over manual setup
spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))
app = Litestar(plugins=[SQLSpecPlugin(sqlspec=spec)])
# end-example

def test_stub():
    assert app is not None
