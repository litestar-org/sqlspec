# start-example
from sanic import Sanic, Request, json
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

app = Sanic("MyApp")

# Initialize SQLSpec
spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"}))

# Store in app context
app.ctx.sqlspec = spec
app.ctx.db_config = db

# Cleanup on shutdown
@app.before_server_stop
async def close_db(app, loop):
    await app.ctx.sqlspec.close_all_pools()
# end-example

def test_stub():
    assert app is not None
