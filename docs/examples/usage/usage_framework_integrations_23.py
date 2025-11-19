# start-example
# FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await spec.close_all_pools()

# Sanic
@app.before_server_stop
async def close_pools(app, loop):
    await spec.close_all_pools()
# end-example

def test_stub():
    assert True
