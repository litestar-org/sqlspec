# start-example
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

# Configure database
spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={
            "dsn": "postgresql://localhost/mydb",
            "min_size": 10,
            "max_size": 20,
        }
    )
)

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    await spec.close_all_pools()

app = FastAPI(lifespan=lifespan)
# end-example

def test_stub():
    assert app is not None
