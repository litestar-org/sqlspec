# start-example
__all__ = ("close_pools", "lifespan", "test_stub")


# FastAPI
from contextlib import asynccontextmanager

from fastapi import FastAPI

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await spec.close_all_pools()


# Sanic
@app.before_server_stop
async def close_pools(app, loop) -> None:
    await spec.close_all_pools()


# end-example


def test_stub() -> None:
    assert True
