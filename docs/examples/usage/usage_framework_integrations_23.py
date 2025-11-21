__all__ = ("close_pools", "lifespan", "test_stub")
# start-example
# FastAPI
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sanic import Sanic

from sqlspec import SQLSpec

spec = SQLSpec()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await spec.close_all_pools()


# Sanic
app = Sanic("sqlspec")


@app.before_server_stop
async def close_pools(app, loop) -> None:
    await spec.close_all_pools()


# end-example


def test_stub() -> None:
    assert True
