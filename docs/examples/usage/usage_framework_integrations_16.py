__all__ = ("add_db_session", "cleanup_db_session", "list_users", "test_stub")
# start-example
import json

from fastapi import FastAPI, Request

app = FastAPI()


@app.middleware("request")
async def add_db_session(request) -> None:
    request.ctx.db = await request.app.ctx.sqlspec.provide_session(request.app.ctx.db_config).__aenter__()


@app.middleware("response")
async def cleanup_db_session(request, response) -> None:
    if hasattr(request.ctx, "db"):
        await request.ctx.db.__aexit__(None, None, None)


# Use in handlers
@app.get("/users")
async def list_users(request: Request):
    result = await request.ctx.db.execute("SELECT * FROM users")
    return json(result.rows)


# end-example


def test_stub() -> None:
    assert True
