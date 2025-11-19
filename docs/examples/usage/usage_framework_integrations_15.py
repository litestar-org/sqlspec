# start-example
__all__ = ("get_user", "test_stub" )


@app.get("/users/<user_id:int>")
async def get_user(request: Request, user_id: int):
    async with request.app.ctx.sqlspec.provide_session(request.app.ctx.db_config) as db:
        result = await db.execute("SELECT id, name, email FROM users WHERE id = $1", user_id)
        return json(result.one())


# end-example


def test_stub() -> None:
    assert True
