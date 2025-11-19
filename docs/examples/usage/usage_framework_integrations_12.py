# start-example
__all__ = ("create_user", "test_stub" )


@app.post("/users")
async def create_user(user_data: dict, db: AsyncDriverAdapterBase = Depends(get_db_session)) -> dict:
    async with db.begin_transaction():
        result = await db.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", user_data["name"], user_data["email"]
        )

        user_id = result.scalar()

        # Additional operations in same transaction
        await db.execute("INSERT INTO audit_log (action, user_id) VALUES ($1, $2)", "user_created", user_id)

        return result.one()


# end-example


def test_stub() -> None:
    assert True
