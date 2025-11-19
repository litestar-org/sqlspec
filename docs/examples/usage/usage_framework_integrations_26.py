# start-example
# Good: Separate repository layer
class UserRepository:
    def __init__(self, db: AsyncDriverAdapterBase):
        self.db = db

    async def get_user(self, user_id: int):
        result = await self.db.execute(
            "SELECT * FROM users WHERE id = $1",
            user_id
        )
        return result.one()

# Use in handlers
@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    db: AsyncDriverAdapterBase = Depends(get_db)
):
    repo = UserRepository(db)
    return await repo.get_user(user_id)
# end-example

def test_stub():
    assert True
