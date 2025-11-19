# start-example
# Use autocommit for simple CRUD
spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {"commit_mode": "autocommit"}
        }
    )
)

# Manual transactions for complex operations
async def manual_transaction(db_session):
    async with db_session.begin_transaction():
        # Multiple operations
        pass
# end-example

def test_stub():
    assert True
