# start-example
__all__ = ("manual_transaction", "test_stub" )


# Use autocommit for simple CRUD
spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://..."}, extension_config={"litestar": {"commit_mode": "autocommit"}})
)


# Manual transactions for complex operations
async def manual_transaction(db_session) -> None:
    async with db_session.begin_transaction():
        # Multiple operations
        pass


# end-example


def test_stub() -> None:
    assert True
