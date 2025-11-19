# start-example
__all__ = ("get_db", "test_stub" )


# Inject sessions, not global instances
async def get_db():
    async with spec.provide_session(config) as session:
        yield session


# end-example


def test_stub() -> None:
    assert True
