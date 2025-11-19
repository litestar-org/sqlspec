# start-example
# Inject sessions, not global instances
async def get_db():
    async with spec.provide_session(config) as session:
        yield session
# end-example

def test_stub():
    assert True
