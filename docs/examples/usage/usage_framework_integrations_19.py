# start-example
__all__ = ("DatabaseSession", "example_usage", "test_stub" )


class DatabaseSession:
    def __init__(self, spec: SQLSpec, config) -> None:
        self.spec = spec
        self.config = config
        self.session = None

    async def __aenter__(self):
        self.session = await self.spec.provide_session(self.config).__aenter__()
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.__aexit__(exc_type, exc_val, exc_tb)


# Usage
async def example_usage() -> None:
    async with DatabaseSession(spec, config) as db:
        await db.execute("SELECT * FROM users")


# end-example


def test_stub() -> None:
    assert True
