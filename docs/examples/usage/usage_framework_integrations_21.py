# start-example
class Database:
    _instance = None
    _spec = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._spec = SQLSpec()
            cls._config = cls._spec.add_config(
                AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"})
            )
        return cls._instance

    async def session(self):
        return self._spec.provide_session(self._config)

# Usage
db = Database()
async def example_usage():
    async with await db.session() as session:
        result = await session.execute("SELECT * FROM users")
# end-example

def test_stub():
    assert True
