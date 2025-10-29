from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

# Create SQLSpec instance and configure database
db_manager = SQLSpec()
db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

# Execute a query
with db_manager.provide_session(db) as session:
    result = session.execute("SELECT 'Hello, SQLSpec!' as message")
    print(result.get_first())  # {'message': 'Hello, SQLSpec!'}


def test_quickstart_1() -> None:
    assert result.get_first() == {"message": "Hello, SQLSpec!"}
