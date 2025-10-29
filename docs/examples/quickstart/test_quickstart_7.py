from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

db_manager = SQLSpec()
db = db_manager.add_config(SqliteConfig(pool_config={"database": "mydb.db"}))

# Transaction committed on successful exit
with db_manager.provide_session(db) as session:
    _ = session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    _ = session.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_name TEXT NOT NULL
        )
    """)
    _ = session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    _ = session.execute("INSERT INTO orders (user_name) VALUES (?)", "Alice")
    # Both committed together

# Transaction rolled back on exception
try:
    with db_manager.provide_session(db) as session:
        _ = session.execute("INSERT INTO users (name) VALUES (?)", "Bob")
        msg = "Something went wrong!"
        raise ValueError(msg)  # noqa: TRY301
except ValueError:
    pass  # Transaction was rolled back automatically


def test_quickstart_7() -> None:
    # Verify that Alice was inserted
    with db_manager.provide_session(db) as session:
        alice = session.select_one("SELECT * FROM users WHERE name = ?", "Alice")
        bob = session.select_one("SELECT * FROM users WHERE name = ?", "Bob")

    assert alice is not None
    assert alice["name"] == "Alice"
    assert bob is None  # Bob's insertion was rolled back
