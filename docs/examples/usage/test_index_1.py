from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
db = spec.add_config(SqliteConfig())
with spec.provide_session(db) as session:
    _ = session.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    _ = session.execute("""INSERT INTO users VALUES (1, 'alice')""")
with spec.provide_session(db) as session:
   result = session.execute("SELECT * FROM users WHERE id = ?", 1)
   user = result.one()



def test_index_1() -> None:

    assert user is not None
