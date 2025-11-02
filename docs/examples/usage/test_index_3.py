from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.loader import SQLFileLoader

spec = SQLSpec()
db = spec.add_config(SqliteConfig())
with spec.provide_session(db) as session:
    _ = session.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT,
        active BOOLEAN NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _ = session.execute("""INSERT INTO users(id, username, email, active) VALUES (1, 'alice', 'alice
@example.com', 1),
                           (2, 'bob', 'bob@example.com', 0),
                           (3, 'carol', 'carol@examplecom', 1)""")


p = Path(__file__).parent.parent / "queries/users.sql"
assert p.exists()
loader = SQLFileLoader()
loader.load_sql(p)

user_query = loader.get_sql("get_user_by_id")
result = session.execute(user_query, user_id=2)


def test_index_3() -> None:
    assert result is not None
