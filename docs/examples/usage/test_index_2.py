from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
db = spec.add_config(SqliteConfig())
with spec.provide_session(db) as session:
    _ = session.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        active BOOLEAN NOT NULL DEFAULT 1
    )
    """)
    _ = session.execute("""INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 1),
                           (2, 'bob', 'bob@example.com', 0),
                           (3, 'carol', 'carol@examplecom', 1)""")

query = sql.select("id", "name", "email").from_("users").where("active = ?")
result = session.execute(query, True)  # noqa: FBT003
users = result.all()


def test_index_2() -> None:
    assert len(users) > 0
