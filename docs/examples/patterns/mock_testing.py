from __future__ import annotations

__all__ = ("test_mock_testing",)


def test_mock_testing() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.mock import MockSyncConfig

    # MockSyncConfig uses SQLite :memory: internally
    # but can transpile SQL from other dialects
    config = MockSyncConfig(target_dialect="postgres")
    spec = SQLSpec()
    spec.add_config(config)

    with spec.provide_session(config) as session:
        # Write SQL in PostgreSQL dialect - it gets transpiled to SQLite
        session.execute("CREATE TABLE users (  id INTEGER PRIMARY KEY,  name VARCHAR(100) NOT NULL)")
        session.execute("INSERT INTO users (name) VALUES ('Alice')")

        users = session.select("SELECT name FROM users")
        print(users)  # [{"name": "Alice"}]

        count = session.select_value("SELECT COUNT(*) FROM users")
        print(count)  # 1
    # end-example

    assert users == [{"name": "Alice"}]
    assert count == 1
