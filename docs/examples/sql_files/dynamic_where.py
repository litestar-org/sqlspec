from __future__ import annotations

from pathlib import Path

__all__ = ("test_dynamic_where",)


def test_dynamic_where(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    # Register a base query
    spec.add_named_sql("list_users", "select id, name, status from users")

    with spec.provide_session(config) as session:
        session.execute("create table users (id integer primary key, name text, status text)")
        session.execute(
            "insert into users (name, status) values ('Alice', 'active'), ('Bob', 'inactive'), ('Charlie', 'active')"
        )

        # Get the base SQL object and chain .where() calls
        base_query = spec.get_sql("list_users")

        # Add a WHERE clause dynamically
        active_query = base_query.where("status = 'active'")
        active_users = session.select(active_query)
        print(active_users)  # [{"id": 1, ...}, {"id": 3, ...}]

        # Use typed where helpers
        filtered = base_query.where_eq("name", "Bob")
        bob = session.select(filtered)
        print(bob)  # [{"id": 2, "name": "Bob", "status": "inactive"}]

        # Chain multiple where conditions (AND)
        specific = base_query.where_eq("status", "active").where_like("name", "A%")
        result = session.select(specific)
        print(result)  # [{"id": 1, "name": "Alice", "status": "active"}]
    # end-example

    assert len(active_users) == 2
    assert len(bob) == 1
    assert len(result) == 1
