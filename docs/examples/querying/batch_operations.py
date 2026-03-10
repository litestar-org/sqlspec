from __future__ import annotations

from pathlib import Path

__all__ = ("test_batch_operations",)


def test_batch_operations(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "batch.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table users (id integer primary key, name text, email text)")

        # execute_many inserts multiple rows in a single call
        session.execute_many(
            "insert into users (name, email) values (?, ?)",
            [
                ("Alice", "alice@example.com"),
                ("Bob", "bob@example.com"),
                ("Charlie", "charlie@example.com"),
            ],
        )

        # select_value returns a single scalar value
        count = session.select_value("select count(*) from users")
        print(count)  # 3

        # select_value with type conversion
        count_int = session.select_value("select count(*) from users", value_type=int)
        print(count_int)  # 3

        # select_value_or_none returns None when no rows match
        email = session.select_value_or_none(
            "select email from users where name = ?", "Nobody"
        )
        print(email)  # None

        # select_with_total for pagination
        from sqlspec.core import SQL

        query = SQL("select id, name from users").paginate(page=1, page_size=2)
        data, total = session.select_with_total(query)
        print(f"Page has {len(data)} rows, total matching: {total}")
    # end-example

    assert count == 3
    assert email is None
    assert total == 3
    assert len(data) == 2
