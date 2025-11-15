"""Minimal smoke test for drivers_and_querying example 15."""

__all__ = ("test_example_15_placeholder",)


from sqlspec import SQLSpec


def test_example_15_placeholder() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()

    config = SqliteConfig(pool_config={"database": ":memory:"})  # Thread local pooling
    with spec.provide_session(config) as session:
        create_users_table_query = """create table if not exists users (id default int primary key, name varchar(128), email text, status varchar(32));
        """

        create_log_table_query = """create table if not exists logs (id default int primary key, action varchar(128), created_at datetime default current_timestamp);
        """
        _ = session.execute(create_users_table_query)
        _ = session.execute(create_log_table_query)
        # Batch examples are documentation-only

        # Batch insert
        session.execute_many(
            "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)",
            [
                (1, "Alice", "alice@example.com", "active"),
                (2, "Bob", "bob@example.com", "inactive"),
                (3, "Charlie", "charlie@example.com", "active"),
            ],
        )

        # start-example
        try:
            session.begin()
            session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
            session.execute("INSERT INTO logs (action) VALUES (?)", "user_created")
            session.commit()
        except Exception:
            session.rollback()
            raise
        # end-example
