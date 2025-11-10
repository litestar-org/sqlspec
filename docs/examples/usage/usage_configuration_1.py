__all__ = ("test_sqlite_memory_db",)


def test_sqlite_memory_db() -> None:

    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # Create SQLSpec instance
    db_manager = SQLSpec()

    # Add database configuration
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    # Use the database
    with db_manager.provide_session(db) as session:
        result = session.execute("SELECT 1")
    # end-example
        assert result[0] == {"1": 1}

