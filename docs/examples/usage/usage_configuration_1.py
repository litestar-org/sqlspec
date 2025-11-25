__all__ = ("test_sqlite_memory_db",)


def test_sqlite_memory_db() -> None:
    from rich import print

    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # Create SQLSpec instance
    db_manager = SQLSpec()

    # Add database configuration
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    # Use the database
    with db_manager.provide_session(db) as session:
        value = session.select_value("SELECT 1")
        print(value)
        # end-example
        assert value == 1
