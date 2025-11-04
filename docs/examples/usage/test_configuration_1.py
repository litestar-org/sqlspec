def test_sqlite_memory_db():
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    # Create SQLSpec instance
    spec = SQLSpec()

    # Add database configuration
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    # Use the database
    with spec.provide_session(db) as session:
        result = session.execute("SELECT 1")
        assert result.fetchone()[0] == 1

