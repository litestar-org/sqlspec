"""Example 12: Convenience Methods."""


def test_convenience_methods() -> None:
    """Test SQLResult convenience methods."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        # Create a test table
        session.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        session.execute("INSERT INTO test VALUES (1, 'Alice')")

        # start-example
        result = session.execute("SELECT * FROM test WHERE id = 1")

        # Get exactly one row (raises if not exactly one)
        user = result.one()

        # Get one or None
        user = result.one_or_none()

        # Get scalar value (first column of first row)
        result2 = session.execute("SELECT COUNT(*) FROM test")
        count = result2.scalar()
        # end-example

        # Verify results
        assert user is not None
        assert count == 1

