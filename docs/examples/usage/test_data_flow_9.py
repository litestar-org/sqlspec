"""Example 9: Driver Execution with session."""


def test_driver_execution() -> None:
    """Test driver execution pattern."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # start-example
    # Driver receives compiled SQL and parameters
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        result = session.execute("SELECT 'test' as message")
    # end-example

    # Verify result was returned
    assert result is not None

