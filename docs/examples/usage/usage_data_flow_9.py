"""Example 9: Driver Execution with session."""

__all__ = ("test_driver_execution",)


def test_driver_execution() -> None:
    """Test driver execution pattern."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # start-example
    # Driver receives compiled SQL and parameters
    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
    with db_manager.provide_session(db) as session:
        result = session.execute("SELECT 'test' as message")
    # end-example

    assert result is not None
