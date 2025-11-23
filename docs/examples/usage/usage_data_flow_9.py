"""Example 9: Driver Execution with session."""

__all__ = ("test_driver_execution",)


def test_driver_execution() -> None:
    """Test driver execution pattern."""
    from rich import print

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # start-example
    # Driver receives compiled SQL and parameters
    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
    with db_manager.provide_session(db) as session:
        message = session.select_value("SELECT 'test' as message")
        print(message)
    # end-example

    assert message == "test"
