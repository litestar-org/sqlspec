"""Example 11: SQLResult Object."""

__all__ = ("test_sql_result_object",)


def test_sql_result_object() -> None:
    """Test accessing SQLResult object properties."""
    from rich import print

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    # start-example
    with db_manager.provide_session(db) as session:
        result = session.select("SELECT 'test' as col1, 'value' as col2")
        print(len(result))  # length of result set

        # Access result data (SQLResult is iterable over rows)
        print(result)
    # end-example

    # Verify result properties
    assert result is not None
    assert len(result) == 1
