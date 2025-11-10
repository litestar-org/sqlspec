"""Example 11: SQLResult Object."""

__all__ = ("test_sql_result_object",)


def test_sql_result_object() -> None:
    """Test accessing SQLResult object properties."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    # start-example
    with db_manager.provide_session(db) as session:
        result = session.execute("SELECT 'test' as col1, 'value' as col2")

        # Access result data
        result.data  # List of dictionaries
        result.rows_affected  # Number of rows modified (INSERT/UPDATE/DELETE)
        result.column_names  # Column names for SELECT
        result.operation_type  # "SELECT", "INSERT", "UPDATE", "DELETE", "SCRIPT"
    # end-example

    # Verify result properties
    assert result.data is not None
    assert result.column_names is not None
