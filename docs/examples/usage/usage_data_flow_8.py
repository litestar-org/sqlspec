"""Example 8: Statement Execution."""

__all__ = ("test_statement_execution",)


def test_statement_execution() -> None:
    """Execute a compiled SQL object through SQLSpec."""
    from sqlspec import SQL, SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    # start-example
    sql_statement = SQL("SELECT ? AS message", "pipeline-complete")

    with db_manager.provide_session(db) as session:
        result = session.execute(sql_statement)
        print(result.rows_affected)
        print(result.parameters)
        message = result.scalar()
    # end-example

    assert message == "pipeline-complete"
