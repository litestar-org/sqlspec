"""Example 10: SQLite Driver Execution implementation."""

__all__ = ("test_sqlite_driver_pattern",)


def test_sqlite_driver_pattern() -> None:
    """Test SQLite driver execution pattern."""
    # start-example
    from typing import Any

    from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase

    class SqliteDriver(SyncDriverAdapterBase):
        def _execute_statement(self, cursor: Any, statement: Any) -> ExecutionResult:
            sql, params = self._get_compiled_sql(statement)
            cursor.execute(sql, params or ())
            return self.create_execution_result(cursor)

        def _execute_many(self, cursor: Any, statement: Any) -> ExecutionResult:
            sql, params = self._get_compiled_sql(statement)
            cursor.executemany(sql, params)
            return self.create_execution_result(cursor)

    # end-example

    # Verify class was defined
    assert SqliteDriver is not None
