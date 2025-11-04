"""Example 10: SQLite Driver Execution implementation."""


def test_sqlite_driver_pattern() -> None:
    """Test SQLite driver execution pattern."""
    # start-example
    from sqlspec.driver._sync import SyncDriverAdapterBase

    class SqliteDriver(SyncDriverAdapterBase):
        def _execute_statement(self, cursor, statement):
            sql, params = self._get_compiled_sql(statement)
            cursor.execute(sql, params or ())
            return self.create_execution_result(cursor)

        def _execute_many(self, cursor, statement):
            sql, params = self._get_compiled_sql(statement)
            cursor.executemany(sql, params)
            return self.create_execution_result(cursor)
    # end-example

    # Verify class was defined
    assert SqliteDriver is not None

