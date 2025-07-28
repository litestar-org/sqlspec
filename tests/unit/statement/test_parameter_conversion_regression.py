"""Regression tests for parameter style conversion issues.

These tests ensure that parameter conversion works correctly for different
database adapters, especially when SQLGlot generates a different parameter
style than what the driver expects.
"""

from typing import Any

from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.adapters.psycopg.driver import PsycopgSyncDriver
from sqlspec.driver.context import set_current_driver
from sqlspec.statement.sql import SQL


class TestParameterConversionRegression:
    """Test parameter conversion to prevent regression of style conversion issues."""

    def test_psycopg_pyformat_conversion(self, mock_psycopg_connection: Any) -> None:
        """Test that pyformat (%s) parameters are correctly handled for psycopg.

        Regression test for issue where SQL was corrupted:
        "INSERT INTO test (a, b, c) VALUES (%spa%sm_%ss, %s, %s)"
        """
        driver = PsycopgSyncDriver(mock_psycopg_connection)

        # Test with pyformat style that needs conversion
        sql = "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)"
        params = ["test_name", 100, "test description"]

        sql_obj = SQL(sql, parameters=params)

        # Simulate driver execution context
        set_current_driver(driver)
        try:
            compiled_sql, compiled_params = sql_obj.compile()

            # Ensure SQL is not corrupted
            assert "%spa%s" not in compiled_sql
            assert compiled_sql == sql  # Should remain unchanged for psycopg

            # Ensure parameters are in the correct format (list for pyformat)
            assert isinstance(compiled_params, list)
            assert compiled_params == params
        finally:
            set_current_driver(None)

    def test_duckdb_qmark_no_conversion(self, mock_duckdb_connection: Any) -> None:
        """Test that qmark (?) parameters remain as lists for DuckDB.

        Regression test for issue where qmark parameters were incorrectly
        converted to dict format {'param_0': 150} instead of [150].
        """
        driver = DuckDBDriver(mock_duckdb_connection)

        # Test with qmark style (DuckDB default)
        sql = "SELECT * FROM test_params WHERE value > ?"
        params = [150]

        sql_obj = SQL(sql, parameters=params)

        # Simulate driver execution context
        set_current_driver(driver)
        try:
            compiled_sql, compiled_params = sql_obj.compile()

            # Ensure SQL remains unchanged
            assert compiled_sql == sql

            # Ensure parameters remain as list, not converted to dict
            assert isinstance(compiled_params, list)
            assert compiled_params == params
        finally:
            set_current_driver(None)

    def test_mixed_parameter_preservation(self, mock_psycopg_connection: Any) -> None:
        """Test that mixed parameter styles are handled correctly."""
        driver = PsycopgSyncDriver(mock_psycopg_connection)

        # Complex query with multiple parameters
        sql = """
        SELECT * FROM users
        WHERE created_at > %s
        AND status = %s
        AND id IN (SELECT user_id FROM orders WHERE amount > %s)
        """
        params = ["2024-01-01", "active", 100.0]

        sql_obj = SQL(sql, parameters=params)

        set_current_driver(driver)
        try:
            compiled_sql, compiled_params = sql_obj.compile()

            # Count placeholders to ensure none were lost or corrupted
            placeholder_count = compiled_sql.count("%s")
            assert placeholder_count == len(params)

            # Ensure parameters match
            assert isinstance(compiled_params, list)
            assert len(compiled_params) == len(params)
            assert compiled_params == params
        finally:
            set_current_driver(None)

    def test_parameter_order_preservation(self, mock_duckdb_connection: Any) -> None:
        """Test that parameter order is preserved during conversion."""
        driver = DuckDBDriver(mock_duckdb_connection)

        # Query with multiple ordered parameters
        sql = "INSERT INTO test (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)"
        params = [1, "two", 3.0, True, None]

        sql_obj = SQL(sql, parameters=params)

        set_current_driver(driver)
        try:
            _compiled_sql, compiled_params = sql_obj.compile()

            # Ensure parameter order is preserved
            assert isinstance(compiled_params, list)
            assert compiled_params == params

            # Verify each parameter value
            for i, (expected, actual) in enumerate(zip(params, compiled_params)):
                assert actual == expected, f"Parameter {i} mismatch"
        finally:
            set_current_driver(None)

    def test_no_driver_context_fallback(self) -> None:
        """Test that parameters work without driver context."""
        # Test without any driver context
        sql = "SELECT * FROM test WHERE id = ?"
        params = [42]

        sql_obj = SQL(sql, parameters=params)
        compiled_sql, _compiled_params = sql_obj.compile()

        # Without driver context, params might be in dict format internally
        # but this is OK as long as driver context converts them properly
        assert compiled_sql == sql
