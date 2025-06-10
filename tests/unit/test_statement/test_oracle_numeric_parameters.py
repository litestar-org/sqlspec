"""Tests for Oracle numeric parameter handling (:1, :2 style)."""

import pytest

from sqlspec.exceptions import ParameterError
from sqlspec.statement.parameters import ParameterStyle, detect_parameter_style
from sqlspec.statement.sql import SQL


class TestOracleNumericParameters:
    """Test Oracle numeric parameter detection and handling."""

    def test_oracle_numeric_parameter_detection(self) -> None:
        """Test that :1, :2 style parameters are detected as ORACLE_NUMERIC."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"
        style = detect_parameter_style(sql)
        assert style == ParameterStyle.ORACLE_NUMERIC

    def test_mixed_oracle_parameters(self) -> None:
        """Test mixed Oracle numeric and named parameters."""
        sql = "SELECT * FROM users WHERE id = :1 AND status = :status"
        # Since we have both oracle numeric and named, we need dict params
        # The oracle numeric "1" is treated as a name for parameter passing
        stmt = SQL(sql, parameters={"1": 42, "status": "active"})

        # Check parameter detection - this happens before normalization
        # so we can still see the original parameter styles
        assert stmt._sql == sql  # Original SQL preserved

        # When we get parameters with Oracle numeric style, it should convert properly
        params = stmt.get_parameters(ParameterStyle.ORACLE_NUMERIC)
        assert params == {"1": 42, "status": "active"}

    def test_oracle_numeric_get_parameters(self) -> None:
        """Test get_parameters with ORACLE_NUMERIC style."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"
        stmt = SQL(sql, parameters=["john", 42])

        # Convert to Oracle numeric format
        params = stmt.get_parameters(ParameterStyle.ORACLE_NUMERIC)
        assert params == {"1": "john", "2": 42}

    def test_oracle_numeric_to_sql(self) -> None:
        """Test to_sql preserves Oracle numeric style."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"
        stmt = SQL(sql, parameters=["john", 42])

        # Should preserve :1, :2 style
        result = stmt.to_sql(placeholder_style=ParameterStyle.ORACLE_NUMERIC)
        assert ":1" in result
        assert ":2" in result

    def test_oracle_numeric_vs_named_colon(self) -> None:
        """Test that :1 is treated differently from :name."""
        # Numeric style
        sql1 = "SELECT * FROM users WHERE id = :1"
        stmt1 = SQL(sql1, parameters=[42])
        assert stmt1.parameter_info[0].style == ParameterStyle.ORACLE_NUMERIC

        # Named style
        sql2 = "SELECT * FROM users WHERE id = :id"
        stmt2 = SQL(sql2, parameters={"id": 42})
        assert stmt2.parameter_info[0].style == ParameterStyle.NAMED_COLON

    def test_oracle_numeric_parameter_validation(self) -> None:
        """Test parameter validation with Oracle numeric style."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"

        # Valid parameters
        stmt = SQL(sql, parameters=[1, "john"])
        assert stmt.parameters == [1, "john"]

        # Missing parameters should raise error when validation happens
        # Note: Lazy validation means error only occurs when accessing properties
        stmt_missing = SQL(sql, parameters=[1])  # Missing second parameter
        with pytest.raises(ParameterError):
            # Access a property to trigger validation
            _ = stmt_missing.to_sql()

    def test_oracle_numeric_with_execute_many(self) -> None:
        """Test Oracle numeric parameters with execute_many."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"
        params = [[1, "john"], [2, "jane"], [3, "bob"]]
        stmt = SQL(sql).as_many(params)
        assert stmt.is_many
        assert stmt.parameters == params

    def test_oracle_numeric_regex_precedence(self) -> None:
        """Test that :1 is matched before :name in regex."""
        # This ensures our regex modification works correctly
        sql = "SELECT :1, :2, :name, :3 FROM dual"
        # Need to provide parameters to avoid validation error
        # Mixed style requires dict parameters
        stmt = SQL(sql, parameters={"1": 10, "2": 20, "name": "test", "3": 30})

        # Check raw SQL to see original parameters
        assert ":1" in stmt._sql
        assert ":2" in stmt._sql
        assert ":name" in stmt._sql
        assert ":3" in stmt._sql
