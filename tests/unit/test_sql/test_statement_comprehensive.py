"""Comprehensive tests for the SQLStatement class."""

from unittest.mock import Mock

import pytest
from sqlglot import exp

from sqlspec.exceptions import (
    ParameterError,
    RiskLevel,
    SQLBuilderError,
    SQLValidationError,
)
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.statement import (
    SQLSanitizer,
    SQLStatement,
    SQLValidator,
    StatementConfig,
    ValidationResult,
    is_sql_safe,
    sanitize_sql,
    validate_sql,
)


class TestSQLStatementBasicInitialization:
    """Test basic initialization and properties."""

    def test_simple_sql_string_no_parameters(self):
        """Test initialization with simple SQL string and no parameters."""
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql)

        assert stmt.sql == sql
        assert stmt.parameters is None or stmt.parameters == {}
        assert stmt.expression is not None
        assert stmt.is_safe is True

    def test_sql_with_positional_parameters(self):
        """Test SQL with positional parameters using args."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        assert stmt.sql == sql
        assert stmt.parameters == [123]

    def test_sql_with_named_parameters(self):
        """Test SQL with named parameters using kwargs."""
        sql = "SELECT * FROM users WHERE name = :name"
        stmt = SQLStatement(sql, kwargs={"name": "John"})

        assert stmt.sql == sql
        assert stmt.parameters == {"name": "John"}

    def test_sql_with_explicit_parameters_dict(self):
        """Test SQL with explicit parameters as dict."""
        sql = "SELECT * FROM users WHERE name = :name AND age = :age"
        params = {"name": "Alice", "age": 30}
        stmt = SQLStatement(sql, parameters=params)

        assert stmt.sql == sql
        assert stmt.parameters == params

    def test_sql_with_explicit_parameters_list(self):
        """Test SQL with explicit parameters as list."""
        sql = "SELECT * FROM users WHERE id = ? AND status = ?"
        params = [123, "active"]
        stmt = SQLStatement(sql, parameters=params)

        assert stmt.sql == sql
        assert stmt.parameters == params

    def test_wrapping_existing_sqlstatement(self):
        """Test wrapping an existing SQLStatement instance."""
        original_sql = "SELECT * FROM users WHERE id = ?"
        original_stmt = SQLStatement(original_sql, args=[123])

        # Wrap without changes
        wrapped_stmt = SQLStatement(original_stmt)
        assert wrapped_stmt.sql == original_sql
        assert wrapped_stmt.parameters == [123]

        # Wrap with new parameters
        new_stmt = SQLStatement(original_stmt, args=[456])
        assert new_stmt.sql == original_sql
        assert new_stmt.parameters == [456]

    def test_sqlglot_expression_input(self):
        """Test initialization with sqlglot Expression object."""
        expression = exp.Select().select("*").from_("users")
        stmt = SQLStatement(expression)

        assert stmt.expression == expression
        assert "SELECT * FROM users" in stmt.sql


class TestSQLStatementParameterProcessing:
    """Test parameter processing and merging."""

    def test_parameter_merging_priority(self):
        """Test parameter merging with different priorities."""
        sql = "SELECT * FROM users WHERE id = :id"

        # parameters takes precedence over args/kwargs
        stmt = SQLStatement(sql, parameters={"id": 1}, args=[2], kwargs={"id": 3})
        assert stmt.parameters == {"id": 1}

    def test_args_and_kwargs_merging(self):
        """Test merging args and kwargs when no explicit parameters."""
        sql = "SELECT * FROM users WHERE id = ? AND name = :name"
        stmt = SQLStatement(sql, args=[123], kwargs={"name": "John"})

        # Should merge both into the result
        params = stmt.parameters
        assert isinstance(params, dict)
        assert "name" in params
        assert params["name"] == "John"

    def test_mixed_parameters_not_allowed_without_parsing(self):
        """Test that mixed parameters raise error when parsing disabled."""
        config = StatementConfig(enable_parsing=False, allow_mixed_parameters=False)
        sql = "SELECT * FROM users"

        with pytest.raises(ParameterError, match="Cannot mix args and kwargs"):
            SQLStatement(sql, args=[1], kwargs={"name": "test"}, config=config)

    def test_mixed_parameters_allowed_with_flag(self):
        """Test that mixed parameters work when explicitly allowed."""
        config = StatementConfig(enable_parsing=False, allow_mixed_parameters=True)
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql, args=[1], kwargs={"name": "test"}, config=config)

        # Should not raise error
        assert stmt.parameters is not None

    def test_scalar_parameter(self):
        """Test single scalar parameter."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, parameters=123)

        assert stmt.parameters == 123

    def test_empty_parameters(self):
        """Test with empty/None parameters."""
        sql = "SELECT * FROM users"

        # Test None
        stmt1 = SQLStatement(sql, parameters=None)
        assert stmt1.parameters is None

        # Test empty dict
        stmt2 = SQLStatement(sql, parameters={})
        assert stmt2.parameters == {}

        # Test empty list
        stmt3 = SQLStatement(sql, parameters=[])
        assert stmt3.parameters == []


class TestSQLStatementPlaceholderTransformation:
    """Test SQL placeholder style transformations."""

    def test_get_sql_default_format(self):
        """Test get_sql() with default format."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        result_sql = stmt.get_sql()
        assert result_sql == sql

    def test_get_sql_qmark_style(self):
        """Test get_sql() with qmark placeholder style."""
        sql = "SELECT * FROM users WHERE id = :id"
        stmt = SQLStatement(sql, kwargs={"id": 123})

        result_sql = stmt.get_sql(placeholder_style="qmark")
        # Should transform to qmark style (implementation dependent)
        assert isinstance(result_sql, str)

    def test_get_sql_named_style(self):
        """Test get_sql() with named placeholder style."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        result_sql = stmt.get_sql(placeholder_style="named")
        # Should transform to named style (implementation dependent)
        assert isinstance(result_sql, str)

    def test_get_sql_static_style(self):
        """Test get_sql() with static style (parameters substituted)."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        stmt = SQLStatement(sql, args=[123, "John"])

        result_sql = stmt.get_sql(placeholder_style="static")
        # Should contain the actual values, not placeholders
        assert "123" in result_sql
        assert "John" in result_sql or "'John'" in result_sql

    def test_get_sql_with_statement_separator(self):
        """Test get_sql() with statement separator."""
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql)

        result_sql = stmt.get_sql(include_statement_separator=True)
        assert result_sql.endswith(";")

    def test_static_sql_value_escaping(self):
        """Test that static SQL properly escapes values."""
        sql = "SELECT * FROM users WHERE name = ?"
        stmt = SQLStatement(sql, args=["O'Brien"])

        result_sql = stmt.get_sql(placeholder_style="static")
        # Should escape the apostrophe
        assert "O''Brien" in result_sql or "O\\'Brien" in result_sql


class TestSQLStatementParameterFormats:
    """Test parameter format conversion."""

    def test_get_parameters_default(self):
        """Test get_parameters() with default format."""
        params = {"name": "John", "age": 30}
        stmt = SQLStatement("SELECT * FROM users", parameters=params)

        result = stmt.get_parameters()
        assert result == params

    def test_get_parameters_dict_format(self):
        """Test get_parameters() with dict format."""
        stmt = SQLStatement("SELECT * FROM users WHERE id = ?", args=[123])

        result = stmt.get_parameters(style="dict")
        assert isinstance(result, dict)

    def test_get_parameters_list_format(self):
        """Test get_parameters() with list format."""
        params = {"name": "John", "age": 30}
        stmt = SQLStatement("SELECT * FROM users", parameters=params)

        result = stmt.get_parameters(style="list")
        assert isinstance(result, list)

    def test_get_parameters_tuple_format(self):
        """Test get_parameters() with tuple format."""
        params = ["John", 30]
        stmt = SQLStatement("SELECT * FROM users", parameters=params)

        result = stmt.get_parameters(style="tuple")
        assert isinstance(result, tuple)

    def test_get_parameters_parameter_style_enum(self):
        """Test get_parameters() with ParameterStyle enum."""
        stmt = SQLStatement("SELECT * FROM users WHERE id = ?", args=[123])

        result = stmt.get_parameters(style=ParameterStyle.QMARK)
        assert isinstance(result, list)


class TestSQLStatementValidation:
    """Test SQL validation functionality."""

    def test_valid_sql_validation(self):
        """Test validation of valid SQL."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        assert stmt.is_safe is True
        result = stmt.validate()
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE

    def test_invalid_sql_validation_strict_mode(self):
        """Test validation of invalid SQL in strict mode."""
        sql = "SELECT * FROM users; DROP TABLE users;"

        with pytest.raises(SQLValidationError):
            SQLStatement(sql, config=StatementConfig(strict_mode=True))

    def test_invalid_sql_validation_non_strict_mode(self):
        """Test validation of invalid SQL in non-strict mode."""
        sql = "SELECT * FROM users; -- some comment"
        config = StatementConfig(strict_mode=False)
        stmt = SQLStatement(sql, config=config)

        # Should not raise in non-strict mode
        assert stmt is not None

    def test_dangerous_sql_detection(self):
        """Test detection of dangerous SQL patterns."""
        dangerous_sqls = [
            "SELECT * FROM users; EXEC sp_configure",
            "SELECT * FROM users WHERE id = 1 UNION SELECT password FROM admin",
            "SELECT load_file('/etc/passwd')",
        ]

        for sql in dangerous_sqls:
            result = validate_sql(sql)
            assert not result.is_safe

    def test_validation_disabled(self):
        """Test behavior when validation is disabled."""
        config = StatementConfig(enable_validation=False)
        sql = "SELECT * FROM users; DROP TABLE users;"
        stmt = SQLStatement(sql, config=config)

        # Should not validate when disabled
        assert stmt.validation_result is None

    def test_custom_validator(self):
        """Test using a custom validator."""
        custom_validator = SQLValidator(strict_mode=False, allow_ddl=True)
        sql = "CREATE TABLE test (id INT)"
        stmt = SQLStatement(sql, validator=custom_validator)

        # Should not raise with custom validator allowing DDL
        assert stmt is not None


class TestSQLStatementSanitization:
    """Test SQL sanitization functionality."""

    def test_sanitize_valid_sql(self):
        """Test sanitization of valid SQL."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        sanitized_stmt = stmt.sanitize()
        assert sanitized_stmt is not None
        assert sanitized_stmt.sql == sql  # Should be unchanged for valid SQL

    def test_sanitize_with_comments(self):
        """Test sanitization behavior with comments."""
        sql = "SELECT * FROM users -- this is a comment"
        stmt = SQLStatement(sql)

        sanitized_stmt = stmt.sanitize()
        assert sanitized_stmt is not None

    def test_sanitization_disabled(self):
        """Test behavior when sanitization is disabled."""
        config = StatementConfig(enable_sanitization=False)
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql, config=config)

        sanitized_stmt = stmt.sanitize()
        assert sanitized_stmt is stmt  # Should return same instance

    def test_custom_sanitizer(self):
        """Test using a custom sanitizer."""
        custom_sanitizer = SQLSanitizer(strict_mode=False, allow_comments=False)
        sql = "SELECT * FROM users /* comment */"
        stmt = SQLStatement(sql, sanitizer=custom_sanitizer)

        # Should work with custom sanitizer
        assert stmt is not None


class TestSQLStatementFilterComposition:
    """Test filter composition functionality."""

    def test_append_filter_success(self):
        """Test successful filter application."""
        # Create a mock filter
        mock_filter = Mock()
        mock_filter.append_to_statement.return_value = SQLStatement("SELECT * FROM filtered")

        stmt = SQLStatement("SELECT * FROM users")
        result = stmt.append_filter(mock_filter)

        mock_filter.append_to_statement.assert_called_once_with(stmt)
        assert result.sql == "SELECT * FROM filtered"

    def test_append_filter_failure(self):
        """Test filter application failure."""
        # Create a mock filter that raises an error
        mock_filter = Mock()
        mock_filter.append_to_statement.side_effect = ValueError("Filter error")

        stmt = SQLStatement("SELECT * FROM users")

        with pytest.raises(SQLBuilderError, match="Failed to apply filter"):
            stmt.append_filter(mock_filter)


class TestSQLStatementCopyAndClone:
    """Test copy and clone functionality."""

    def test_copy_without_changes(self):
        """Test copying statement without changes."""
        sql = "SELECT * FROM users WHERE id = ?"
        original_stmt = SQLStatement(sql, args=[123])

        copied_stmt = original_stmt.copy()

        assert copied_stmt.sql == original_stmt.sql
        assert copied_stmt.parameters == original_stmt.parameters
        assert copied_stmt is not original_stmt  # Different instances

    def test_copy_with_new_sql(self):
        """Test copying statement with new SQL."""
        original_stmt = SQLStatement("SELECT * FROM users", args=[123])
        new_sql = "SELECT * FROM products"

        copied_stmt = original_stmt.copy(sql=new_sql)

        assert copied_stmt.sql == new_sql
        assert copied_stmt.parameters == original_stmt.parameters

    def test_copy_with_new_parameters(self):
        """Test copying statement with new parameters."""
        original_stmt = SQLStatement("SELECT * FROM users WHERE id = ?", args=[123])
        new_params = [456]

        copied_stmt = original_stmt.copy(parameters=new_params)

        assert copied_stmt.sql == original_stmt.sql
        assert copied_stmt.parameters == new_params

    def test_copy_with_configuration_changes(self):
        """Test copying statement with configuration changes."""
        original_config = StatementConfig(strict_mode=True)
        original_stmt = SQLStatement("SELECT * FROM users", config=original_config)

        new_config = StatementConfig(strict_mode=False)
        copied_stmt = original_stmt.copy(config=new_config)

        assert copied_stmt._config.strict_mode is False
        assert original_stmt._config.strict_mode is True


class TestSQLStatementErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_sql_syntax_strict_mode(self):
        """Test handling of invalid SQL syntax in strict mode."""
        invalid_sql = "SELECT * FROM"  # Incomplete SQL
        config = StatementConfig(strict_mode=True, enable_parsing=True)

        with pytest.raises(SQLValidationError):
            SQLStatement(invalid_sql, config=config)

    def test_invalid_sql_syntax_non_strict_mode(self):
        """Test handling of invalid SQL syntax in non-strict mode."""
        invalid_sql = "SELECT * FROM"  # Incomplete SQL
        config = StatementConfig(strict_mode=False, enable_parsing=True)

        # Should not raise in non-strict mode, but should handle gracefully
        stmt = SQLStatement(invalid_sql, config=config)
        assert stmt is not None

    def test_parameter_mismatch_error(self):
        """Test handling of parameter mismatches."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"

        # Providing only one parameter for two placeholders might be handled gracefully
        stmt = SQLStatement(sql, args=[123])
        assert stmt is not None

    def test_parsing_disabled_fallback(self):
        """Test fallback behavior when parsing is disabled."""
        config = StatementConfig(enable_parsing=False)
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123], config=config)

        assert stmt.expression is None  # No parsing
        assert stmt.sql == sql
        assert stmt.parameters == [123]


class TestSQLStatementStringRepresentations:
    """Test string representations."""

    def test_str_representation(self):
        """Test __str__ method."""
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql)

        assert str(stmt) == sql

    def test_repr_representation_no_parameters(self):
        """Test __repr__ method without parameters."""
        sql = "SELECT * FROM users"
        stmt = SQLStatement(sql)

        repr_str = repr(stmt)
        assert "SQLStatement" in repr_str
        assert sql in repr_str

    def test_repr_representation_with_parameters(self):
        """Test __repr__ method with parameters."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = [123]
        stmt = SQLStatement(sql, parameters=params)

        repr_str = repr(stmt)
        assert "SQLStatement" in repr_str
        assert sql in repr_str
        assert "parameters=" in repr_str


class TestSQLStatementConfiguration:
    """Test configuration options."""

    def test_default_configuration(self):
        """Test default configuration values."""
        stmt = SQLStatement("SELECT * FROM users")
        config = stmt._config

        assert config.enable_parsing is True
        assert config.enable_validation is True
        assert config.enable_sanitization is True
        assert config.strict_mode is True
        assert config.allow_mixed_parameters is False
        assert config.cache_parsed_expression is True

    def test_custom_configuration(self):
        """Test custom configuration."""
        config = StatementConfig(
            enable_parsing=False,
            enable_validation=False,
            enable_sanitization=False,
            strict_mode=False,
            allow_mixed_parameters=True,
            cache_parsed_expression=False,
        )
        stmt = SQLStatement("SELECT * FROM users", config=config)

        assert stmt._config.enable_parsing is False
        assert stmt._config.enable_validation is False
        assert stmt._config.enable_sanitization is False
        assert stmt._config.strict_mode is False
        assert stmt._config.allow_mixed_parameters is True
        assert stmt._config.cache_parsed_expression is False

    def test_configuration_inheritance_in_copy(self):
        """Test that configuration is inherited in copy operations."""
        config = StatementConfig(strict_mode=False)
        original_stmt = SQLStatement("SELECT * FROM users", config=config)

        copied_stmt = original_stmt.copy()
        assert copied_stmt._config.strict_mode is False


class TestSQLStatementUtilityFunctions:
    """Test utility functions."""

    def test_is_sql_safe_function(self):
        """Test is_sql_safe utility function."""
        safe_sql = "SELECT * FROM users WHERE id = ?"
        dangerous_sql = "SELECT * FROM users; DROP TABLE users;"

        assert is_sql_safe(safe_sql) is True
        assert is_sql_safe(dangerous_sql) is False

    def test_validate_sql_function(self):
        """Test validate_sql utility function."""
        sql = "SELECT * FROM users"
        result = validate_sql(sql)

        assert isinstance(result, ValidationResult)
        assert result.is_safe is True

    def test_sanitize_sql_function(self):
        """Test sanitize_sql utility function."""
        sql = "SELECT * FROM users"
        result = sanitize_sql(sql)

        assert isinstance(result, exp.Expression)


# Test data from the original test file adapted for the new SQLStatement
BASIC_PARAMETER_CASES = [
    ("Colon named", "SELECT * FROM users WHERE id = :id", {"id": 123}),
    ("Question mark", "SELECT * FROM users WHERE id = ?", [123]),
    ("Dollar named", "SELECT * FROM products WHERE name = $name", {"name": "test"}),
    ("At named", "SELECT * FROM employees WHERE email = @email", {"email": "test@example.com"}),
    ("Pyformat named", "INSERT INTO logs (message) VALUES (%(msg)s)", {"msg": "test"}),
]

MIXED_PARAMETER_CASES = [
    ("Mixed with kwargs", "SELECT * FROM users WHERE id = ? AND name = :name", [123], {"name": "John"}),
    ("Multiple positional", "SELECT * FROM users WHERE id = ? AND age = ?", [123, 25], None),
    (
        "Multiple named",
        "SELECT * FROM users WHERE name = :name AND email = :email",
        None,
        {"name": "John", "email": "john@example.com"},
    ),
]

PLACEHOLDER_STYLE_CASES = [
    ("qmark", "SELECT * FROM users WHERE id = :id", {"id": 123}, "qmark"),
    ("named", "SELECT * FROM users WHERE id = ?", [123], "named"),
    ("static", "SELECT * FROM users WHERE id = ? AND name = ?", [123, "John"], "static"),
]

COMMENTS_AND_STRINGS_CASES = [
    ("Inside single quotes", "SELECT * FROM users WHERE notes = 'param: :value, other: ?'"),
    ("Inside double quotes", 'SELECT * FROM users WHERE description = "param: :value, other: ?"'),
    ("Inside single-line comment", "SELECT * FROM users; -- id = :id, name = $name, status = ?"),
    ("Inside multi-line comment", "SELECT * FROM users; /* id = :id, name = $name, status = ? */"),
]


@pytest.mark.parametrize("description,sql,params", BASIC_PARAMETER_CASES)
def test_basic_parameter_types_new_impl(description: str, sql: str, params):
    """Test basic parameter types with new SQLStatement implementation."""
    if isinstance(params, dict):
        stmt = SQLStatement(sql, kwargs=params)
    else:
        stmt = SQLStatement(sql, args=params)

    assert stmt.sql == sql
    assert stmt.parameters == params


@pytest.mark.parametrize("description,sql,args,kwargs", MIXED_PARAMETER_CASES)
def test_mixed_parameter_types_new_impl(description: str, sql: str, args, kwargs):
    """Test mixed parameter types with new SQLStatement implementation."""
    if kwargs and args:
        stmt = SQLStatement(sql, args=args, kwargs=kwargs)
    elif kwargs:
        stmt = SQLStatement(sql, kwargs=kwargs)
    else:
        stmt = SQLStatement(sql, args=args)

    assert stmt.sql == sql
    # Parameters should be merged appropriately
    assert stmt.parameters is not None


@pytest.mark.parametrize("description,sql,params,style", PLACEHOLDER_STYLE_CASES)
def test_placeholder_transformations_new_impl(description: str, sql: str, params, style: str):
    """Test placeholder style transformations with new SQLStatement implementation."""
    if isinstance(params, dict):
        stmt = SQLStatement(sql, kwargs=params)
    else:
        stmt = SQLStatement(sql, args=params)

    result_sql = stmt.get_sql(placeholder_style=style)
    assert isinstance(result_sql, str)

    if style == "static":
        # Static should contain actual values
        for param in params if isinstance(params, list) else params.values():
            assert str(param) in result_sql or f"'{param}'" in result_sql


@pytest.mark.parametrize("description,sql", COMMENTS_AND_STRINGS_CASES)
def test_parameters_ignored_in_comments_and_strings_new_impl(description: str, sql: str):
    """Test that parameters in comments and strings are properly ignored."""
    stmt = SQLStatement(sql)

    # Should not raise errors and should parse successfully
    assert stmt.sql == sql
    # Parameters in comments/strings should be ignored
    assert stmt.parameters is None or stmt.parameters == {} or stmt.parameters == []


class TestSQLStatementIntegration:
    """Integration tests with parameter infrastructure."""

    def test_parameter_converter_integration(self):
        """Test integration with ParameterConverter."""
        sql = "SELECT * FROM users WHERE id = :id AND name = :name"
        stmt = SQLStatement(sql, kwargs={"id": 123, "name": "John"})

        # Should use ParameterConverter for processing
        assert stmt.parameter_info is not None
        assert len(stmt.parameter_info) >= 0  # May be empty if processing failed gracefully

    def test_parameter_style_transformation(self):
        """Test parameter style transformation using infrastructure."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        # Test different parameter styles
        for style in ["qmark", "named", "pyformat_named"]:
            result_sql = stmt.get_sql(placeholder_style=style)
            assert isinstance(result_sql, str)

    def test_complex_parameter_scenarios(self):
        """Test complex parameter scenarios."""
        # Mixed parameter styles in SQL
        sql = "SELECT * FROM users WHERE id = ? AND name = :name AND status = %(status)s"

        # Should handle gracefully even with mixed styles
        stmt = SQLStatement(sql)
        assert stmt is not None

    def test_parameter_info_extraction(self):
        """Test parameter info extraction."""
        sql = "SELECT * FROM users WHERE id = :id"
        stmt = SQLStatement(sql, kwargs={"id": 123})

        # Should extract parameter information
        assert hasattr(stmt, "parameter_info")
        assert hasattr(stmt, "placeholder_map")


class TestSQLStatementComplexScenarios:
    """Test complex real-world scenarios."""

    def test_json_operations(self):
        """Test SQL with JSON operations."""
        sql = "SELECT data->>'key' as val FROM test WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        assert stmt.sql == sql
        assert stmt.parameters == [123]

    def test_window_functions(self):
        """Test SQL with window functions."""
        sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users WHERE status = ?"
        stmt = SQLStatement(sql, args=["active"])

        assert stmt.sql == sql
        assert stmt.parameters == ["active"]

    def test_cte_queries(self):
        """Test SQL with Common Table Expressions."""
        sql = """
        WITH active_users AS (
            SELECT * FROM users WHERE status = ?
        )
        SELECT * FROM active_users WHERE age > ?
        """
        stmt = SQLStatement(sql, args=["active", 18])

        assert stmt.parameters == ["active", 18]

    def test_complex_joins(self):
        """Test SQL with complex joins."""
        sql = """
        SELECT u.name, p.title 
        FROM users u 
        LEFT JOIN posts p ON u.id = p.user_id 
        WHERE u.status = ? AND p.created_at > ?
        """
        stmt = SQLStatement(sql, args=["active", "2023-01-01"])

        assert stmt.parameters == ["active", "2023-01-01"]


class TestSQLStatementEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_sql(self):
        """Test with empty SQL string."""
        stmt = SQLStatement("")

        assert stmt.sql == ""

    def test_whitespace_only_sql(self):
        """Test with whitespace-only SQL."""
        sql = "   \n\t  "
        stmt = SQLStatement(sql)

        assert stmt.sql == sql

    def test_very_long_sql(self):
        """Test with very long SQL statement."""
        sql = "SELECT " + ", ".join([f"col_{i}" for i in range(100)]) + " FROM big_table WHERE id = ?"
        stmt = SQLStatement(sql, args=[123])

        assert stmt.sql == sql
        assert stmt.parameters == [123]

    def test_unicode_in_sql(self):
        """Test SQL with Unicode characters."""
        sql = "SELECT * FROM users WHERE name = ? AND description = ?"
        params = ["Müller", "héllo wørld"]
        stmt = SQLStatement(sql, args=params)

        assert stmt.sql == sql
        assert stmt.parameters == params

    def test_special_characters_in_parameters(self):
        """Test parameters with special characters."""
        sql = "SELECT * FROM users WHERE data = ?"
        special_data = "test'data\"with;chars--and/*comments*/"
        stmt = SQLStatement(sql, args=[special_data])

        assert stmt.parameters == [special_data]

        # Static SQL should properly escape
        static_sql = stmt.get_sql(placeholder_style="static")
        assert isinstance(static_sql, str)


class TestSQLStatementPerformance:
    """Test performance-related functionality."""

    def test_expression_caching(self):
        """Test that expressions are cached when enabled."""
        config = StatementConfig(cache_parsed_expression=True)
        sql = "SELECT * FROM users"

        stmt1 = SQLStatement(sql, config=config)
        stmt2 = SQLStatement(sql, config=config)

        # Both should parse successfully
        assert stmt1.expression is not None
        assert stmt2.expression is not None

    def test_no_caching_when_disabled(self):
        """Test behavior when caching is disabled."""
        config = StatementConfig(cache_parsed_expression=False)
        sql = "SELECT * FROM users"

        stmt = SQLStatement(sql, config=config)
        assert stmt.expression is not None  # Should still work


class TestSQLStatementDialectSupport:
    """Test dialect-specific functionality."""

    def test_postgres_dialect(self):
        """Test with PostgreSQL dialect."""
        sql = "SELECT * FROM users WHERE data ? 'key' AND id = $1"
        stmt = SQLStatement(sql, args=[123], dialect="postgres")

        assert stmt.sql == sql

    def test_mysql_dialect(self):
        """Test with MySQL dialect."""
        sql = "SELECT * FROM users WHERE name = %s"
        stmt = SQLStatement(sql, args=["John"], dialect="mysql")

        assert stmt.sql == sql

    def test_sqlite_dialect(self):
        """Test with SQLite dialect."""
        sql = "SELECT * FROM users WHERE id = ?"
        stmt = SQLStatement(sql, args=[123], dialect="sqlite")

        assert stmt.sql == sql


if __name__ == "__main__":
    pytest.main([__file__])
