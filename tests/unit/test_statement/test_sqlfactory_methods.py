"""Unit tests for SQLFactory method implementations.

This module provides comprehensive test coverage for SQLFactory methods
that will be refactored to use centralized parameter-to-literal conversion.

Covers:
- decode() method with various parameter types
- nvl() method with various parameter types
- Case.when() and Case.else_() methods with various parameter types
- any() method with various parameter types
"""

from typing import Any

import pytest
from sqlglot import exp

from sqlspec._sql import SQLFactory


@pytest.fixture
def sql_factory() -> SQLFactory:
    """Create SQLFactory instance for testing."""
    return SQLFactory()


class TestSQLFactoryDecode:
    """Test decode() method parameter handling."""

    def test_decode_with_string_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with string value parameter."""
        result = sql_factory.decode("column", "search_value", "replacement")

        assert isinstance(result, exp.Case)
        # Should have proper string literals in WHEN clause
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition (column = search_value)
        condition = when_clause.args["this"]  # The condition part
        assert isinstance(condition, exp.EQ)
        assert condition.expression.is_string  # search_value
        # Check the result value
        result_value = when_clause.args["then"]
        assert result_value.is_string  # replacement

    def test_decode_with_numeric_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with numeric value parameters."""
        result = sql_factory.decode("column", 42, 100)

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        assert condition.expression.is_number  # 42
        # Check the result value
        result_value = when_clause.args["then"]
        assert result_value.is_number  # 100

    def test_decode_with_float_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with float value parameters."""
        result = sql_factory.decode("column", 3.14, 2.71)

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        assert condition.expression.is_number  # 3.14
        # Check the result value
        result_value = when_clause.args["then"]
        assert result_value.is_number  # 2.71

    def test_decode_with_expression_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with SQLGlot expression parameters."""
        search_expr = exp.column("other_column")
        replace_expr = exp.Literal.string("custom")

        result = sql_factory.decode("column", search_expr, replace_expr)

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        assert isinstance(condition.expression, exp.Column)  # other_column
        # Check the result value
        result_value = when_clause.args["then"]
        assert isinstance(result_value, exp.Literal)  # custom

    def test_decode_with_none_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with None value parameters."""
        result = sql_factory.decode("column", None, "null_replacement")

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        # None values are now properly converted to exp.Null
        assert isinstance(condition.expression, exp.Null)
        # Check the result value
        result_value = when_clause.args["then"]
        assert result_value.is_string  # "null_replacement"

    def test_decode_with_bool_value(self, sql_factory: SQLFactory) -> None:
        """Test decode() with boolean value parameters."""
        result = sql_factory.decode("column", True, False)

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        # Boolean values are now properly converted to exp.Boolean
        assert isinstance(condition.expression, exp.Boolean)
        # Check the result value
        result_value = when_clause.args["then"]
        assert isinstance(result_value, exp.Boolean)

    def test_decode_with_mixed_types(self, sql_factory: SQLFactory) -> None:
        """Test decode() with mixed parameter types."""
        result = sql_factory.decode("column", "text", 42)

        assert isinstance(result, exp.Case)
        ifs = result.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the search value in the condition
        condition = when_clause.args["this"]
        assert isinstance(condition, exp.EQ)
        assert condition.expression.is_string  # "text"
        # Check the result value
        result_value = when_clause.args["then"]
        assert result_value.is_number  # 42


class TestSQLFactoryNvl:
    """Test nvl() method parameter handling."""

    def test_nvl_with_string_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with string value parameter."""
        result = sql_factory.nvl("column", "default_value")

        assert isinstance(result, exp.Coalesce)
        # Should have proper string literal for default
        expressions = result.expressions
        assert len(expressions) == 2
        assert expressions[1].is_string

    def test_nvl_with_numeric_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with numeric value parameter."""
        result = sql_factory.nvl("column", 42)

        assert isinstance(result, exp.Coalesce)
        expressions = result.expressions
        assert len(expressions) == 2
        assert expressions[1].is_number

    def test_nvl_with_float_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with float value parameter."""
        result = sql_factory.nvl("column", 3.14)

        assert isinstance(result, exp.Coalesce)
        expressions = result.expressions
        assert len(expressions) == 2
        assert expressions[1].is_number

    def test_nvl_with_expression_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with SQLGlot expression parameter."""
        default_expr = exp.column("backup_column")

        result = sql_factory.nvl("column", default_expr)

        assert isinstance(result, exp.Coalesce)
        expressions = result.expressions
        assert len(expressions) == 2
        assert isinstance(expressions[1], exp.Column)

    def test_nvl_with_none_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with None value parameter."""
        result = sql_factory.nvl("column", None)

        assert isinstance(result, exp.Coalesce)
        expressions = result.expressions
        assert len(expressions) == 2
        # None should be converted to NULL literal
        assert isinstance(expressions[1], exp.Null)

    def test_nvl_with_bool_value(self, sql_factory: SQLFactory) -> None:
        """Test nvl() with boolean value parameter."""
        result = sql_factory.nvl("column", True)

        assert isinstance(result, exp.Coalesce)
        expressions = result.expressions
        assert len(expressions) == 2
        assert isinstance(expressions[1], exp.Boolean)


class TestSQLFactoryCase:
    """Test Case.when() and Case.else_() method parameter handling."""

    def test_case_when_with_string_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.when() with string value parameter."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", "string_result")

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        # Should have proper WHEN clause with string literal
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        # Check the result value
        result_value = when_clause.args["true"]
        assert result_value.is_string

    def test_case_when_with_numeric_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.when() with numeric value parameter."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", 42)

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        result_value = when_clause.args["true"]
        assert result_value.is_number

    def test_case_when_with_float_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.when() with float value parameter."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", 3.14)

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        result_value = when_clause.args["true"]
        assert result_value.is_number

    def test_case_when_with_expression_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.when() with SQLGlot expression parameter."""
        value_expr = exp.column("other_column")
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", value_expr)

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) == 1
        when_clause = ifs[0]
        result_value = when_clause.args["true"]
        assert isinstance(result_value, exp.Column)

    def test_case_else_with_string_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.else_() with string value parameter."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", "first")
        case_builder = case_builder.else_("default_string")

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        # Should have proper ELSE clause with string literal
        default = case_expr.args.get("default")
        assert default is not None
        assert default.is_string

    def test_case_else_with_numeric_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.else_() with numeric value parameter."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", "first")
        case_builder = case_builder.else_(42)

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        default = case_expr.args.get("default")
        assert default is not None
        assert default.is_number

    def test_case_else_with_expression_value(self, sql_factory: SQLFactory) -> None:
        """Test Case.else_() with SQLGlot expression parameter."""
        default_expr = exp.column("backup_column")
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", "first")
        case_builder = case_builder.else_(default_expr)

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        default = case_expr.args.get("default")
        assert default is not None
        assert isinstance(default, exp.Column)

    def test_case_multiple_when_mixed_types(self, sql_factory: SQLFactory) -> None:
        """Test Case with multiple when() calls using mixed parameter types."""
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", "string_result")
        case_builder = case_builder.when("condition = 2", 42)
        case_builder = case_builder.when("condition = 3", 3.14)
        case_builder = case_builder.else_("default")

        case_expr = case_builder.end()
        assert isinstance(case_expr, exp.Case)
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) == 3

        # Verify each when clause has correct literal type
        assert ifs[0].args["true"].is_string  # "string_result"
        assert ifs[1].args["true"].is_number  # 42
        assert ifs[2].args["true"].is_number  # 3.14

        # Verify else clause
        default = case_expr.args.get("default")
        assert default is not None
        assert default.is_string


class TestSQLFactoryAny:
    """Test any() method parameter handling."""

    def test_any_with_string_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with string value parameters."""
        result = sql_factory.any(["value1", "value2", "value3"])

        assert isinstance(result, exp.Any)
        # Should have proper string literals in array
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        for expr in expressions:
            assert expr.is_string

    def test_any_with_numeric_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with numeric value parameters."""
        result = sql_factory.any([1, 2, 3])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        for expr in expressions:
            assert expr.is_number

    def test_any_with_float_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with float value parameters."""
        result = sql_factory.any([1.1, 2.2, 3.3])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        for expr in expressions:
            assert expr.is_number

    def test_any_with_mixed_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with mixed value types."""
        result = sql_factory.any(["text", 42, 3.14])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        assert expressions[0].is_string  # "text"
        assert expressions[1].is_number  # 42
        assert expressions[2].is_number  # 3.14

    def test_any_with_expression_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with SQLGlot expression parameters."""
        expr_list = [exp.column("other_column"), exp.Literal.string("literal"), exp.Literal.number(42)]
        result = sql_factory.any(expr_list)

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        assert isinstance(expressions[0], exp.Column)
        assert isinstance(expressions[1], exp.Literal)
        assert isinstance(expressions[2], exp.Literal)

    def test_any_with_none_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with None values in list."""
        result = sql_factory.any(["value", None, 42])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 3
        assert expressions[0].is_string  # "value"
        assert isinstance(expressions[1], exp.Null)  # None becomes NULL
        assert expressions[2].is_number  # 42

    def test_any_with_bool_values(self, sql_factory: SQLFactory) -> None:
        """Test any() with boolean values."""
        result = sql_factory.any([True, False])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 2
        assert isinstance(expressions[0], exp.Boolean)  # True becomes Boolean
        assert isinstance(expressions[1], exp.Boolean)  # False becomes Boolean

    def test_any_empty_list(self, sql_factory: SQLFactory) -> None:
        """Test any() with empty list."""
        result = sql_factory.any([])

        assert isinstance(result, exp.Any)
        array_expr = result.args["this"]
        assert isinstance(array_expr, exp.Array)
        expressions = array_expr.expressions
        assert len(expressions) == 0


class TestParameterTypeConsistency:
    """Test that all methods handle parameters consistently."""

    @pytest.mark.parametrize(
        "value,expected_type",
        [
            ("string", "string"),
            (42, "number"),
            (3.14, "number"),
            (True, "boolean"),  # Booleans become Boolean objects
            (False, "boolean"),
            (None, "null"),  # None becomes NULL
        ],
    )
    def test_consistent_parameter_handling(self, sql_factory: SQLFactory, value: Any, expected_type: str) -> None:
        """Test that all methods handle the same parameter types consistently."""
        # Test decode method
        decode_result = sql_factory.decode("col", value, "replacement")
        ifs = decode_result.args.get("ifs", [])
        when_clause = ifs[0]
        condition = when_clause.args["this"]
        search_literal = condition.expression

        if expected_type == "string":
            assert search_literal.is_string
        elif expected_type == "number":
            assert search_literal.is_number
        elif expected_type == "boolean":
            assert isinstance(search_literal, exp.Boolean)
        elif expected_type == "null":
            assert isinstance(search_literal, exp.Null)

        # Test nvl method
        nvl_result = sql_factory.nvl("col", value)
        nvl_literal = nvl_result.expressions[1]

        if expected_type == "string":
            assert nvl_literal.is_string
        elif expected_type == "number":
            assert nvl_literal.is_number
        elif expected_type == "boolean":
            assert isinstance(nvl_literal, exp.Boolean)
        elif expected_type == "null":
            assert isinstance(nvl_literal, exp.Null)

        # Test case when method
        case_builder = sql_factory.case()
        case_builder = case_builder.when("condition = 1", value)
        case_expr = case_builder.end()
        ifs = case_expr.args.get("ifs", [])
        when_literal = ifs[0].args["true"]

        if expected_type == "string":
            assert when_literal.is_string
        elif expected_type == "number":
            assert when_literal.is_number
        elif expected_type == "boolean":
            assert isinstance(when_literal, exp.Boolean)
        elif expected_type == "null":
            assert isinstance(when_literal, exp.Null)

        # Test case else method
        case_builder2 = sql_factory.case()
        case_builder2 = case_builder2.when("condition = 1", "first")
        case_builder2 = case_builder2.else_(value)
        case_expr2 = case_builder2.end()
        else_literal = case_expr2.args.get("default")

        if expected_type == "string":
            assert else_literal.is_string
        elif expected_type == "number":
            assert else_literal.is_number
        elif expected_type == "boolean":
            assert isinstance(else_literal, exp.Boolean)
        elif expected_type == "null":
            assert isinstance(else_literal, exp.Null)

        # Test any method (single value in list)
        any_result = sql_factory.any([value])
        array_expr = any_result.args["this"]
        any_literal = array_expr.expressions[0]

        if expected_type == "string":
            assert any_literal.is_string
        elif expected_type == "number":
            assert any_literal.is_number
        elif expected_type == "boolean":
            assert isinstance(any_literal, exp.Boolean)
        elif expected_type == "null":
            assert isinstance(any_literal, exp.Null)
