"""Tests for conditional parameter normalization for SQLGlot compatibility."""

from sqlspec.statement.parameters import SQLGLOT_INCOMPATIBLE_STYLES, ParameterConverter, ParameterStyle
from sqlspec.statement.sql import SQL


class TestParameterNormalization:
    """Test conditional normalization of SQLGlot-incompatible parameter styles."""

    def test_pyformat_normalization(self) -> None:
        """Test that pyformat parameters are normalized for SQLGlot."""
        sql = "SELECT * FROM users WHERE name = %s AND age > %s"
        params = ["john", 25]

        converter = ParameterConverter()
        result = converter.convert_parameters(sql, params)

        # Check that normalization occurred
        transformed_sql, _param_info, _merged_params, extra_info = result
        assert extra_info["was_normalized"] is True
        assert ":__param_0" in transformed_sql
        assert ":__param_1" in transformed_sql
        assert "%s" not in transformed_sql

    def test_pyformat_named_normalization(self) -> None:
        """Test that named pyformat parameters are normalized."""
        sql = "SELECT * FROM users WHERE name = %(name)s AND age > %(age)s"
        params = {"name": "john", "age": 25}

        converter = ParameterConverter()
        result = converter.convert_parameters(sql, params)

        transformed_sql, _param_info, _merged_params, extra_info = result
        assert extra_info["was_normalized"] is True
        assert ":__param_0" in transformed_sql
        assert ":__param_1" in transformed_sql
        assert "%(name)s" not in transformed_sql
        assert "%(age)s" not in transformed_sql

    def test_no_normalization_for_compatible_styles(self) -> None:
        """Test that SQLGlot-compatible styles are not normalized."""
        test_cases = [
            ("SELECT * FROM users WHERE id = ?", [42], ParameterStyle.QMARK),
            ("SELECT * FROM users WHERE id = :id", {"id": 42}, ParameterStyle.NAMED_COLON),
            ("SELECT * FROM users WHERE id = $1", [42], ParameterStyle.NUMERIC),
            ("SELECT * FROM users WHERE id = @id", {"id": 42}, ParameterStyle.NAMED_AT),
        ]

        converter = ParameterConverter()
        for sql, params, expected_style in test_cases:
            result = converter.convert_parameters(sql, params)
            transformed_sql, param_info, _merged_params, extra_info = result

            # Should not be normalized
            assert extra_info["was_normalized"] is False
            assert transformed_sql == sql
            assert param_info[0].style == expected_style

    def test_denormalization(self) -> None:
        """Test denormalization back to original style."""
        sql = "SELECT * FROM users WHERE name = %s"
        stmt = SQL(sql, parameters=["john"])

        # Get SQL with original style
        result_sql = stmt.to_sql(placeholder_style=ParameterStyle.POSITIONAL_PYFORMAT)

        # Should have %s back
        assert "%s" in result_sql
        assert ":__param_" not in result_sql

    def test_mixed_incompatible_styles(self) -> None:
        """Test error handling for mixed SQLGlot-incompatible styles."""
        # This should still normalize since one style is incompatible
        sql = "SELECT * FROM users WHERE name = %s AND id = ?"
        params = ["john", 42]

        converter = ParameterConverter()
        result = converter.convert_parameters(sql, params)

        _transformed_sql, _param_info, _merged_params, extra_info = result
        assert extra_info["was_normalized"] is True

    def test_sqlglot_incompatible_styles_constant(self) -> None:
        """Test that SQLGLOT_INCOMPATIBLE_STYLES contains the correct styles."""
        assert ParameterStyle.POSITIONAL_PYFORMAT in SQLGLOT_INCOMPATIBLE_STYLES
        assert ParameterStyle.NAMED_PYFORMAT in SQLGLOT_INCOMPATIBLE_STYLES
        assert ParameterStyle.POSITIONAL_COLON in SQLGLOT_INCOMPATIBLE_STYLES

        # These should NOT be in the incompatible set
        assert ParameterStyle.QMARK not in SQLGLOT_INCOMPATIBLE_STYLES
        assert ParameterStyle.NAMED_COLON not in SQLGLOT_INCOMPATIBLE_STYLES
        assert ParameterStyle.NUMERIC not in SQLGLOT_INCOMPATIBLE_STYLES
        assert ParameterStyle.NAMED_AT not in SQLGLOT_INCOMPATIBLE_STYLES

    def test_normalization_preserves_parameter_order(self) -> None:
        """Test that normalization preserves parameter order."""
        sql = "INSERT INTO users (name, age, email) VALUES (%s, %s, %s)"
        params = ["john", 25, "john@example.com"]

        converter = ParameterConverter()
        result = converter.convert_parameters(sql, params)

        transformed_sql, _param_info, merged_params, _extra_info = result

        # Check order is preserved
        assert ":__param_0" in transformed_sql
        assert ":__param_1" in transformed_sql
        assert ":__param_2" in transformed_sql

        # Check values are in correct order
        assert merged_params == params

    def test_positional_colon_normalized(self) -> None:
        """Test that Oracle numeric style is normalized for SQLGlot."""
        sql = "INSERT INTO users (id, name) VALUES (:1, :2)"
        params = [42, "john"]

        converter = ParameterConverter()
        result = converter.convert_parameters(sql, params)

        transformed_sql, _param_info, _merged_params, extra_info = result
        # Oracle numeric is incompatible with SQLGlot, so it should be normalized
        assert extra_info["was_normalized"] is True
        assert ":__param_0" in transformed_sql
        assert ":__param_1" in transformed_sql
        assert ":1" not in transformed_sql
        assert ":2" not in transformed_sql
