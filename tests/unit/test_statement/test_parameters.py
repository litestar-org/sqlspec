"""Unit tests for sqlspec.statement.parameters module."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import patch

import pytest

from sqlspec.exceptions import ExtraParameterError, MissingParameterError, ParameterStyleMismatchError
from sqlspec.statement.parameters import ParameterConverter, ParameterInfo, ParameterStyle, ParameterValidator

if TYPE_CHECKING:
    from sqlspec.typing import SQLParameterType


# Test ParameterStyle enum
@pytest.mark.parametrize(
    "style,expected_value",
    [
        (ParameterStyle.NONE, "none"),
        (ParameterStyle.STATIC, "static"),
        (ParameterStyle.QMARK, "qmark"),
        (ParameterStyle.NUMERIC, "numeric"),
        (ParameterStyle.NAMED_COLON, "named_colon"),
        (ParameterStyle.POSITIONAL_COLON, "positional_colon"),
        (ParameterStyle.NAMED_AT, "named_at"),
        (ParameterStyle.NAMED_DOLLAR, "named_dollar"),
        (ParameterStyle.NAMED_PYFORMAT, "pyformat_named"),
        (ParameterStyle.POSITIONAL_PYFORMAT, "pyformat_positional"),
    ],
)
def test_parameter_style_values(style: ParameterStyle, expected_value: str) -> None:
    """Test ParameterStyle enum values."""
    assert style.value == expected_value
    assert str(style) == expected_value


# Test ParameterInfo dataclass
@pytest.mark.parametrize(
    "name,style,position,ordinal,placeholder_text",
    [
        ("user_id", ParameterStyle.NAMED_COLON, 25, 0, ":user_id"),
        (None, ParameterStyle.QMARK, 10, 1, "?"),
        ("param1", ParameterStyle.NAMED_PYFORMAT, 35, 0, "%(param1)s"),
        (None, ParameterStyle.POSITIONAL_PYFORMAT, 15, 2, "%s"),
        ("id", ParameterStyle.NAMED_AT, 20, 0, "@id"),
        ("value", ParameterStyle.NAMED_DOLLAR, 30, 0, "$value"),
        (None, ParameterStyle.NUMERIC, 5, 1, "$1"),
    ],
    ids=["named_colon", "qmark", "pyformat_named", "pyformat_positional", "named_at", "named_dollar", "numeric"],
)
def test_parameter_info_creation(
    name: Optional[str], style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
) -> None:
    """Test ParameterInfo creation with various parameter types."""
    param_info = ParameterInfo(
        name=name, style=style, position=position, ordinal=ordinal, placeholder_text=placeholder_text
    )

    assert param_info.name == name
    assert param_info.style == style
    assert param_info.position == position
    assert param_info.ordinal == ordinal
    assert param_info.placeholder_text == placeholder_text


def test_parameter_info_equality() -> None:
    """Test ParameterInfo equality comparison."""
    param1 = ParameterInfo("test", ParameterStyle.NAMED_COLON, 10, 0, ":test")
    param2 = ParameterInfo("test", ParameterStyle.NAMED_COLON, 10, 0, ":test")
    param3 = ParameterInfo("other", ParameterStyle.NAMED_COLON, 10, 0, ":other")

    assert param1 == param2
    assert param1 != param3


# Test ParameterValidator
@pytest.fixture
def validator() -> ParameterValidator:
    """Create a ParameterValidator instance."""
    return ParameterValidator()


@pytest.mark.parametrize(
    "sql,expected_count,expected_styles",
    [
        ("SELECT * FROM users", 0, []),
        ("SELECT * FROM users WHERE id = ?", 1, [ParameterStyle.QMARK]),
        ("SELECT * FROM users WHERE name = :name", 1, [ParameterStyle.NAMED_COLON]),
        ("SELECT * FROM users WHERE id = ? AND name = ?", 2, [ParameterStyle.QMARK, ParameterStyle.QMARK]),
        (
            "SELECT * FROM users WHERE name = :name AND email = :email",
            2,
            [ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON],
        ),
        ("SELECT * FROM users WHERE id = %(id)s", 1, [ParameterStyle.NAMED_PYFORMAT]),
        ("SELECT * FROM users WHERE name = %s", 1, [ParameterStyle.POSITIONAL_PYFORMAT]),
        ("SELECT * FROM users WHERE id = @id", 1, [ParameterStyle.NAMED_AT]),
        ("SELECT * FROM users WHERE id = $1", 1, [ParameterStyle.NUMERIC]),
        ("SELECT * FROM users WHERE name = $name", 1, [ParameterStyle.NAMED_DOLLAR]),
        ("SELECT * FROM users WHERE id = :1", 1, [ParameterStyle.POSITIONAL_COLON]),
    ],
    ids=[
        "no_params",
        "single_qmark",
        "single_named_colon",
        "multiple_qmark",
        "multiple_named_colon",
        "pyformat_named",
        "pyformat_positional",
        "named_at",
        "numeric",
        "named_dollar",
        "positional_colon",
    ],
)
def test_extract_parameters(
    validator: ParameterValidator, sql: str, expected_count: int, expected_styles: list[ParameterStyle]
) -> None:
    """Test parameter extraction from various SQL patterns."""
    params = validator.extract_parameters(sql)

    assert len(params) == expected_count
    for i, expected_style in enumerate(expected_styles):
        assert params[i].style == expected_style


@pytest.mark.parametrize(
    "sql,should_be_ignored",
    [
        ("SELECT 'test with ? inside'", True),
        ('SELECT "test with ? inside"', True),
        ("SELECT $tag$content with ? and :param$tag$", True),
        ("SELECT * FROM test -- comment with ? and :param", True),
        ("SELECT * FROM test /* comment with ? and :param */", True),
        ("SELECT * FROM json WHERE data ?? 'key'", True),  # PostgreSQL JSON operator
        ("SELECT * FROM json WHERE data ?| array['key']", True),  # PostgreSQL JSON operator
        ("SELECT * FROM json WHERE data ?& array['key']", True),  # PostgreSQL JSON operator
        ("SELECT * FROM users WHERE id::int = 5", False),  # PostgreSQL cast operator
    ],
    ids=[
        "single_quoted",
        "double_quoted",
        "dollar_quoted",
        "line_comment",
        "block_comment",
        "postgres_json_exists",
        "postgres_json_exists_any",
        "postgres_json_exists_all",
        "postgres_cast",
    ],
)
def test_extract_parameters_ignores_special_cases(
    validator: ParameterValidator, sql: str, should_be_ignored: bool
) -> None:
    """Test that parameters in special contexts are handled correctly."""
    params = validator.extract_parameters(sql)

    if should_be_ignored:
        assert len(params) == 0
    else:
        # PostgreSQL cast should not create a parameter
        assert all(p.placeholder_text != "::int" for p in params)


@pytest.mark.parametrize(
    "sql,expected_style",
    [
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK),
        ("SELECT * FROM users WHERE name = :name", ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = %(id)s", ParameterStyle.NAMED_PYFORMAT),
        ("SELECT * FROM users WHERE name = %s", ParameterStyle.POSITIONAL_PYFORMAT),
        ("SELECT * FROM users WHERE id = @id", ParameterStyle.NAMED_AT),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC),
        ("SELECT * FROM users WHERE name = $name", ParameterStyle.NAMED_DOLLAR),
        ("SELECT * FROM users WHERE id = :1", ParameterStyle.POSITIONAL_COLON),
        ("SELECT * FROM users", ParameterStyle.NONE),
    ],
)
def test_get_parameter_style(validator: ParameterValidator, sql: str, expected_style: ParameterStyle) -> None:
    """Test parameter style detection."""
    params = validator.extract_parameters(sql)
    style = validator.get_parameter_style(params)
    assert style == expected_style


@pytest.mark.parametrize(
    "sql,expected_type",
    [
        ("SELECT * FROM users", None),
        ("SELECT * FROM users WHERE id = ?", list),
        ("SELECT * FROM users WHERE name = :name", dict),
        ("SELECT * FROM users WHERE id = %(id)s", dict),
        ("SELECT * FROM users WHERE name = %s", list),
        ("SELECT * FROM users WHERE id = @id", dict),
        ("SELECT * FROM users WHERE id = $1", list),
        ("SELECT * FROM users WHERE name = $name", dict),
    ],
)
def test_determine_parameter_input_type(validator: ParameterValidator, sql: str, expected_type: Optional[type]) -> None:
    """Test parameter input type determination."""
    params = validator.extract_parameters(sql)
    input_type = validator.determine_parameter_input_type(params)
    assert input_type == expected_type


@pytest.mark.parametrize(
    "sql,provided_params,should_pass",
    [
        # Valid cases
        ("SELECT * FROM users WHERE id = ?", [123], True),
        ("SELECT * FROM users WHERE name = :name", {"name": "John"}, True),
        ("SELECT * FROM users WHERE id = ? AND active = ?", [123, True], True),
        ("SELECT * FROM users WHERE name = :name AND email = :email", {"name": "John", "email": "john@test.com"}, True),
        ("SELECT * FROM users", None, True),
        ("SELECT * FROM users", [], True),
        # Invalid cases - style mismatch
        ("SELECT * FROM users WHERE id = ?", {"id": 123}, False),
        ("SELECT * FROM users WHERE name = :name", ["John"], False),
        # Invalid cases - missing parameters
        ("SELECT * FROM users WHERE id = ? AND active = ?", [123], False),
        ("SELECT * FROM users WHERE name = :name AND email = :email", {"name": "John"}, False),
        # Invalid cases - extra parameters
        ("SELECT * FROM users WHERE id = ?", [123, 456], False),
        ("SELECT * FROM users WHERE name = :name", {"name": "John", "extra": "value"}, False),
    ],
    ids=[
        "valid_qmark",
        "valid_named",
        "valid_multiple_qmark",
        "valid_multiple_named",
        "valid_no_params_none",
        "valid_no_params_empty",
        "invalid_qmark_with_dict",
        "invalid_named_with_list",
        "invalid_missing_qmark",
        "invalid_missing_named",
        "invalid_extra_qmark",
        "invalid_extra_named",
    ],
)
def test_validate_parameters(
    validator: ParameterValidator, sql: str, provided_params: "SQLParameterType", should_pass: bool
) -> None:
    """Test parameter validation."""
    params = validator.extract_parameters(sql)

    if should_pass:
        validator.validate_parameters(params, provided_params, sql)
    else:
        with pytest.raises((ParameterStyleMismatchError, MissingParameterError, ExtraParameterError)):
            validator.validate_parameters(params, provided_params, sql)


def test_parameter_extraction_caching(validator: ParameterValidator) -> None:
    """Test that parameter extraction results are cached."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"

    # First extraction
    params1 = validator.extract_parameters(sql)

    # Second extraction - should use cache
    params2 = validator.extract_parameters(sql)

    # Should be the same object (cached)
    assert params1 is params2


# Test ParameterConverter
@pytest.fixture
def converter() -> ParameterConverter:
    """Create a ParameterConverter instance."""
    return ParameterConverter()


@pytest.mark.parametrize(
    "parameters,args,kwargs,expected_result",
    [
        # Parameters takes precedence
        ({"id": 1}, [2], {"id": 3}, {"id": 1}),
        # kwargs when no parameters
        (None, None, {"name": "John"}, {"name": "John"}),
        # args when no parameters or kwargs
        (None, [123, "test"], None, [123, "test"]),
        # None when nothing provided
        (None, None, None, None),
        # Empty collections
        ({}, None, None, {}),
        (None, [], None, []),
    ],
    ids=["parameters_precedence", "kwargs_only", "args_only", "all_none", "empty_dict", "empty_list"],
)
def test_merge_parameters(
    converter: ParameterConverter,
    parameters: "SQLParameterType",
    args: "Optional[Sequence[Any]]",
    kwargs: "Optional[dict[str, Any]]",
    expected_result: "SQLParameterType",
) -> None:
    """Test parameter merging logic."""
    result = converter.merge_parameters(parameters, args, kwargs)
    assert result == expected_result


@pytest.mark.parametrize(
    "sql,parameters,args,kwargs,validate,should_succeed",
    [
        # Valid conversions
        ("SELECT * FROM users WHERE id = ?", None, [123], None, True, True),
        ("SELECT * FROM users WHERE name = :name", None, None, {"name": "John"}, True, True),
        ("SELECT * FROM users WHERE id = ? AND name = :name", None, [123], {"name": "John"}, True, True),
        ("SELECT * FROM users", None, None, None, True, True),
        # Invalid with validation enabled
        ("SELECT * FROM users WHERE id = ?", None, None, {"id": 123}, True, False),
        ("SELECT * FROM users WHERE name = :name", None, [123], None, True, False),
        # Valid with validation disabled
        ("SELECT * FROM users WHERE id = ?", None, None, {"id": 123}, False, True),
    ],
    ids=[
        "valid_qmark",
        "valid_named",
        "valid_mixed",
        "valid_no_params",
        "invalid_style_mismatch_qmark",
        "invalid_style_mismatch_named",
        "invalid_but_validation_off",
    ],
)
def test_convert_parameters(
    converter: ParameterConverter,
    sql: str,
    parameters: "SQLParameterType",
    args: "SQLParameterType",
    kwargs: "dict[str, Any]",
    validate: bool,
    should_succeed: bool,
) -> None:
    """Test parameter conversion process."""
    if should_succeed:
        result = converter.convert_parameters(sql, parameters, [args], kwargs, validate)
        transformed_sql, param_info, _merged_params, extra_info = result

        assert isinstance(transformed_sql, str)
        assert isinstance(param_info, list)
        assert isinstance(extra_info, dict)
    else:
        with pytest.raises((ParameterStyleMismatchError, MissingParameterError, ExtraParameterError)):
            converter.convert_parameters(sql, parameters, [args], kwargs, validate)


def test_transform_sql_for_parsing(converter: ParameterConverter) -> None:
    """Test SQL transformation for parsing."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    param_info = converter.validator.extract_parameters(sql)

    transformed_sql, placeholder_map = converter._transform_sql_for_parsing(sql, param_info)

    # Should have unique placeholder names
    assert ":__param_0" in transformed_sql
    assert ":__param_1" in transformed_sql

    # Should have mapping
    assert "__param_0" in placeholder_map
    assert "__param_1" in placeholder_map


def test_merge_mixed_parameters(converter: ParameterConverter) -> None:
    """Test merging of mixed parameter styles."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    param_info = converter.validator.extract_parameters(sql)
    args = [123]
    kwargs = {"name": "John"}

    merged = converter._merge_mixed_parameters(param_info, args, kwargs)

    assert isinstance(merged, dict)
    assert merged["name"] == "John"
    assert "_arg_0" in merged
    assert merged["_arg_0"] == 123


@pytest.mark.parametrize(
    "sql,expected_dominant_style",
    [
        ("SELECT * FROM users WHERE id = ? AND name = :name", ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = %(id)s AND active = %s", ParameterStyle.NAMED_PYFORMAT),
        ("SELECT * FROM users WHERE id = $1 AND name = $name", ParameterStyle.NAMED_DOLLAR),
    ],
)
def test_mixed_parameter_style_detection(
    validator: ParameterValidator, sql: str, expected_dominant_style: ParameterStyle
) -> None:
    """Test detection of dominant parameter style in mixed cases."""
    params = validator.extract_parameters(sql)
    style = validator.get_parameter_style(params)
    assert style == expected_dominant_style


def test_scalar_parameter_handling(converter: ParameterConverter) -> None:
    """Test handling of scalar parameters for single parameter queries."""
    sql = "SELECT * FROM users WHERE id = ?"
    result = converter.convert_parameters(sql, 123, None, None, validate=True)
    _, _, merged_params, _ = result
    assert merged_params == 123


def test_parameter_validation_with_empty_sql(validator: ParameterValidator) -> None:
    """Test parameter validation with empty SQL."""
    params = validator.extract_parameters("")
    assert len(params) == 0


def test_parameter_conversion_error_handling(converter: ParameterConverter) -> None:
    """Test parameter conversion error handling with validation disabled."""
    sql = "SELECT * FROM users WHERE id = ?"

    # Mock validation error
    with patch.object(converter.validator, "validate_parameters", side_effect=ValueError("Test error")):
        result = converter.convert_parameters(sql, {"id": 123}, None, None, validate=False)
        _, _, merged_params, _ = result
        assert merged_params == {"id": 123}


@pytest.mark.parametrize(
    "sql,params", [("SELECT * FROM users WHERE id = ?", []), ("SELECT * FROM users WHERE name = :name", {})]
)
def test_missing_parameter_errors(validator: ParameterValidator, sql: str, params: "SQLParameterType") -> None:
    """Test missing parameter error conditions."""
    param_info = validator.extract_parameters(sql)
    with pytest.raises(MissingParameterError):
        validator.validate_parameters(param_info, params, sql)


def test_complex_sql_parameter_extraction(validator: ParameterValidator) -> None:
    """Test parameter extraction from complex SQL with multiple styles."""
    sql = """
    SELECT u.*, o.*
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.name = :name
      AND u.email = %(email)s
      AND o.created_at > ?
      AND o.status = @status
      AND o.total > $1
    """
    params = validator.extract_parameters(sql)

    assert len(params) == 5
    styles = {p.style for p in params}
    assert ParameterStyle.NAMED_COLON in styles
    assert ParameterStyle.NAMED_PYFORMAT in styles
    assert ParameterStyle.QMARK in styles
    assert ParameterStyle.NAMED_AT in styles
    assert ParameterStyle.NUMERIC in styles


def test_parameter_position_tracking(validator: ParameterValidator) -> None:
    """Test that parameter positions are tracked correctly."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    params = validator.extract_parameters(sql)

    assert len(params) == 2
    qmark_param = next(p for p in params if p.style == ParameterStyle.QMARK)
    named_param = next(p for p in params if p.style == ParameterStyle.NAMED_COLON)

    assert qmark_param.position < named_param.position
    assert sql[qmark_param.position] == "?"
    assert sql[named_param.position : named_param.position + len(":name")] == ":name"


def test_positional_colon_parameters(validator: ParameterValidator) -> None:
    """Test Oracle-style numeric parameters (:1, :2, etc.)."""
    sql = "SELECT * FROM users WHERE id = :1 AND name = :2 AND active = :3"
    params = validator.extract_parameters(sql)

    assert len(params) == 3
    assert all(p.style == ParameterStyle.POSITIONAL_COLON for p in params)
    assert params[0].placeholder_text == ":1"
    assert params[1].placeholder_text == ":2"
    assert params[2].placeholder_text == ":3"


def test_dollar_numeric_vs_named_parameters(validator: ParameterValidator) -> None:
    """Test differentiation between $1 (numeric) and $name (named) parameters."""
    sql = "SELECT * FROM users WHERE id = $1 AND name = $username"
    params = validator.extract_parameters(sql)

    assert len(params) == 2
    numeric_param = next(p for p in params if p.placeholder_text == "$1")
    named_param = next(p for p in params if p.placeholder_text == "$username")

    assert numeric_param.style == ParameterStyle.NUMERIC
    assert named_param.style == ParameterStyle.NAMED_DOLLAR


def test_parameter_ordinal_assignment(validator: ParameterValidator) -> None:
    """Test that parameter ordinals are assigned correctly for positional parameters."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND email = ?"
    params = validator.extract_parameters(sql)

    assert len(params) == 3
    assert params[0].ordinal == 0
    assert params[1].ordinal == 1
    assert params[2].ordinal == 2


def test_parameter_info_repr() -> None:
    """Test ParameterInfo string representation."""
    param = ParameterInfo("test_param", ParameterStyle.NAMED_COLON, 10, 0, ":test_param")
    repr_str = repr(param)

    # Should contain key information
    assert "ParameterInfo" in repr_str
    assert "test_param" in repr_str
    assert "named_colon" in repr_str
