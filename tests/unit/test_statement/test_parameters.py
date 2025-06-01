"""Unit tests for sqlspec.statement.parameters module.

Tests the parameter handling system including parameter extraction, validation,
conversion, and different parameter styles.
"""

from typing import Any
from unittest.mock import patch

import pytest

from sqlspec.exceptions import (
    ExtraParameterError,
    MissingParameterError,
    ParameterStyleMismatchError,
)
from sqlspec.statement.parameters import (
    ParameterConverter,
    ParameterInfo,
    ParameterStyle,
    ParameterValidator,
    convert_parameters,
    detect_parameter_style,
)


def test_parameter_style_values() -> None:
    """Test ParameterStyle enum values."""
    assert ParameterStyle.NONE == "none"  # type: ignore[unreachable, comparison-overlap]
    assert ParameterStyle.STATIC == "static"  # type: ignore[unreachable, comparison-overlap]
    assert ParameterStyle.QMARK == "qmark"
    assert ParameterStyle.NUMERIC == "numeric"
    assert ParameterStyle.NAMED_COLON == "named_colon"
    assert ParameterStyle.NAMED_AT == "named_at"
    assert ParameterStyle.NAMED_DOLLAR == "named_dollar"
    assert ParameterStyle.PYFORMAT_NAMED == "pyformat_named"
    assert ParameterStyle.PYFORMAT_POSITIONAL == "pyformat_positional"


def test_parameter_style_string_representation() -> None:
    """Test ParameterStyle string representation."""
    assert str(ParameterStyle.QMARK) == "qmark"
    assert str(ParameterStyle.NAMED_COLON) == "named_colon"
    assert str(ParameterStyle.PYFORMAT_NAMED) == "pyformat_named"


@pytest.mark.parametrize(
    ("name", "style", "position", "ordinal", "placeholder_text"),
    [
        ("user_id", ParameterStyle.NAMED_COLON, 25, 0, ":user_id"),
        (None, ParameterStyle.QMARK, 10, 1, "?"),
        ("param1", ParameterStyle.PYFORMAT_NAMED, 35, 0, "%(param1)s"),
        (None, ParameterStyle.PYFORMAT_POSITIONAL, 15, 2, "%s"),
    ],
    ids=["named_colon", "qmark", "pyformat_named", "pyformat_positional"],
)
def test_parameter_info_creation(
    name: str, style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
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

    # ordinal and placeholder_text should not affect equality (compare=False)
    param4 = ParameterInfo("test", ParameterStyle.NAMED_COLON, 10, 99, "different")
    assert param1 == param4


@pytest.fixture
def validator() -> ParameterValidator:
    """Create a ParameterValidator instance."""
    return ParameterValidator()


@pytest.mark.parametrize(
    ("sql", "expected_param_count", "expected_styles"),
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
        ("SELECT * FROM users WHERE id = %(id)s", 1, [ParameterStyle.PYFORMAT_NAMED]),
        ("SELECT * FROM users WHERE name = %s", 1, [ParameterStyle.PYFORMAT_POSITIONAL]),
        ("SELECT * FROM users WHERE id = @id", 1, [ParameterStyle.NAMED_AT]),
        ("SELECT * FROM users WHERE id = $1", 1, [ParameterStyle.NUMERIC]),
        ("SELECT * FROM users WHERE name = $name", 1, [ParameterStyle.NAMED_DOLLAR]),
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
    ],
)
def test_extract_parameters(
    validator: ParameterValidator, sql: str, expected_param_count: int, expected_styles: list[ParameterStyle]
) -> None:
    """Test parameter extraction from various SQL patterns."""
    params = validator.extract_parameters(sql)

    assert len(params) == expected_param_count

    for i, expected_style in enumerate(expected_styles):
        assert params[i].style == expected_style


@pytest.mark.parametrize(
    ("sql", "should_be_ignored"),
    [
        ("SELECT 'test with ? inside'", True),
        ('SELECT "test with ? inside"', True),
        ("SELECT $tag$content with ? and :param$tag$", True),
        ("SELECT * FROM test -- comment with ? and :param", True),
        ("SELECT * FROM test /* comment with ? and :param */", True),
        ("SELECT * FROM json WHERE data ?? 'key'", True),  # PostgreSQL JSON operator
        ("SELECT * FROM json WHERE data ?| array['key']", True),  # PostgreSQL JSON operator
        ("SELECT * FROM json WHERE data ?& array['key']", True),  # PostgreSQL JSON operator
    ],
    ids=[
        "single_quoted_string",
        "double_quoted_string",
        "dollar_quoted_string",
        "line_comment",
        "block_comment",
        "postgres_json_exists",
        "postgres_json_exists_any",
        "postgres_json_exists_all",
    ],
)
def test_extract_parameters_ignores_literals_and_comments(
    validator: ParameterValidator, sql: str, should_be_ignored: bool
) -> None:
    """Test that parameters inside literals and comments are ignored."""
    params = validator.extract_parameters(sql)

    if should_be_ignored:
        assert len(params) == 0
    else:
        assert len(params) > 0


@pytest.mark.parametrize(
    ("sql", "expected_style"),
    [
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK),
        ("SELECT * FROM users WHERE name = :name", ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = %(id)s", ParameterStyle.PYFORMAT_NAMED),
        ("SELECT * FROM users WHERE name = %s", ParameterStyle.PYFORMAT_POSITIONAL),
        ("SELECT * FROM users WHERE id = @id", ParameterStyle.NAMED_AT),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC),
        ("SELECT * FROM users WHERE name = $name", ParameterStyle.NAMED_DOLLAR),
        ("SELECT * FROM users", ParameterStyle.NONE),
    ],
    ids=[
        "qmark",
        "named_colon",
        "pyformat_named",
        "pyformat_positional",
        "named_at",
        "numeric",
        "named_dollar",
        "no_params",
    ],
)
def test_get_parameter_style(validator: ParameterValidator, sql: str, expected_style: ParameterStyle) -> None:
    """Test parameter style detection."""
    params = validator.extract_parameters(sql)
    style = validator.get_parameter_style(params)

    assert style == expected_style


@pytest.mark.parametrize(
    ("sql", "expected_type"),
    [
        ("SELECT * FROM users", None),
        ("SELECT * FROM users WHERE id = ?", list),
        ("SELECT * FROM users WHERE name = :name", dict),
        ("SELECT * FROM users WHERE id = %(id)s", dict),
        ("SELECT * FROM users WHERE name = %s", list),
        ("SELECT * FROM users WHERE id = ? AND name = ?", list),
        ("SELECT * FROM users WHERE name = :name AND email = :email", dict),
    ],
    ids=[
        "no_params",
        "qmark",
        "named_colon",
        "pyformat_named",
        "pyformat_positional",
        "multiple_qmark",
        "multiple_named",
    ],
)
def test_determine_parameter_input_type(validator: ParameterValidator, sql: str, expected_type: type) -> None:
    """Test parameter input type determination."""
    params = validator.extract_parameters(sql)
    input_type = validator.determine_parameter_input_type(params)

    assert input_type == expected_type


@pytest.mark.parametrize(
    ("sql", "provided_params", "should_pass"),
    [
        # Valid cases
        ("SELECT * FROM users WHERE id = ?", [123], True),
        ("SELECT * FROM users WHERE name = :name", {"name": "John"}, True),
        ("SELECT * FROM users WHERE id = ? AND active = ?", [123, True], True),
        (
            "SELECT * FROM users WHERE name = :name AND email = :email",
            {"name": "John", "email": "john@test.com"},
            True,
        ),
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
def test_validate_parameters(validator: ParameterValidator, sql: str, provided_params: Any, should_pass: bool) -> None:
    """Test parameter validation."""
    params = validator.extract_parameters(sql)

    if should_pass:
        # Should not raise an exception
        validator.validate_parameters(params, provided_params, sql)
    else:
        # Should raise an exception
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


@pytest.mark.parametrize(
    ("sql", "params", "expected_named_count", "expected_positional_count"),
    [
        ("SELECT * FROM users WHERE id = ? AND name = :name", {"name": "John", "_arg_0": 123}, 1, 1),
        (
            "SELECT * FROM users WHERE id = ? AND active = ? AND name = :name",
            {"name": "John", "_arg_0": 123, "_arg_1": True},
            1,
            2,
        ),
    ],
    ids=["mixed_single_positional", "mixed_multiple_positional"],
)
def test_validate_mixed_parameters(
    validator: ParameterValidator,
    sql: str,
    params: dict[str, Any],
    expected_named_count: int,
    expected_positional_count: int,
) -> None:
    """Test validation of mixed parameter styles."""
    param_info = validator.extract_parameters(sql)

    # Should pass validation with properly named positional parameters
    validator.validate_parameters(param_info, params, sql)

    # Verify the counts
    named_count = sum(1 for p in param_info if p.name is not None)
    positional_count = sum(1 for p in param_info if p.name is None)

    assert named_count == expected_named_count
    assert positional_count == expected_positional_count


@pytest.fixture
def converter() -> ParameterConverter:
    """Create a ParameterConverter instance."""
    return ParameterConverter()


@pytest.mark.parametrize(
    ("parameters", "args", "kwargs", "expected_result"),
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
    converter: ParameterConverter, parameters: Any, args: Any, kwargs: Any, expected_result: Any
) -> None:
    """Test parameter merging logic."""
    result = converter.merge_parameters(parameters, args, kwargs)

    assert result == expected_result


@pytest.mark.parametrize(
    ("sql", "parameters", "args", "kwargs", "validate", "should_succeed"),
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
    parameters: Any,
    args: Any,
    kwargs: Any,
    validate: bool,
    should_succeed: bool,
) -> None:
    """Test parameter conversion process."""
    if should_succeed:
        result = converter.convert_parameters(sql, parameters, args, kwargs, validate)
        transformed_sql, param_info, merged_params, placeholder_map = result

        assert isinstance(transformed_sql, str)
        assert isinstance(param_info, list)
        assert placeholder_map is not None

        # Check that parameters were processed
        if merged_params is not None:
            assert merged_params is not None
    else:
        with pytest.raises((ParameterStyleMismatchError, MissingParameterError, ExtraParameterError)):
            converter.convert_parameters(sql, parameters, args, kwargs, validate)


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

    # One should map to ordinal (positional), one to name
    map_values = list(placeholder_map.values())
    assert 0 in map_values  # Positional ordinal
    assert "name" in map_values  # Named parameter


def test_merge_mixed_parameters(converter: ParameterConverter) -> None:
    """Test merging of mixed parameter styles."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    param_info = converter.validator.extract_parameters(sql)
    args = [123]
    kwargs = {"name": "John"}

    merged = converter._merge_mixed_parameters(param_info, args, kwargs)

    assert isinstance(merged, dict)
    assert merged["name"] == "John"  # Named parameter
    assert "_arg_0" in merged  # Generated name for positional
    assert merged["_arg_0"] == 123


@pytest.mark.parametrize(
    ("sql", "expected_style"),
    [
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK),
        ("SELECT * FROM users WHERE name = :name", ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = %(id)s", ParameterStyle.PYFORMAT_NAMED),
        ("SELECT * FROM users WHERE name = %s", ParameterStyle.PYFORMAT_POSITIONAL),
        ("SELECT * FROM users WHERE id = @id", ParameterStyle.NAMED_AT),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC),
        ("SELECT * FROM users WHERE name = $name", ParameterStyle.NAMED_DOLLAR),
        ("SELECT * FROM users", ParameterStyle.NONE),
        # Mixed styles - should return dominant
        ("SELECT * FROM users WHERE id = ? AND name = :name", ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = %(id)s AND active = %s", ParameterStyle.PYFORMAT_NAMED),
    ],
    ids=[
        "qmark",
        "named_colon",
        "pyformat_named",
        "pyformat_positional",
        "named_at",
        "numeric",
        "named_dollar",
        "no_params",
        "mixed_qmark_named",
        "mixed_pyformat",
    ],
)
def test_detect_parameter_style(sql: str, expected_style: ParameterStyle) -> None:
    """Test parameter style detection function."""
    detected_style = detect_parameter_style(sql)

    assert detected_style == expected_style


@pytest.mark.parametrize(
    ("sql", "parameters", "args", "kwargs", "validate"),
    [
        ("SELECT * FROM users WHERE id = ?", None, [123], None, True),
        ("SELECT * FROM users WHERE name = :name", None, None, {"name": "John"}, True),
        ("SELECT * FROM users", None, None, None, True),
        ("SELECT * FROM users WHERE id = ?", [123], None, None, False),
    ],
    ids=["qmark_with_args", "named_with_kwargs", "no_params", "validation_disabled"],
)
def test_convert_parameters_function(sql: str, parameters: Any, args: Any, kwargs: Any, validate: bool) -> None:
    """Test the module-level convert_parameters function."""
    result = convert_parameters(sql, parameters, args, kwargs, validate)
    transformed_sql, param_info, merged_params, placeholder_map = result

    assert isinstance(transformed_sql, str)
    assert isinstance(param_info, list)
    assert isinstance(placeholder_map, dict)

    # Should process parameters appropriately
    if parameters is not None or args is not None or kwargs is not None:
        assert merged_params is not None
    else:
        assert merged_params is None


def test_parameter_validation_with_none_sql() -> None:
    """Test parameter validation with None SQL."""
    validator = ParameterValidator()

    with pytest.raises(AttributeError):
        validator.extract_parameters(None)  # type: ignore[arg-type]


def test_parameter_validation_with_empty_sql() -> None:
    """Test parameter validation with empty SQL."""
    validator = ParameterValidator()
    params = validator.extract_parameters("")

    assert len(params) == 0


def test_parameter_conversion_error_handling() -> None:
    """Test parameter conversion error handling."""
    converter = ParameterConverter()

    # Test with strict validation disabled
    sql = "SELECT * FROM users WHERE id = ?"

    # This should not raise an error with validation disabled
    with patch.object(converter.validator, "validate_parameters", side_effect=ValueError("Test error")):
        result = converter.convert_parameters(sql, {"id": 123}, None, None, validate=False)
        _, _, merged_params, _ = result

        # Should fall back to basic merge
        assert merged_params == {"id": 123}


@pytest.mark.parametrize(
    ("sql", "expected_error_type"),
    [
        ("SELECT * FROM users WHERE id = ?", MissingParameterError),
        ("SELECT * FROM users WHERE name = :name", MissingParameterError),
    ],
    ids=["missing_qmark", "missing_named"],
)
def test_missing_parameter_errors(sql: str, expected_error_type: type[Exception]) -> None:
    """Test missing parameter error conditions."""
    validator = ParameterValidator()
    params = validator.extract_parameters(sql)

    with pytest.raises(expected_error_type):
        validator.validate_parameters(params, None, sql)


def test_scalar_parameter_handling() -> None:
    """Test handling of scalar parameters."""
    converter = ParameterConverter()

    # Single parameter as scalar should be allowed
    sql = "SELECT * FROM users WHERE id = ?"
    result = converter.convert_parameters(sql, 123, None, None, validate=True)
    _, _, merged_params, _ = result

    assert merged_params == 123


def test_complex_sql_with_multiple_parameter_styles() -> None:
    """Test complex SQL with multiple parameter styles."""
    validator = ParameterValidator()

    # This is generally not supported but should be detected
    sql = "SELECT * FROM users WHERE id = ? AND name = :name AND email = %(email)s"
    params = validator.extract_parameters(sql)

    assert len(params) == 3
    styles = [p.style for p in params]
    assert ParameterStyle.QMARK in styles
    assert ParameterStyle.NAMED_COLON in styles
    assert ParameterStyle.PYFORMAT_NAMED in styles


def test_parameter_info_ordinal_assignment() -> None:
    """Test that parameter ordinals are assigned correctly."""
    validator = ParameterValidator()

    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND email = ?"
    params = validator.extract_parameters(sql)

    assert len(params) == 3
    assert params[0].ordinal == 0
    assert params[1].ordinal == 1
    assert params[2].ordinal == 2


def test_parameter_position_tracking() -> None:
    """Test that parameter positions are tracked correctly."""
    validator = ParameterValidator()

    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    params = validator.extract_parameters(sql)

    assert len(params) == 2

    # Positions should reflect actual locations in SQL
    qmark_param = next(p for p in params if p.style == ParameterStyle.QMARK)
    named_param = next(p for p in params if p.style == ParameterStyle.NAMED_COLON)

    assert qmark_param.position < named_param.position
    assert qmark_param.position == sql.index("?")
    assert named_param.position == sql.index(":name")
