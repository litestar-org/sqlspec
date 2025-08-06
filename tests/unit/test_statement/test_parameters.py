"""Unit tests for sqlspec.parameters module."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

import pytest

from sqlspec.exceptions import ExtraParameterError, MissingParameterError, ParameterStyleMismatchError
from sqlspec.parameters import ParameterConverter, ParameterInfo, ParameterStyle, ParameterValidator

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters


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
        "no_parameters",
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
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == expected_count
    for i, expected_style in enumerate(expected_styles):
        assert parameters[i].style == expected_style


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
    parameters = validator.extract_parameters(sql)

    if should_be_ignored:
        assert len(parameters) == 0
    else:
        assert all(p.placeholder_text != "::int" for p in parameters)


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
    parameters = validator.extract_parameters(sql)
    style = validator.get_parameter_style(parameters)
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
    parameters = validator.extract_parameters(sql)
    input_type = validator.determine_parameter_input_type(parameters)
    assert input_type == expected_type


@pytest.mark.parametrize(
    "sql,provided_parameters,should_pass",
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
        "valid_no_parameters_none",
        "valid_no_parameters_empty",
        "invalid_qmark_with_dict",
        "invalid_named_with_list",
        "invalid_missing_qmark",
        "invalid_missing_named",
        "invalid_extra_qmark",
        "invalid_extra_named",
    ],
)
def test_validate_parameters(
    validator: ParameterValidator, sql: str, provided_parameters: "StatementParameters", should_pass: bool
) -> None:
    """Test parameter validation."""
    parameters = validator.extract_parameters(sql)

    if should_pass:
        validator.validate_parameters(parameters, provided_parameters, sql)
    else:
        with pytest.raises((ParameterStyleMismatchError, MissingParameterError, ExtraParameterError)):
            validator.validate_parameters(parameters, provided_parameters, sql)


def test_parameter_extraction_caching(validator: ParameterValidator) -> None:
    """Test that parameter extraction results are cached."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"

    parameters1 = validator.extract_parameters(sql)
    parameters2 = validator.extract_parameters(sql)

    assert parameters1 is parameters2


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
    parameters: "StatementParameters",
    args: "Optional[Sequence[Any]]",
    kwargs: "Optional[dict[str, Any]]",
    expected_result: "StatementParameters",
) -> None:
    """Test parameter merging logic."""
    result = converter.merge_parameters(parameters, list(args) if args is not None else None, kwargs)
    assert result == expected_result


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
    parameters = validator.extract_parameters(sql)
    style = validator.get_parameter_style(parameters)
    assert style == expected_dominant_style


def test_parameter_validation_with_empty_sql(validator: ParameterValidator) -> None:
    """Test parameter validation with empty SQL."""
    parameters = validator.extract_parameters("")
    assert len(parameters) == 0


@pytest.mark.parametrize(
    "sql,parameters", [("SELECT * FROM users WHERE id = ?", []), ("SELECT * FROM users WHERE name = :name", {})]
)
def test_missing_parameter_errors(validator: ParameterValidator, sql: str, parameters: "StatementParameters") -> None:
    """Test missing parameter error conditions."""
    param_info = validator.extract_parameters(sql)
    with pytest.raises(MissingParameterError):
        validator.validate_parameters(param_info, parameters, sql)


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
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 5
    styles = {p.style for p in parameters}
    assert ParameterStyle.NAMED_COLON in styles
    assert ParameterStyle.NAMED_PYFORMAT in styles
    assert ParameterStyle.QMARK in styles
    assert ParameterStyle.NAMED_AT in styles
    assert ParameterStyle.NUMERIC in styles


def test_parameter_position_tracking(validator: ParameterValidator) -> None:
    """Test that parameter positions are tracked correctly."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 2
    qmark_param = next(p for p in parameters if p.style == ParameterStyle.QMARK)
    named_param = next(p for p in parameters if p.style == ParameterStyle.NAMED_COLON)

    assert qmark_param.position < named_param.position
    assert sql[qmark_param.position] == "?"
    assert sql[named_param.position : named_param.position + len(":name")] == ":name"


def test_positional_colon_parameters(validator: ParameterValidator) -> None:
    """Test Oracle-style numeric parameters (:1, :2, etc.)."""
    sql = "SELECT * FROM users WHERE id = :1 AND name = :2 AND active = :3"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 3
    assert all(p.style == ParameterStyle.POSITIONAL_COLON for p in parameters)
    assert parameters[0].placeholder_text == ":1"
    assert parameters[1].placeholder_text == ":2"
    assert parameters[2].placeholder_text == ":3"


def test_dollar_numeric_vs_named_parameters(validator: ParameterValidator) -> None:
    """Test differentiation between $1 (numeric) and $name (named) parameters."""
    sql = "SELECT * FROM users WHERE id = $1 AND name = $username"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 2
    numeric_param = next(p for p in parameters if p.placeholder_text == "$1")
    named_param = next(p for p in parameters if p.placeholder_text == "$username")

    assert numeric_param.style == ParameterStyle.NUMERIC
    assert named_param.style == ParameterStyle.NAMED_DOLLAR


def test_parameter_ordinal_assignment(validator: ParameterValidator) -> None:
    """Test that parameter ordinals are assigned correctly for positional parameters."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND email = ?"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 3
    assert parameters[0].ordinal == 0
    assert parameters[1].ordinal == 1
    assert parameters[2].ordinal == 2


def test_parameter_info_repr() -> None:
    """Test ParameterInfo string representation."""
    param = ParameterInfo("test_param", ParameterStyle.NAMED_COLON, 10, 0, ":test_param")
    repr_str = repr(param)

    assert "ParameterInfo" in repr_str
    assert "test_param" in repr_str
    assert "named_colon" in repr_str
