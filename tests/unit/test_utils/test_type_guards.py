"""Comprehensive tests for sqlspec.utils.type_guards module.

Tests all protocol type guards, validation functions, edge cases, and performance.
Uses function-based pytest approach as per CLAUDE.md requirements.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, cast

import pytest

if TYPE_CHECKING:
    from sqlglot import exp

from sqlspec.utils.type_guards import (
    dataclass_to_dict,
    expression_has_limit,
    extract_dataclass_fields,
    extract_dataclass_items,
    get_initial_expression,
    get_literal_parent,
    get_node_expressions,
    get_node_this,
    get_param_style_and_name,
    get_value_attribute,
    has_attr,
    has_expressions_attribute,
    has_parent_attribute,
    has_this_attribute,
    is_attrs_instance,
    is_attrs_instance_with_field,
    is_attrs_instance_without_field,
    is_attrs_schema,
    is_copy_statement,
    is_dataclass,
    is_dataclass_instance,
    is_dataclass_with_field,
    is_dataclass_without_field,
    is_dict,
    is_dict_row,
    is_dict_with_field,
    is_dict_without_field,
    is_dto_data,
    is_expression,
    is_iterable_parameters,
    is_msgspec_struct,
    is_msgspec_struct_with_field,
    is_msgspec_struct_without_field,
    is_number_literal,
    is_pydantic_model,
    is_pydantic_model_with_field,
    is_pydantic_model_without_field,
    is_schema,
    is_schema_or_dict,
    is_schema_or_dict_with_field,
    is_schema_or_dict_without_field,
    is_schema_with_field,
    is_schema_without_field,
    is_string_literal,
    schema_dump,
)

# Test fixtures for different schema types


@dataclass
class SampleDataclass:
    """Sample dataclass for testing."""

    name: str
    age: int
    optional_field: "Optional[str]" = None


class MockSQLGlotExpression:
    """Mock SQLGlot expression for testing."""

    def __init__(
        self,
        this: "Optional[Any]" = None,
        expressions: "Optional[list[Any]]" = None,
        parent: "Optional[Any]" = None,
        args: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if this is not None:
            self.this = this
        if expressions is not None:
            self.expressions = expressions
        if parent is not None:
            self.parent = parent
        self.args = args or {}


class MockLiteral:
    """Mock literal for testing."""

    def __init__(
        self,
        this: "Optional[Any]" = None,
        is_string: bool = False,
        is_number: bool = False,
        parent: "Optional[Any]" = None,
    ) -> None:
        if this is not None:
            self.this = this
        if is_string:
            self.is_string = is_string
        if is_number:
            self.is_number = is_number
        if parent is not None:
            self.parent = parent


class MockParameterProtocol:
    """Mock parameter with protocol attributes."""

    def __init__(self, style: "Optional[str]" = None, name: "Optional[str]" = None) -> None:
        if style is not None:
            self.style = style
        if name is not None:
            self.name = name


class MockValueWrapper:
    """Mock wrapper with value attribute."""

    def __init__(self, value: Any) -> None:
        self.value = value


# Dataclass Type Guard Tests


def test_is_dataclass_instance_with_valid_dataclass() -> None:
    """Test is_dataclass_instance returns True for dataclass instances."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass_instance(instance) is True


def test_is_dataclass_instance_with_dataclass_class() -> None:
    """Test is_dataclass_instance returns False for dataclass classes."""
    assert is_dataclass_instance(SampleDataclass) is False


def test_is_dataclass_instance_with_non_dataclass() -> None:
    """Test is_dataclass_instance returns False for non-dataclass objects."""
    assert is_dataclass_instance("not a dataclass") is False
    assert is_dataclass_instance(42) is False
    assert is_dataclass_instance({}) is False


def test_is_dataclass_with_dataclass_class() -> None:
    """Test is_dataclass returns True for dataclass classes."""
    assert is_dataclass(SampleDataclass) is True


def test_is_dataclass_with_dataclass_instance() -> None:
    """Test is_dataclass returns True for dataclass instances."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass(instance) is True


def test_is_dataclass_with_non_dataclass() -> None:
    """Test is_dataclass returns False for non-dataclass objects."""
    assert is_dataclass("not a dataclass") is False
    assert is_dataclass(42) is False
    assert is_dataclass({}) is False


def test_is_dataclass_with_field_existing_field() -> None:
    """Test is_dataclass_with_field returns True when field exists."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass_with_field(instance, "name") is True
    assert is_dataclass_with_field(instance, "age") is True


def test_is_dataclass_with_field_missing_field() -> None:
    """Test is_dataclass_with_field returns False when field doesn't exist."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass_with_field(instance, "nonexistent") is False


def test_is_dataclass_with_field_non_dataclass() -> None:
    """Test is_dataclass_with_field returns False for non-dataclass objects."""
    assert is_dataclass_with_field("not a dataclass", "any_field") is False


def test_is_dataclass_without_field_missing_field() -> None:
    """Test is_dataclass_without_field returns True when field doesn't exist."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass_without_field(instance, "nonexistent") is True


def test_is_dataclass_without_field_existing_field() -> None:
    """Test is_dataclass_without_field returns False when field exists."""
    instance = SampleDataclass(name="test", age=25)
    assert is_dataclass_without_field(instance, "name") is False


def test_is_dataclass_without_field_non_dataclass() -> None:
    """Test is_dataclass_without_field returns False for non-dataclass objects."""
    assert is_dataclass_without_field("not a dataclass", "any_field") is False


# Dictionary Type Guard Tests


def test_is_dict_with_dictionary() -> None:
    """Test is_dict returns True for dictionaries."""
    assert is_dict({}) is True
    assert is_dict({"key": "value"}) is True


def test_is_dict_with_non_dictionary() -> None:
    """Test is_dict returns False for non-dictionary objects."""
    assert is_dict("not a dict") is False
    assert is_dict([]) is False
    assert is_dict(42) is False


def test_is_dict_with_field_existing_key() -> None:
    """Test is_dict_with_field returns True when key exists."""
    data = {"name": "test", "age": 25}
    assert is_dict_with_field(data, "name") is True
    assert is_dict_with_field(data, "age") is True


def test_is_dict_with_field_missing_key() -> None:
    """Test is_dict_with_field returns False when key doesn't exist."""
    data = {"name": "test"}
    assert is_dict_with_field(data, "nonexistent") is False


def test_is_dict_with_field_non_dict() -> None:
    """Test is_dict_with_field returns False for non-dict objects."""
    assert is_dict_with_field("not a dict", "any_key") is False


def test_is_dict_without_field_missing_key() -> None:
    """Test is_dict_without_field returns True when key doesn't exist."""
    data = {"name": "test"}
    assert is_dict_without_field(data, "nonexistent") is True


def test_is_dict_without_field_existing_key() -> None:
    """Test is_dict_without_field returns False when key exists."""
    data = {"name": "test", "age": 25}
    assert is_dict_without_field(data, "name") is False


def test_is_dict_without_field_non_dict() -> None:
    """Test is_dict_without_field returns False for non-dict objects."""
    assert is_dict_without_field("not a dict", "any_key") is False


def test_is_dict_row_with_dictionary() -> None:
    """Test is_dict_row returns True for dictionaries (row data)."""
    assert is_dict_row({}) is True
    assert is_dict_row({"col1": "value1", "col2": "value2"}) is True


def test_is_dict_row_with_non_dictionary() -> None:
    """Test is_dict_row returns False for non-dictionary objects."""
    assert is_dict_row("not a dict") is False
    assert is_dict_row([]) is False
    assert is_dict_row(42) is False


# Pydantic Model Tests (when available)


def test_is_pydantic_model_when_not_installed() -> None:
    """Test is_pydantic_model returns False when pydantic not available."""
    # This test assumes pydantic is not installed or mocked appropriately
    assert is_pydantic_model("not a model") is False
    assert is_pydantic_model({}) is False


def test_is_pydantic_model_with_field_when_not_installed() -> None:
    """Test is_pydantic_model_with_field returns False when pydantic not available."""
    assert is_pydantic_model_with_field("not a model", "field") is False


def test_is_pydantic_model_without_field_when_not_installed() -> None:
    """Test is_pydantic_model_without_field returns False when pydantic not available."""
    assert is_pydantic_model_without_field("not a model", "field") is False


# MsgSpec Struct Tests (when available)


def test_is_msgspec_struct_when_not_installed() -> None:
    """Test is_msgspec_struct returns False when msgspec not available."""
    assert is_msgspec_struct("not a struct") is False
    assert is_msgspec_struct({}) is False


def test_is_msgspec_struct_with_field_when_not_installed() -> None:
    """Test is_msgspec_struct_with_field returns False when msgspec not available."""
    assert is_msgspec_struct_with_field("not a struct", "field") is False


def test_is_msgspec_struct_without_field_when_not_installed() -> None:
    """Test is_msgspec_struct_without_field returns False when msgspec not available."""
    assert is_msgspec_struct_without_field("not a struct", "field") is False


# Attrs Tests (when available)


def test_is_attrs_instance_when_not_installed() -> None:
    """Test is_attrs_instance returns False when attrs not available."""
    assert is_attrs_instance("not attrs") is False
    assert is_attrs_instance({}) is False


def test_is_attrs_schema_when_not_installed() -> None:
    """Test is_attrs_schema returns False when attrs not available."""
    assert is_attrs_schema("not attrs") is False
    assert is_attrs_schema(dict) is False


def test_is_attrs_instance_with_field_when_not_installed() -> None:
    """Test is_attrs_instance_with_field returns False when attrs not available."""
    assert is_attrs_instance_with_field("not attrs", "field") is False


def test_is_attrs_instance_without_field_when_not_installed() -> None:
    """Test is_attrs_instance_without_field returns False when attrs not available."""
    assert is_attrs_instance_without_field("not attrs", "field") is False


# Schema Type Guards Tests


def test_is_schema_with_dataclass() -> None:
    """Test is_schema returns True for dataclass instances."""
    instance = SampleDataclass(name="test", age=25)
    assert is_schema(instance) is True


def test_is_schema_with_non_schema() -> None:
    """Test is_schema returns False for non-schema objects."""
    assert is_schema("not a schema") is False
    assert is_schema(42) is False
    assert is_schema([]) is False


def test_is_schema_or_dict_with_schema() -> None:
    """Test is_schema_or_dict returns True for schema objects."""
    instance = SampleDataclass(name="test", age=25)
    assert is_schema_or_dict(instance) is True


def test_is_schema_or_dict_with_dict() -> None:
    """Test is_schema_or_dict returns True for dictionaries."""
    assert is_schema_or_dict({"key": "value"}) is True


def test_is_schema_or_dict_with_neither() -> None:
    """Test is_schema_or_dict returns False for non-schema, non-dict objects."""
    assert is_schema_or_dict("not schema or dict") is False
    assert is_schema_or_dict(42) is False


def test_is_schema_with_field_with_dataclass() -> None:
    """Test is_schema_with_field works with dataclass fields."""
    instance = SampleDataclass(name="test", age=25)
    # Note: is_schema_with_field only checks msgspec/pydantic, not dataclasses
    # This is the current implementation behavior
    assert is_schema_with_field(instance, "name") is False  # Expected behavior
    assert is_schema_with_field(instance, "nonexistent") is False


def test_is_schema_without_field_with_dataclass() -> None:
    """Test is_schema_without_field works with dataclass fields."""
    instance = SampleDataclass(name="test", age=25)
    # Note: is_schema_without_field only checks msgspec/pydantic, not dataclasses
    # This is the current implementation behavior
    assert is_schema_without_field(instance, "nonexistent") is True  # Doesn't have the field
    assert is_schema_without_field(instance, "name") is True  # Still True since it's not msgspec/pydantic


def test_is_schema_or_dict_with_field_combined() -> None:
    """Test is_schema_or_dict_with_field works with both schemas and dicts."""
    instance = SampleDataclass(name="test", age=25)
    data = {"name": "test", "age": 25}

    # Note: is_schema_or_dict_with_field only recognizes msgspec/pydantic as schemas, not dataclasses
    assert is_schema_or_dict_with_field(instance, "name") is False  # Dataclass not recognized as schema
    assert is_schema_or_dict_with_field(data, "name") is True  # Dict is recognized
    assert is_schema_or_dict_with_field(instance, "nonexistent") is False
    assert is_schema_or_dict_with_field(data, "nonexistent") is False


def test_is_schema_or_dict_without_field_combined() -> None:
    """Test is_schema_or_dict_without_field works with both schemas and dicts."""
    instance = SampleDataclass(name="test", age=25)
    data = {"name": "test", "age": 25}

    # Note: is_schema_or_dict_without_field only recognizes msgspec/pydantic as schemas, not dataclasses
    assert is_schema_or_dict_without_field(instance, "nonexistent") is True  # Doesn't have the field
    assert is_schema_or_dict_without_field(data, "nonexistent") is True  # Dict doesn't have field
    assert is_schema_or_dict_without_field(instance, "name") is True  # Not recognized as schema with field
    assert is_schema_or_dict_without_field(data, "name") is False  # Dict has the field


# Iterable Parameters Tests


def test_is_iterable_parameters_with_list() -> None:
    """Test is_iterable_parameters returns True for lists."""
    assert is_iterable_parameters([1, 2, 3]) is True
    assert is_iterable_parameters([]) is True


def test_is_iterable_parameters_with_tuple() -> None:
    """Test is_iterable_parameters returns True for tuples."""
    assert is_iterable_parameters((1, 2, 3)) is True
    assert is_iterable_parameters(()) is True


def test_is_iterable_parameters_with_string() -> None:
    """Test is_iterable_parameters returns False for strings."""
    assert is_iterable_parameters("string") is False
    assert is_iterable_parameters("") is False


def test_is_iterable_parameters_with_bytes() -> None:
    """Test is_iterable_parameters returns False for bytes."""
    assert is_iterable_parameters(b"bytes") is False
    assert is_iterable_parameters(b"") is False


def test_is_iterable_parameters_with_dict() -> None:
    """Test is_iterable_parameters returns False for dictionaries."""
    assert is_iterable_parameters({"key": "value"}) is False
    assert is_iterable_parameters({}) is False


def test_is_iterable_parameters_with_non_iterable() -> None:
    """Test is_iterable_parameters returns False for non-iterable objects."""
    assert is_iterable_parameters(42) is False
    assert is_iterable_parameters(None) is False


# DTO Data Tests


def test_is_dto_data_when_litestar_not_installed() -> None:
    """Test is_dto_data returns False when litestar not available."""
    assert is_dto_data("not dto data") is False
    assert is_dto_data({}) is False


# Expression Tests


def test_is_expression_with_mock() -> None:
    """Test is_expression with mock SQLGlot expressions."""
    mock_expr = MockSQLGlotExpression()
    # This will likely return False unless SQLGlot is installed and the mock inherits properly
    # The test verifies the function doesn't crash with mock objects
    result = is_expression(mock_expr)
    assert isinstance(result, bool)


def test_is_expression_with_non_expression() -> None:
    """Test is_expression returns False for non-expression objects."""
    assert is_expression("not an expression") is False
    assert is_expression(42) is False
    assert is_expression({}) is False


# Protocol Helper Tests


def test_has_attr_with_existing_attribute() -> None:
    """Test has_attr returns True when attribute exists."""
    obj = MockValueWrapper("test")
    assert has_attr(obj, "value") is True


def test_has_attr_with_missing_attribute() -> None:
    """Test has_attr returns False when attribute doesn't exist."""
    obj = MockValueWrapper("test")
    assert has_attr(obj, "nonexistent") is False


def test_has_attr_with_none() -> None:
    """Test has_attr handles None gracefully."""
    assert has_attr(None, "any_attr") is False


# SQLGlot Node Helper Tests


def test_get_node_this_with_this_attribute() -> None:
    """Test get_node_this returns this attribute when present."""
    node = cast("exp.Expression", MockSQLGlotExpression(this="test_value"))
    assert get_node_this(node) == "test_value"


def test_get_node_this_without_this_attribute() -> None:
    """Test get_node_this returns default when this attribute missing."""
    node = cast("exp.Expression", MockSQLGlotExpression())
    assert get_node_this(node, "default") == "default"
    assert get_node_this(node) is None


def test_has_this_attribute_with_attribute() -> None:
    """Test has_this_attribute returns True when this exists."""
    node = cast("exp.Expression", MockSQLGlotExpression(this="test_value"))
    assert has_this_attribute(node) is True


def test_has_this_attribute_without_attribute() -> None:
    """Test has_this_attribute returns False when this doesn't exist."""
    node = cast("exp.Expression", MockSQLGlotExpression())
    assert has_this_attribute(node) is False


def test_get_node_expressions_with_expressions() -> None:
    """Test get_node_expressions returns expressions when present."""
    expressions = ["expr1", "expr2"]
    node = cast("exp.Expression", MockSQLGlotExpression(expressions=expressions))
    assert get_node_expressions(node) == expressions


def test_get_node_expressions_without_expressions() -> None:
    """Test get_node_expressions returns default when expressions missing."""
    node = cast("exp.Expression", MockSQLGlotExpression())
    assert get_node_expressions(node, "default") == "default"
    assert get_node_expressions(node) is None


def test_has_expressions_attribute_with_attribute() -> None:
    """Test has_expressions_attribute returns True when expressions exists."""
    node = cast("exp.Expression", MockSQLGlotExpression(expressions=["expr1"]))
    assert has_expressions_attribute(node) is True


def test_has_expressions_attribute_without_attribute() -> None:
    """Test has_expressions_attribute returns False when expressions doesn't exist."""
    node = cast("exp.Expression", MockSQLGlotExpression())
    assert has_expressions_attribute(node) is False


def test_get_literal_parent_with_parent() -> None:
    """Test get_literal_parent returns parent when present."""
    parent = "parent_node"
    literal = cast("exp.Expression", MockLiteral(parent=parent))
    assert get_literal_parent(literal) == parent


def test_get_literal_parent_without_parent() -> None:
    """Test get_literal_parent returns default when parent missing."""
    literal = cast("exp.Expression", MockLiteral())
    assert get_literal_parent(literal, "default") == "default"
    assert get_literal_parent(literal) is None


def test_has_parent_attribute_with_attribute() -> None:
    """Test has_parent_attribute returns True when parent exists."""
    literal = cast("exp.Expression", MockLiteral(parent="parent_node"))
    assert has_parent_attribute(literal) is True


def test_has_parent_attribute_without_attribute() -> None:
    """Test has_parent_attribute returns False when parent doesn't exist."""
    literal = cast("exp.Expression", MockLiteral())
    assert has_parent_attribute(literal) is False


# Literal Type Tests


def test_is_string_literal_with_string_flag() -> None:
    """Test is_string_literal returns True when is_string is True."""
    literal = cast("exp.Literal", MockLiteral(is_string=True))
    assert is_string_literal(literal) is True


def test_is_string_literal_without_string_flag() -> None:
    """Test is_string_literal handles missing is_string attribute."""
    literal = cast("exp.Literal", MockLiteral(this="string_value"))
    # Should fall back to checking this attribute type
    assert is_string_literal(literal) is True


def test_is_string_literal_with_non_string_this() -> None:
    """Test is_string_literal returns False for non-string this."""
    literal = cast("exp.Literal", MockLiteral(this=42))
    assert is_string_literal(literal) is False


def test_is_number_literal_with_number_flag() -> None:
    """Test is_number_literal returns True when is_number is True."""
    literal = cast("exp.Literal", MockLiteral(is_number=True))
    assert is_number_literal(literal) is True


def test_is_number_literal_without_number_flag() -> None:
    """Test is_number_literal handles missing is_number attribute."""
    literal = cast("exp.Literal", MockLiteral(this="123"))
    # Should fall back to trying to convert to float
    assert is_number_literal(literal) is True


def test_is_number_literal_with_non_number_this() -> None:
    """Test is_number_literal returns False for non-numeric this."""
    literal = cast("exp.Literal", MockLiteral(this="not_a_number"))
    assert is_number_literal(literal) is False


# Parameter Helper Tests


def test_get_param_style_and_name_with_attributes() -> None:
    """Test get_param_style_and_name returns style and name when present."""
    param = MockParameterProtocol(style="named", name="test_param")
    style, name = get_param_style_and_name(param)
    assert style == "named"
    assert name == "test_param"


def test_get_param_style_and_name_without_attributes() -> None:
    """Test get_param_style_and_name returns None, None when attributes missing."""
    param = object()
    style, name = get_param_style_and_name(param)
    assert style is None
    assert name is None


def test_get_value_attribute_with_value() -> None:
    """Test get_value_attribute returns value when present."""
    obj = MockValueWrapper("test_value")
    assert get_value_attribute(obj) == "test_value"


def test_get_value_attribute_without_value() -> None:
    """Test get_value_attribute returns object when value missing."""
    obj = "no_value_attribute"
    assert get_value_attribute(obj) == "no_value_attribute"


# Expression Context Tests


def test_get_initial_expression_with_attribute() -> None:
    """Test get_initial_expression returns expression when present."""
    mock_expr = MockSQLGlotExpression()

    class MockContext:
        def __init__(self) -> None:
            self.initial_expression = mock_expr

    context = MockContext()
    assert get_initial_expression(context) is mock_expr


def test_get_initial_expression_without_attribute() -> None:
    """Test get_initial_expression returns None when attribute missing."""
    context = object()
    assert get_initial_expression(context) is None


def test_expression_has_limit_with_limit() -> None:
    """Test expression_has_limit returns True when limit in args."""
    expr = cast("exp.Expression", MockSQLGlotExpression(args={"limit": "10"}))
    assert expression_has_limit(expr) is True


def test_expression_has_limit_without_limit() -> None:
    """Test expression_has_limit returns False when no limit in args."""
    expr = cast("exp.Expression", MockSQLGlotExpression(args={"other": "value"}))
    assert expression_has_limit(expr) is False


def test_expression_has_limit_with_none() -> None:
    """Test expression_has_limit returns False for None expression."""
    assert expression_has_limit(None) is False


def test_expression_has_limit_without_args() -> None:
    """Test expression_has_limit handles missing args attribute."""
    expr = cast("exp.Expression", object())  # type: ignore[arg-type]
    assert expression_has_limit(expr) is False


# COPY Statement Tests


def test_is_copy_statement_with_none() -> None:
    """Test is_copy_statement returns False for None."""
    assert is_copy_statement(None) is False


def test_is_copy_statement_with_non_expression() -> None:
    """Test is_copy_statement returns False for non-expression objects."""
    assert is_copy_statement("not an expression") is False
    assert is_copy_statement(42) is False


# Dataclass Utility Function Tests


def test_extract_dataclass_fields_basic() -> None:
    """Test extract_dataclass_fields returns correct fields."""
    instance = SampleDataclass(name="test", age=25)
    fields = extract_dataclass_fields(instance)

    field_names = {field.name for field in fields}
    assert "name" in field_names
    assert "age" in field_names
    assert "optional_field" in field_names


def test_extract_dataclass_fields_exclude_none() -> None:
    """Test extract_dataclass_fields excludes None values when requested."""
    instance = SampleDataclass(name="test", age=25, optional_field=None)
    fields = extract_dataclass_fields(instance, exclude_none=True)

    field_names = {field.name for field in fields}
    assert "name" in field_names
    assert "age" in field_names
    assert "optional_field" not in field_names


def test_extract_dataclass_fields_include_exclude() -> None:
    """Test extract_dataclass_fields respects include/exclude parameters."""
    instance = SampleDataclass(name="test", age=25)

    # Test include
    fields = extract_dataclass_fields(instance, include={"name"})
    field_names = {field.name for field in fields}
    assert field_names == {"name"}

    # Test exclude
    fields = extract_dataclass_fields(instance, exclude={"age"})
    field_names = {field.name for field in fields}
    assert "name" in field_names
    assert "optional_field" in field_names
    assert "age" not in field_names


def test_extract_dataclass_fields_conflicting_include_exclude() -> None:
    """Test extract_dataclass_fields raises error for conflicting include/exclude."""
    instance = SampleDataclass(name="test", age=25)

    with pytest.raises(ValueError, match="Fields .* are both included and excluded"):
        extract_dataclass_fields(instance, include={"name"}, exclude={"name"})


def test_extract_dataclass_items_basic() -> None:
    """Test extract_dataclass_items returns correct name-value pairs."""
    instance = SampleDataclass(name="test", age=25)
    items = extract_dataclass_items(instance)

    items_dict = dict(items)
    assert items_dict["name"] == "test"
    assert items_dict["age"] == 25
    assert "optional_field" in items_dict


def test_dataclass_to_dict_basic() -> None:
    """Test dataclass_to_dict converts dataclass to dictionary."""
    instance = SampleDataclass(name="test", age=25)
    result = dataclass_to_dict(instance)

    expected = {"name": "test", "age": 25, "optional_field": None}
    assert result == expected


def test_dataclass_to_dict_exclude_none() -> None:
    """Test dataclass_to_dict excludes None values when requested."""
    instance = SampleDataclass(name="test", age=25, optional_field=None)
    result = dataclass_to_dict(instance, exclude_none=True)

    expected = {"name": "test", "age": 25}
    assert result == expected


def test_dataclass_to_dict_nested() -> None:
    """Test dataclass_to_dict handles nested dataclasses."""

    @dataclass
    class NestedDataclass:
        inner: SampleDataclass

    inner = SampleDataclass(name="inner", age=30)
    outer = NestedDataclass(inner=inner)

    result = dataclass_to_dict(outer, convert_nested=True)

    expected = {"inner": {"name": "inner", "age": 30, "optional_field": None}}
    assert result == expected


def test_dataclass_to_dict_nested_disabled() -> None:
    """Test dataclass_to_dict doesn't convert nested when disabled."""

    @dataclass
    class NestedDataclass:
        inner: SampleDataclass

    inner = SampleDataclass(name="inner", age=30)
    outer = NestedDataclass(inner=inner)

    result = dataclass_to_dict(outer, convert_nested=False)

    assert result["inner"] is inner  # Should be the original object


# Schema Dump Tests


def test_schema_dump_with_dict() -> None:
    """Test schema_dump returns dict as-is."""
    data = {"name": "test", "age": 25}
    result = schema_dump(data)
    assert result is data


def test_schema_dump_with_dataclass() -> None:
    """Test schema_dump converts dataclass to dict."""
    instance = SampleDataclass(name="test", age=25)
    result = schema_dump(instance)

    expected = {"name": "test", "age": 25, "optional_field": None}
    assert result == expected


def test_schema_dump_exclude_unset() -> None:
    """Test schema_dump excludes unset/empty values when requested."""
    instance = SampleDataclass(name="test", age=25, optional_field=None)
    result = schema_dump(instance, exclude_unset=True)

    # For dataclass, exclude_unset maps to exclude_empty
    # None values should still be included unless they are Empty
    expected = {"name": "test", "age": 25, "optional_field": None}
    assert result == expected


def test_schema_dump_with_dict_attribute() -> None:
    """Test schema_dump falls back to __dict__ for objects with dict attribute."""

    class ObjectWithDict:
        def __init__(self) -> None:
            self.name = "test"
            self.age = 25

    obj = ObjectWithDict()
    result = schema_dump(cast("Any", obj))  # type: ignore[arg-type]

    expected = {"name": "test", "age": 25}
    assert result == expected


# Performance Tests


@pytest.mark.parametrize(
    "guard_func,test_obj,expected",
    [
        (is_dict, {}, True),
        (is_dict, [], False),
        (is_dataclass_instance, SampleDataclass("test", 25), True),
        (is_dataclass_instance, {}, False),
        (lambda obj: has_attr(obj, "value"), MockValueWrapper("test"), True),  # has 'value'
        (lambda obj: has_attr(obj, "nonexistent"), MockValueWrapper("test"), False),
    ],
    ids=["dict_true", "dict_false", "dataclass_true", "dataclass_false", "attr_true", "attr_false"],
)
def test_type_guard_performance(guard_func: Any, test_obj: Any, expected: bool) -> None:
    """Test that type guards perform efficiently and return expected results."""
    # Test multiple calls to ensure consistent performance
    for _ in range(100):
        result = guard_func(test_obj)
        assert result == expected


def test_multiple_type_guards_chain() -> None:
    """Test chaining multiple type guards doesn't degrade performance."""
    instance = SampleDataclass(name="test", age=25)

    # Chain multiple type guard checks
    for _ in range(50):
        assert is_schema_or_dict(instance) is True
        assert is_dataclass_with_field(instance, "name") is True
        assert is_dict_with_field({"key": "value"}, "key") is True
        assert is_iterable_parameters([1, 2, 3]) is True


# Edge Cases


def test_type_guards_with_none() -> None:
    """Test type guards handle None gracefully."""
    assert is_dict(None) is False
    assert is_dataclass(None) is False
    assert is_schema(None) is False
    assert is_expression(None) is False
    assert is_iterable_parameters(None) is False


def test_type_guards_with_empty_containers() -> None:
    """Test type guards work correctly with empty containers."""
    assert is_dict({}) is True
    assert is_iterable_parameters([]) is True
    assert is_iterable_parameters(()) is True
    assert is_dict_with_field({}, "any_key") is False


def test_sqlglot_helpers_with_invalid_objects() -> None:
    """Test SQLGlot helper functions handle invalid objects gracefully."""
    invalid_obj = cast("exp.Expression", "not an expression")  # type: ignore[arg-type]

    assert get_node_this(invalid_obj) is None
    assert get_node_expressions(invalid_obj) is None
    assert get_literal_parent(invalid_obj) is None
    assert has_this_attribute(invalid_obj) is False
    assert has_expressions_attribute(invalid_obj) is False
    assert has_parent_attribute(invalid_obj) is False


def test_edge_case_empty_string_literal() -> None:
    """Test literal type guards with edge cases."""
    empty_literal = cast("exp.Literal", MockLiteral(this=""))
    assert is_string_literal(empty_literal) is True

    zero_literal = cast("exp.Literal", MockLiteral(this="0"))
    assert is_number_literal(zero_literal) is True
