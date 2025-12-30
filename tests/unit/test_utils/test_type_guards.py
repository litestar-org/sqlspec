"""Tests for sqlspec.utils.type_guards module.

Tests all protocol type guards, validation functions, edge cases, and performance.
Uses function-based pytest approach as per AGENTS.md requirements.
"""

from dataclasses import dataclass
from typing import Any, cast

import msgspec
import pytest
from sqlglot import exp
from typing_extensions import TypedDict

from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.serializers import (
    get_collection_serializer,
    get_serializer_metrics,
    reset_serializer_cache,
    schema_dump,
    serialize_collection,
)
from sqlspec.utils.type_guards import (
    dataclass_to_dict,
    expression_has_limit,
    extract_dataclass_fields,
    extract_dataclass_items,
    get_initial_expression,
    get_literal_parent,
    get_msgspec_rename_config,
    get_node_expressions,
    get_node_this,
    get_param_style_and_name,
    get_value_attribute,
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
    is_typed_dict,
    supports_arrow_results,
)

pytestmark = pytest.mark.xdist_group("utils")

_UNSET = object()


@dataclass
class SampleDataclass:
    """Sample dataclass for testing."""

    name: str
    age: int
    optional_field: "str | None" = None


class SampleTypedDict(TypedDict):
    """Sample TypedDict for testing."""

    name: str
    age: int
    optional_field: "str | None"


@dataclass
class _SerializerRecord:
    identifier: int
    name: str


class MockSQLGlotExpression:
    """Mock SQLGlot expression for testing type guard functions.

    This mock allows us to test cases where attributes don't exist,
    which is needed to test the AttributeError handling in type guards.
    """

    def __init__(
        self, this: Any = _UNSET, expressions: Any = _UNSET, parent: Any = _UNSET, args: "dict[str, Any] | None" = None
    ) -> None:
        # Only set attributes if they were explicitly provided
        if this is not _UNSET:
            self.this = this
        if expressions is not _UNSET:
            self.expressions = expressions
        if parent is not _UNSET:
            self.parent = parent

        # SQLGlot expressions always have an args dict
        self.args = args or {}

        # Set any additional attributes from args
        if args:
            for key, value in args.items():
                if key not in {"this", "expressions", "parent"}:
                    setattr(self, key, value)


class MockLiteral:
    """Mock literal for testing."""

    def __init__(
        self, this: "Any | None" = None, is_string: bool = False, is_number: bool = False, parent: "Any | None" = None
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

    def __init__(self, style: "str | None" = None, name: "str | None" = None) -> None:
        if style is not None:
            self.style = style
        if name is not None:
            self.name = name


class MockValueWrapper:
    """Mock wrapper with value attribute."""

    def __init__(self, value: Any) -> None:
        self.value = value


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


def test_is_pydantic_model_when_not_installed() -> None:
    """Test is_pydantic_model returns False when pydantic not available."""

    assert is_pydantic_model("not a model") is False
    assert is_pydantic_model({}) is False


def test_is_pydantic_model_with_field_when_not_installed() -> None:
    """Test is_pydantic_model_with_field returns False when pydantic not available."""
    assert is_pydantic_model_with_field("not a model", "field") is False


def test_is_pydantic_model_without_field_when_not_installed() -> None:
    """Test is_pydantic_model_without_field returns False when pydantic not available."""
    assert is_pydantic_model_without_field("not a model", "field") is False


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

    assert is_schema_with_field(instance, "name") is False
    assert is_schema_with_field(instance, "nonexistent") is False


def test_is_schema_without_field_with_dataclass() -> None:
    """Test is_schema_without_field works with dataclass fields."""
    instance = SampleDataclass(name="test", age=25)

    assert is_schema_without_field(instance, "nonexistent") is True
    assert is_schema_without_field(instance, "name") is True


def test_is_schema_or_dict_with_field_combined() -> None:
    """Test is_schema_or_dict_with_field works with both schemas and dicts."""
    instance = SampleDataclass(name="test", age=25)
    data = {"name": "test", "age": 25}

    assert is_schema_or_dict_with_field(instance, "name") is False
    assert is_schema_or_dict_with_field(data, "name") is True
    assert is_schema_or_dict_with_field(instance, "nonexistent") is False
    assert is_schema_or_dict_with_field(data, "nonexistent") is False


def test_is_schema_or_dict_without_field_combined() -> None:
    """Test is_schema_or_dict_without_field works with both schemas and dicts."""
    instance = SampleDataclass(name="test", age=25)
    data = {"name": "test", "age": 25}

    assert is_schema_or_dict_without_field(instance, "nonexistent") is True
    assert is_schema_or_dict_without_field(data, "nonexistent") is True
    assert is_schema_or_dict_without_field(instance, "name") is True
    assert is_schema_or_dict_without_field(data, "name") is False


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


def test_is_dto_data_when_litestar_not_installed() -> None:
    """Test is_dto_data returns False when litestar not available."""
    assert is_dto_data("not dto data") is False
    assert is_dto_data({}) is False


def test_is_expression_with_mock() -> None:
    """Test is_expression with mock SQLGlot expressions."""
    mock_expr = MockSQLGlotExpression()

    result = is_expression(mock_expr)
    assert isinstance(result, bool)


def test_is_expression_with_non_expression() -> None:
    """Test is_expression returns False for non-expression objects."""
    assert is_expression("not an expression") is False
    assert is_expression(42) is False
    assert is_expression({}) is False


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


def test_is_string_literal_with_string_flag() -> None:
    """Test is_string_literal returns True when is_string is True."""
    literal = cast("exp.Literal", MockLiteral(is_string=True))
    assert is_string_literal(literal) is True


def test_is_string_literal_without_string_flag() -> None:
    """Test is_string_literal handles missing is_string attribute."""
    literal = cast("exp.Literal", MockLiteral(this="string_value"))

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

    assert is_number_literal(literal) is True


def test_is_number_literal_with_non_number_this() -> None:
    """Test is_number_literal returns False for non-numeric this."""
    literal = cast("exp.Literal", MockLiteral(this="not_a_number"))
    assert is_number_literal(literal) is False


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


def test_get_initial_expression_with_attribute() -> None:
    """Test get_initial_expression returns expression when present."""
    mock_expr = MockSQLGlotExpression()

    class MockContext:
        def __init__(self) -> None:
            self.initial_expression = mock_expr

    context = MockContext()
    assert get_initial_expression(context) is mock_expr  # type: ignore[comparison-overlap]


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
    expr = cast("exp.Expression", object())
    assert expression_has_limit(expr) is False


def test_is_copy_statement_with_none() -> None:
    """Test is_copy_statement returns False for None."""
    assert is_copy_statement(None) is False


def test_is_copy_statement_with_non_expression() -> None:
    """Test is_copy_statement returns False for non-expression objects."""
    assert is_copy_statement("not an expression") is False
    assert is_copy_statement(42) is False


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

    fields = extract_dataclass_fields(instance, include={"name"})
    field_names = {field.name for field in fields}
    assert field_names == {"name"}

    fields = extract_dataclass_fields(instance, exclude={"age"})
    field_names = {field.name for field in fields}
    assert "name" in field_names
    assert "optional_field" in field_names
    assert "age" not in field_names


def test_extract_dataclass_fields_conflicting_include_exclude() -> None:
    """Test extract_dataclass_fields raises error for conflicting include/exclude."""
    instance = SampleDataclass(name="test", age=25)

    with pytest.raises(ValueError, match="Fields .* are both included and excluded"):  # noqa: RUF043
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

    assert result["inner"] is inner


def test_schema_dump_with_dict() -> None:
    """Test schema_dump returns dict as-is."""
    data = {"name": "test", "age": 25}
    result = schema_dump(data)
    assert result is data


def test_schema_dump_with_primitives() -> None:
    """Test schema_dump returns primitive payload unchanged."""
    payload = "primary"
    result = schema_dump(payload)
    assert result == payload  # type: ignore[comparison-overlap]


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

    expected = {"name": "test", "age": 25, "optional_field": None}
    assert result == expected


def test_schema_dump_with_dict_attribute() -> None:
    """Test schema_dump falls back to __dict__ for objects with dict attribute."""

    class ObjectWithDict:
        def __init__(self) -> None:
            self.name = "test"
            self.age = 25

    obj = ObjectWithDict()
    result = schema_dump(cast("Any", obj))

    expected = {"name": "test", "age": 25}
    assert result == expected


def test_serializer_pipeline_reuses_entry() -> None:
    reset_serializer_cache()
    metrics = get_serializer_metrics()
    assert metrics["size"] == 0

    sample = _SerializerRecord(identifier=1, name="first")
    pipeline = get_collection_serializer(sample)
    metrics = get_serializer_metrics()
    assert metrics["size"] == 1

    same_pipeline = get_collection_serializer(_SerializerRecord(identifier=2, name="second"))
    assert pipeline is same_pipeline


def test_serializer_metrics_track_hits_and_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_serializer_cache()
    monkeypatch.setenv("SQLSPEC_DEBUG_PIPELINE_CACHE", "1")

    sample = _SerializerRecord(identifier=1, name="instrumented")
    get_collection_serializer(sample)
    metrics = get_serializer_metrics()
    assert metrics["misses"] == 1

    get_collection_serializer(sample)
    metrics = get_serializer_metrics()
    assert metrics["hits"] == 1


def test_serialize_collection_mixed_models() -> None:
    items = [_SerializerRecord(identifier=1, name="alpha"), {"identifier": 2, "name": "beta"}]
    serialized = serialize_collection(items)
    assert serialized == [{"identifier": 1, "name": "alpha"}, {"identifier": 2, "name": "beta"}]


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
def test_serializer_pipeline_arrow_conversion() -> None:
    sample = _SerializerRecord(identifier=1, name="alpha")
    pipeline = get_collection_serializer(sample)
    table = pipeline.to_arrow([sample, _SerializerRecord(identifier=2, name="beta")])
    assert table.num_rows == 2
    assert table.column(0).to_pylist() == [1, 2]


@pytest.mark.parametrize(
    "guard_func,test_obj,expected",
    [
        (is_dict, {}, True),
        (is_dict, [], False),
        (is_dataclass_instance, SampleDataclass("test", 25), True),
        (is_dataclass_instance, {}, False),
    ],
    ids=["dict_true", "dict_false", "dataclass_true", "dataclass_false"],
)
def test_type_guard_performance(guard_func: Any, test_obj: Any, expected: bool) -> None:
    """Test that type guards perform efficiently and return expected results."""

    for _ in range(100):
        result = guard_func(test_obj)
        assert result == expected


def test_multiple_type_guards_chain() -> None:
    """Test chaining multiple type guards doesn't degrade performance."""
    instance = SampleDataclass(name="test", age=25)

    for _ in range(50):
        assert is_schema_or_dict(instance) is True
        assert is_dataclass_with_field(instance, "name") is True
        assert is_dict_with_field({"key": "value"}, "key") is True
        assert is_iterable_parameters([1, 2, 3]) is True


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
    invalid_obj = cast("exp.Expression", "not an expression")

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


class MockMsgspecStructWithCamelRename(msgspec.Struct, rename="camel"):  # type: ignore[misc]
    """Mock msgspec struct with camel rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithKebabRename(msgspec.Struct, rename="kebab"):  # type: ignore[misc]
    """Mock msgspec struct with kebab rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithPascalRename(msgspec.Struct, rename="pascal"):  # type: ignore[misc]
    """Mock msgspec struct with pascal rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithoutRename(msgspec.Struct):
    """Mock msgspec struct without rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithoutConfig(msgspec.Struct):
    """Mock msgspec struct without __struct_config__ attribute."""

    test_name: str = "test"


def test_get_msgspec_rename_config_with_camel_rename() -> None:
    """Test get_msgspec_rename_config returns 'camel' for camel rename config."""
    schema_type = MockMsgspecStructWithCamelRename
    result = get_msgspec_rename_config(schema_type)
    assert result == "camel"


def test_get_msgspec_rename_config_with_kebab_rename() -> None:
    """Test get_msgspec_rename_config returns 'kebab' for kebab rename config."""
    schema_type = MockMsgspecStructWithKebabRename
    result = get_msgspec_rename_config(schema_type)
    assert result == "kebab"


def test_get_msgspec_rename_config_with_pascal_rename() -> None:
    """Test get_msgspec_rename_config returns 'pascal' for pascal rename config."""
    schema_type = MockMsgspecStructWithPascalRename
    result = get_msgspec_rename_config(schema_type)
    assert result == "pascal"


def test_is_typed_dict_with_typeddict_class() -> None:
    """Test is_typed_dict returns True for TypedDict classes."""
    assert is_typed_dict(SampleTypedDict) is True


def test_is_typed_dict_with_typeddict_instance() -> None:
    """Test is_typed_dict returns False for TypedDict instances (they are dicts)."""
    sample_data: SampleTypedDict = {"name": "test", "age": 25, "optional_field": "value"}
    assert is_typed_dict(sample_data) is False


def test_is_typed_dict_with_non_typeddict() -> None:
    """Test is_typed_dict returns False for non-TypedDict types."""
    assert is_typed_dict(dict) is False
    assert is_typed_dict(SampleDataclass) is False
    assert is_typed_dict(str) is False
    assert is_typed_dict(42) is False
    assert is_typed_dict({}) is False


def test_is_typed_dict_with_regular_dict() -> None:
    """Test is_typed_dict returns False for regular dict instances."""
    assert is_typed_dict({"key": "value"}) is False


def test_get_msgspec_rename_config_without_rename() -> None:
    """Test get_msgspec_rename_config returns None when no rename config."""
    schema_type = MockMsgspecStructWithoutRename
    result = get_msgspec_rename_config(schema_type)
    assert result is None


def test_get_msgspec_rename_config_without_struct_config() -> None:
    """Test get_msgspec_rename_config returns None when no __struct_config__."""
    schema_type = MockMsgspecStructWithoutConfig
    result = get_msgspec_rename_config(schema_type)
    assert result is None


def test_get_msgspec_rename_config_with_non_msgspec_class() -> None:
    """Test get_msgspec_rename_config returns None for non-msgspec classes."""
    result = get_msgspec_rename_config(SampleDataclass)
    assert result is None

    result = get_msgspec_rename_config(dict)
    assert result is None

    result = get_msgspec_rename_config(list)
    assert result is None


def test_get_msgspec_rename_config_with_invalid_config_structure() -> None:
    """Test get_msgspec_rename_config handles invalid config structures."""

    class InvalidConfigStruct:
        __struct_config__ = "not a dict"

    result = get_msgspec_rename_config(InvalidConfigStruct)
    assert result is None

    class InvalidConfigStruct2:
        __struct_config__ = None  # type: ignore[var-annotated]

    result = get_msgspec_rename_config(InvalidConfigStruct2)
    assert result is None


def test_get_msgspec_rename_config_performance() -> None:
    """Test get_msgspec_rename_config performs efficiently."""
    schema_type = MockMsgspecStructWithCamelRename

    # Test repeated calls to ensure no performance degradation
    for _ in range(100):
        result = get_msgspec_rename_config(schema_type)
        assert result == "camel"


def test_supports_arrow_results_with_protocol_implementation() -> None:
    """Test supports_arrow_results with object implementing SupportsArrowResults."""

    class MockDriverWithArrow:
        def select_to_arrow(
            self,
            statement,
            /,
            *parameters,
            statement_config=None,
            return_format="table",
            native_only=False,
            batch_size=None,
            arrow_schema=None,
            **kwargs,
        ):
            pass

    driver = MockDriverWithArrow()
    assert supports_arrow_results(driver) is True


def test_supports_arrow_results_without_protocol_implementation() -> None:
    """Test supports_arrow_results with object not implementing protocol."""

    class MockDriverWithoutArrow:
        def execute(self, sql):
            pass

    driver = MockDriverWithoutArrow()
    assert supports_arrow_results(driver) is False


def test_supports_arrow_results_with_none() -> None:
    """Test supports_arrow_results with None."""

    assert supports_arrow_results(None) is False


def test_supports_arrow_results_with_primitive_types() -> None:
    """Test supports_arrow_results with primitive types."""

    assert supports_arrow_results("string") is False
    assert supports_arrow_results(42) is False
    assert supports_arrow_results([1, 2, 3]) is False
    assert supports_arrow_results({"key": "value"}) is False
