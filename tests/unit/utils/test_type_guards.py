"""Tests for sqlspec.utils.type_guards module.

Tests all protocol type guards, validation functions, edge cases, and performance.
Uses function-based pytest approach as per AGENTS.md requirements.
"""

import typing
from collections import UserDict
from collections.abc import Mapping
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
    is_async_readable,
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
    is_readable,
    is_schema,
    is_schema_or_dict,
    is_schema_or_dict_with_field,
    is_schema_or_dict_without_field,
    is_schema_with_field,
    is_schema_without_field,
    is_string_literal,
    is_typed_dict,
    resolve_row_format,
    supports_arrow_results,
)

_UNSET = object()


@pytest.mark.parametrize("default", ["dict", "tuple", "record"])
@pytest.mark.parametrize("rows", [None, [], [(1,)]], ids=["none", "empty", "tuple"])
def test_resolve_row_format_preserves_default(rows: Any, default: Any) -> None:
    assert resolve_row_format(rows, default=default) == default


@pytest.mark.parametrize("default", ["dict", "tuple", "record"])
@pytest.mark.parametrize("row, expected", [({"x": 1}, "dict"), (UserDict({"x": 1}), "record")])
def test_resolve_row_format_detects_mappings(row: Any, expected: str, default: Any) -> None:
    assert resolve_row_format([row], default=default) == expected


def test_resolve_row_format_preserves_tuple_subclass_detection() -> None:
    class TupleRecord(tuple[int, ...]):
        __slots__ = ()

        def keys(self) -> tuple[str, ...]:
            return ("x",)

    assert resolve_row_format([TupleRecord((1,))], default="dict") == "record"


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
        if this is not _UNSET:
            self.this = this
        if expressions is not _UNSET:
            self.expressions = expressions
        if parent is not _UNSET:
            self.parent = parent
        self.args = args or {}
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


class SyncReadable:
    def read(self) -> str:
        return "sync"


class AsyncReadable:
    async def read(self) -> str:
        return "async"


@pytest.mark.parametrize(
    ("target", "guard", "expected"),
    [
        pytest.param(SyncReadable(), is_readable, True, id="sync_readable_with_is_readable"),
        pytest.param(SyncReadable(), is_async_readable, False, id="sync_readable_with_is_async_readable"),
        pytest.param(AsyncReadable(), is_async_readable, True, id="async_readable_with_is_async_readable"),
    ],
)
def test_readable_guards(target: Any, guard: Any, expected: bool) -> None:
    """Validate readable and async readable protocol guards."""
    assert guard(target) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(SampleDataclass(name="test", age=25), True, id="instance"),
        pytest.param(SampleDataclass, False, id="class"),
        pytest.param("not a dataclass", False, id="string"),
        pytest.param(42, False, id="integer"),
        pytest.param({}, False, id="dict"),
    ],
)
def test_is_dataclass_instance(value: Any, expected: bool) -> None:
    """Validate is_dataclass_instance returns True for dataclass instances only."""
    assert is_dataclass_instance(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(SampleDataclass, True, id="class"),
        pytest.param(SampleDataclass(name="test", age=25), True, id="instance"),
        pytest.param("not a dataclass", False, id="string"),
        pytest.param(42, False, id="integer"),
        pytest.param({}, False, id="dict"),
    ],
)
def test_is_dataclass(value: Any, expected: bool) -> None:
    """Validate is_dataclass returns True for dataclass classes and instances."""
    assert is_dataclass(value) is expected


@pytest.mark.parametrize(
    ("target", "field_name", "expected"),
    [
        pytest.param(SampleDataclass(name="test", age=25), "name", True, id="existing_field_name"),
        pytest.param(SampleDataclass(name="test", age=25), "age", True, id="existing_field_age"),
        pytest.param(SampleDataclass(name="test", age=25), "nonexistent", False, id="missing_field"),
        pytest.param("not a dataclass", "any_field", False, id="non_dataclass"),
    ],
)
def test_is_dataclass_with_field(target: Any, field_name: str, expected: bool) -> None:
    """Validate is_dataclass_with_field returns True when field exists on dataclass."""
    assert is_dataclass_with_field(target, field_name) is expected


@pytest.mark.parametrize(
    ("target", "field_name", "expected"),
    [
        pytest.param(SampleDataclass(name="test", age=25), "nonexistent", True, id="missing_field"),
        pytest.param(SampleDataclass(name="test", age=25), "name", False, id="existing_field"),
        pytest.param("not a dataclass", "any_field", False, id="non_dataclass"),
    ],
)
def test_is_dataclass_without_field(target: Any, field_name: str, expected: bool) -> None:
    """Validate is_dataclass_without_field returns True when field is absent from dataclass."""
    assert is_dataclass_without_field(target, field_name) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param({}, True, id="empty_dict"),
        pytest.param({"key": "value"}, True, id="populated_dict"),
        pytest.param("not a dict", False, id="string"),
        pytest.param([], False, id="list"),
        pytest.param(42, False, id="integer"),
    ],
)
def test_is_dict(value: Any, expected: bool) -> None:
    """Validate is_dict returns True for dictionaries only."""
    assert is_dict(value) is expected


@pytest.mark.parametrize(
    ("target", "field_name", "expected"),
    [
        pytest.param({"name": "test", "age": 25}, "name", True, id="existing_key_name"),
        pytest.param({"name": "test", "age": 25}, "age", True, id="existing_key_age"),
        pytest.param({"name": "test"}, "nonexistent", False, id="missing_key"),
        pytest.param("not a dict", "any_key", False, id="non_dict"),
    ],
)
def test_is_dict_with_field(target: Any, field_name: str, expected: bool) -> None:
    """Validate is_dict_with_field returns True when key exists in dictionary."""
    assert is_dict_with_field(target, field_name) is expected


@pytest.mark.parametrize(
    ("target", "field_name", "expected"),
    [
        pytest.param({"name": "test"}, "nonexistent", True, id="missing_key"),
        pytest.param({"name": "test", "age": 25}, "name", False, id="existing_key"),
        pytest.param("not a dict", "any_key", False, id="non_dict"),
    ],
)
def test_is_dict_without_field(target: Any, field_name: str, expected: bool) -> None:
    """Validate is_dict_without_field returns True when key is absent from dictionary."""
    assert is_dict_without_field(target, field_name) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param({}, True, id="empty_dict"),
        pytest.param({"col1": "value1", "col2": "value2"}, True, id="populated_dict"),
        pytest.param("not a dict", False, id="string"),
        pytest.param([], False, id="list"),
        pytest.param(42, False, id="integer"),
    ],
)
def test_is_dict_row(value: Any, expected: bool) -> None:
    """Validate is_dict_row returns True for dictionaries representing row data."""
    assert is_dict_row(value) is expected


@pytest.mark.parametrize(
    ("guard", "args"),
    [
        pytest.param(is_pydantic_model, ("not a model",), id="model_string"),
        pytest.param(is_pydantic_model, ({},), id="model_dict"),
        pytest.param(is_pydantic_model_with_field, ("not a model", "field"), id="with_field_string"),
        pytest.param(is_pydantic_model_without_field, ("not a model", "field"), id="without_field_string"),
    ],
)
def test_pydantic_model_fallback_guards(guard: Any, args: tuple[Any, ...]) -> None:
    """Validate pydantic guard behavior when handling non-pydantic inputs."""
    assert guard(*args) is False


@pytest.mark.parametrize(
    ("guard", "args"),
    [
        pytest.param(is_msgspec_struct, ("not a struct",), id="struct_string"),
        pytest.param(is_msgspec_struct, ({},), id="struct_dict"),
        pytest.param(is_msgspec_struct_with_field, ("not a struct", "field"), id="with_field_string"),
        pytest.param(is_msgspec_struct_without_field, ("not a struct", "field"), id="without_field_string"),
    ],
)
def test_msgspec_struct_fallback_guards(guard: Any, args: tuple[Any, ...]) -> None:
    """Validate msgspec guard behavior when handling non-struct inputs."""
    assert guard(*args) is False


@pytest.mark.parametrize(
    ("guard", "args"),
    [
        pytest.param(is_attrs_instance, ("not attrs",), id="instance_string"),
        pytest.param(is_attrs_instance, ({},), id="instance_dict"),
        pytest.param(is_attrs_schema, ("not attrs",), id="schema_string"),
        pytest.param(is_attrs_schema, (dict,), id="schema_dict_type"),
        pytest.param(is_attrs_instance_with_field, ("not attrs", "field"), id="with_field_string"),
        pytest.param(is_attrs_instance_without_field, ("not attrs", "field"), id="without_field_string"),
    ],
)
def test_attrs_fallback_guards(guard: Any, args: tuple[Any, ...]) -> None:
    """Validate attrs guard behavior when handling non-attrs inputs."""
    assert guard(*args) is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(SampleDataclass(name="test", age=25), True, id="dataclass_instance"),
        pytest.param("not a schema", False, id="string"),
        pytest.param(42, False, id="integer"),
        pytest.param([], False, id="list"),
    ],
)
def test_is_schema(value: Any, expected: bool) -> None:
    """Validate is_schema returns True for schema objects and False otherwise."""
    assert is_schema(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(SampleDataclass(name="test", age=25), True, id="schema_dataclass"),
        pytest.param({"key": "value"}, True, id="dict"),
        pytest.param("not schema or dict", False, id="string"),
        pytest.param(42, False, id="integer"),
    ],
)
def test_is_schema_or_dict(value: Any, expected: bool) -> None:
    """Validate is_schema_or_dict returns True for schemas and dicts."""
    assert is_schema_or_dict(value) is expected


@pytest.mark.parametrize(
    ("field_name", "expected_with", "expected_without"),
    [pytest.param("name", False, True, id="existing_name"), pytest.param("nonexistent", False, True, id="nonexistent")],
)
def test_is_schema_field_guards_with_dataclass(field_name: str, expected_with: bool, expected_without: bool) -> None:
    """Validate schema field guards on dataclass instances."""
    instance = SampleDataclass(name="test", age=25)
    assert is_schema_with_field(instance, field_name) is expected_with
    assert is_schema_without_field(instance, field_name) is expected_without


@pytest.mark.parametrize(
    ("target", "field_name", "expected_with", "expected_without"),
    [
        pytest.param(SampleDataclass(name="test", age=25), "name", False, True, id="dataclass_existing_field"),
        pytest.param({"name": "test", "age": 25}, "name", True, False, id="dict_existing_field"),
        pytest.param(SampleDataclass(name="test", age=25), "nonexistent", False, True, id="dataclass_missing_field"),
        pytest.param({"name": "test", "age": 25}, "nonexistent", False, True, id="dict_missing_field"),
    ],
)
def test_is_schema_or_dict_field_guards(
    target: Any, field_name: str, expected_with: bool, expected_without: bool
) -> None:
    """Validate schema or dict field presence guards across schemas and dicts."""
    assert is_schema_or_dict_with_field(target, field_name) is expected_with
    assert is_schema_or_dict_without_field(target, field_name) is expected_without


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param([1, 2, 3], True, id="populated_list"),
        pytest.param([], True, id="empty_list"),
        pytest.param((1, 2, 3), True, id="populated_tuple"),
        pytest.param((), True, id="empty_tuple"),
        pytest.param("string", False, id="populated_string"),
        pytest.param("", False, id="empty_string"),
        pytest.param(b"bytes", False, id="populated_bytes"),
        pytest.param(b"", False, id="empty_bytes"),
        pytest.param({"key": "value"}, False, id="populated_dict"),
        pytest.param({}, False, id="empty_dict"),
        pytest.param(42, False, id="integer"),
        pytest.param(None, False, id="none"),
    ],
)
def test_is_iterable_parameters(value: Any, expected: bool) -> None:
    """Validate is_iterable_parameters returns True for lists and tuples only."""
    assert is_iterable_parameters(value) is expected


@pytest.mark.parametrize("value", [pytest.param("not dto data", id="string"), pytest.param({}, id="dict")])
def test_is_dto_data_when_litestar_not_installed(value: Any) -> None:
    """Validate is_dto_data returns False for non-DTO data."""
    assert is_dto_data(value) is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(exp.var("x"), True, id="sqlglot_expression"),
        pytest.param(cast("exp.Expr", MockSQLGlotExpression()), False, id="mock_expression"),
        pytest.param("not an expression", False, id="string"),
        pytest.param(42, False, id="integer"),
        pytest.param({}, False, id="dict"),
    ],
)
def test_is_expression(value: Any, expected: bool) -> None:
    """Validate is_expression returns True for SQLGlot expressions only."""
    assert is_expression(value) is expected


@pytest.mark.parametrize(
    ("node", "expected_this", "expected_has"),
    [
        pytest.param(cast("exp.Expr", MockSQLGlotExpression(this="test_value")), "test_value", True, id="with_this"),
        pytest.param(cast("exp.Expr", MockSQLGlotExpression()), None, False, id="without_this"),
    ],
)
def test_node_this_helpers(node: Any, expected_this: Any, expected_has: bool) -> None:
    """Validate get_node_this and has_this_attribute behavior."""
    assert get_node_this(node) == expected_this
    if expected_this is None:
        assert get_node_this(node, "default") == "default"
    assert has_this_attribute(node) is expected_has


@pytest.mark.parametrize(
    ("node", "expected_expressions", "expected_has"),
    [
        pytest.param(
            cast("exp.Expression", MockSQLGlotExpression(expressions=["expr1", "expr2"])),
            ["expr1", "expr2"],
            True,
            id="with_expressions",
        ),
        pytest.param(cast("exp.Expression", MockSQLGlotExpression()), None, False, id="without_expressions"),
    ],
)
def test_node_expressions_helpers(node: Any, expected_expressions: Any, expected_has: bool) -> None:
    """Validate get_node_expressions and has_expressions_attribute behavior."""
    assert get_node_expressions(node) == expected_expressions
    if expected_expressions is None:
        assert get_node_expressions(node, "default") == "default"
    assert has_expressions_attribute(node) is expected_has


@pytest.mark.parametrize(
    ("literal", "expected_parent", "expected_has"),
    [
        pytest.param(cast("exp.Expression", MockLiteral(parent="parent_node")), "parent_node", True, id="with_parent"),
        pytest.param(cast("exp.Expression", MockLiteral()), None, False, id="without_parent"),
    ],
)
def test_literal_parent_helpers(literal: Any, expected_parent: Any, expected_has: bool) -> None:
    """Validate get_literal_parent and has_parent_attribute behavior."""
    assert get_literal_parent(literal) == expected_parent
    if expected_parent is None:
        assert get_literal_parent(literal, "default") == "default"
    assert has_parent_attribute(literal) is expected_has


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        pytest.param(cast("exp.Literal", MockLiteral(is_string=True)), True, id="string_flag"),
        pytest.param(cast("exp.Literal", MockLiteral(this="string_value")), True, id="string_this"),
        pytest.param(cast("exp.Literal", MockLiteral(this="")), True, id="empty_string_this"),
        pytest.param(cast("exp.Literal", MockLiteral(this=42)), False, id="non_string_this"),
    ],
)
def test_is_string_literal(literal: Any, expected: bool) -> None:
    """Validate is_string_literal with various mock literal configurations."""
    assert is_string_literal(literal) is expected


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        pytest.param(cast("exp.Literal", MockLiteral(is_number=True)), True, id="number_flag"),
        pytest.param(cast("exp.Literal", MockLiteral(this="123")), True, id="numeric_string_this"),
        pytest.param(cast("exp.Literal", MockLiteral(this="0")), True, id="zero_string_this"),
        pytest.param(cast("exp.Literal", MockLiteral(this="not_a_number")), False, id="non_numeric_this"),
    ],
)
def test_is_number_literal(literal: Any, expected: bool) -> None:
    """Validate is_number_literal with various mock literal configurations."""
    assert is_number_literal(literal) is expected


@pytest.mark.parametrize(
    ("param", "expected_style", "expected_name"),
    [
        pytest.param(
            MockParameterProtocol(style="named", name="test_param"), "named", "test_param", id="with_attributes"
        ),
        pytest.param(object(), None, None, id="without_attributes"),
    ],
)
def test_get_param_style_and_name(param: Any, expected_style: "str | None", expected_name: "str | None") -> None:
    """Validate get_param_style_and_name with and without protocol attributes."""
    style, name = get_param_style_and_name(param)
    assert style == expected_style
    assert name == expected_name


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        pytest.param(MockValueWrapper("test_value"), "test_value", id="with_value"),
        pytest.param("no_value_attribute", "no_value_attribute", id="without_value"),
    ],
)
def test_get_value_attribute(target: Any, expected: Any) -> None:
    """Validate get_value_attribute returns wrapped value or original object."""
    assert get_value_attribute(target) == expected


@pytest.mark.parametrize(
    ("has_initial", "expected_match"),
    [
        pytest.param(True, True, id="with_initial_expression"),
        pytest.param(False, False, id="without_initial_expression"),
    ],
)
def test_get_initial_expression(has_initial: bool, expected_match: bool) -> None:
    """Validate get_initial_expression extracts initial_expression if present."""
    mock_expr = MockSQLGlotExpression()

    class MockContext:
        def __init__(self) -> None:
            if has_initial:
                self.initial_expression = mock_expr

    context = MockContext()
    result = get_initial_expression(context)
    if expected_match:
        assert cast("object", result) is cast("object", mock_expr)
    else:
        assert result is None


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        pytest.param(cast("exp.Expression", MockSQLGlotExpression(args={"limit": "10"})), True, id="with_limit"),
        pytest.param(cast("exp.Expression", MockSQLGlotExpression(args={"other": "value"})), False, id="without_limit"),
        pytest.param(None, False, id="none"),
        pytest.param(cast("exp.Expression", object()), False, id="without_args"),
    ],
)
def test_expression_has_limit(expr: Any, expected: bool) -> None:
    """Validate expression_has_limit across various expression shapes."""
    assert expression_has_limit(expr) is expected


@pytest.mark.parametrize(
    "value",
    [pytest.param(None, id="none"), pytest.param("not an expression", id="string"), pytest.param(42, id="integer")],
)
def test_is_copy_statement_non_expression(value: Any) -> None:
    """Validate is_copy_statement returns False for non-expression objects."""
    assert is_copy_statement(value) is False


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
    with pytest.raises(ValueError, match=r"Fields .* are both included and excluded"):
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
    assert result == payload


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
    from sqlspec.utils.serializers import _schema as schema_module

    reset_serializer_cache()
    monkeypatch.setattr(schema_module, "_METRICS_ENABLED", True)
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


@pytest.mark.parametrize(
    "guard_func",
    [
        pytest.param(is_dict, id="is_dict"),
        pytest.param(is_dataclass, id="is_dataclass"),
        pytest.param(is_schema, id="is_schema"),
        pytest.param(is_expression, id="is_expression"),
        pytest.param(is_iterable_parameters, id="is_iterable_parameters"),
    ],
)
def test_type_guards_with_none(guard_func: Any) -> None:
    """Validate that type guards handle None gracefully by returning False."""
    assert guard_func(None) is False


def test_type_guards_with_empty_containers() -> None:
    """Test type guards work correctly with empty containers."""
    assert is_dict({}) is True
    assert is_iterable_parameters([]) is True
    assert is_iterable_parameters(()) is True
    assert is_dict_with_field({}, "any_key") is False


def test_sqlglot_helpers_with_invalid_objects() -> None:
    """Test SQLGlot helper functions handle invalid objects gracefully."""
    invalid_expr = cast("exp.Expr", "not an expression")
    invalid_expression = cast("exp.Expression", "not an expression")
    assert get_node_this(invalid_expr) is None
    assert get_node_expressions(invalid_expression) is None
    assert get_literal_parent(invalid_expression) is None
    assert has_this_attribute(invalid_expr) is False
    assert has_expressions_attribute(invalid_expression) is False
    assert has_parent_attribute(invalid_expression) is False


def test_edge_case_empty_string_literal() -> None:
    """Test literal type guards with edge cases."""
    empty_literal = cast("exp.Literal", MockLiteral(this=""))
    assert is_string_literal(empty_literal) is True
    zero_literal = cast("exp.Literal", MockLiteral(this="0"))
    assert is_number_literal(zero_literal) is True


class MockMsgspecStructWithCamelRename(msgspec.Struct, rename="camel"):
    """Mock msgspec struct with camel rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithKebabRename(msgspec.Struct, rename="kebab"):
    """Mock msgspec struct with kebab rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithPascalRename(msgspec.Struct, rename="pascal"):
    """Mock msgspec struct with pascal rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithoutRename(msgspec.Struct):
    """Mock msgspec struct without rename configuration."""

    test_name: str = "test"


class MockMsgspecStructWithoutConfig(msgspec.Struct):
    """Mock msgspec struct without __struct_config__ attribute."""

    test_name: str = "test"


class _InvalidConfigStructString:
    __struct_config__ = "not a dict"


class _InvalidConfigStructNone:
    __struct_config__ = None


@pytest.mark.parametrize(
    ("schema_type", "expected"),
    [
        pytest.param(MockMsgspecStructWithCamelRename, "camel", id="camel"),
        pytest.param(MockMsgspecStructWithKebabRename, "kebab", id="kebab"),
        pytest.param(MockMsgspecStructWithPascalRename, "pascal", id="pascal"),
        pytest.param(MockMsgspecStructWithoutRename, None, id="without_rename"),
        pytest.param(MockMsgspecStructWithoutConfig, None, id="without_struct_config"),
        pytest.param(SampleDataclass, None, id="dataclass"),
        pytest.param(dict, None, id="dict"),
        pytest.param(list, None, id="list"),
        pytest.param(_InvalidConfigStructString, None, id="invalid_config_string"),
        pytest.param(_InvalidConfigStructNone, None, id="invalid_config_none"),
    ],
)
def test_get_msgspec_rename_config(schema_type: Any, expected: "str | None") -> None:
    """Validate get_msgspec_rename_config handles configured and unconfigured types."""
    assert get_msgspec_rename_config(schema_type) == expected


def test_get_msgspec_rename_config_caches_per_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated rename-config lookups for one type should hit the cache."""
    original_fields = msgspec.structs.fields
    call_count = 0

    class CachedMsgspecStruct(msgspec.Struct, rename="camel"):
        test_name: str = "test"

    def count_fields(schema_type: Any) -> Any:
        nonlocal call_count
        call_count += 1
        return original_fields(schema_type)

    monkeypatch.setattr(msgspec.structs, "fields", count_fields)
    assert get_msgspec_rename_config(CachedMsgspecStruct) == "camel"
    assert get_msgspec_rename_config(CachedMsgspecStruct) == "camel"
    assert call_count == 1


def test_get_msgspec_rename_config_performance() -> None:
    """Test get_msgspec_rename_config performs efficiently."""
    schema_type = MockMsgspecStructWithCamelRename
    for _ in range(100):
        result = get_msgspec_rename_config(schema_type)
        assert result == "camel"


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        pytest.param(SampleTypedDict, True, id="typed_dict_class"),
        pytest.param(
            cast("SampleTypedDict", {"name": "test", "age": 25, "optional_field": "value"}),
            False,
            id="typed_dict_instance",
        ),
        pytest.param(dict, False, id="dict_type"),
        pytest.param(SampleDataclass, False, id="dataclass_type"),
        pytest.param(str, False, id="str_type"),
        pytest.param(42, False, id="integer"),
        pytest.param({}, False, id="empty_dict"),
        pytest.param({"key": "value"}, False, id="dict_instance"),
    ],
)
def test_is_typed_dict(target: Any, expected: bool) -> None:
    """Validate is_typed_dict distinguishes TypedDict classes from instances and other types."""
    assert is_typed_dict(target) is expected


class MockDriverWithArrow:
    """Mock driver implementing SupportsArrowResults protocol."""

    def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: Any = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> None:
        pass


class MockDriverWithoutArrow:
    """Mock driver not implementing SupportsArrowResults protocol."""

    def execute(self, sql: Any) -> None:
        pass


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        pytest.param(MockDriverWithArrow(), True, id="supports_arrow"),
        pytest.param(MockDriverWithoutArrow(), False, id="missing_arrow_method"),
        pytest.param(None, False, id="none"),
        pytest.param("string", False, id="string"),
        pytest.param(42, False, id="integer"),
        pytest.param([1, 2, 3], False, id="list"),
        pytest.param({"key": "value"}, False, id="dict"),
    ],
)
def test_supports_arrow_results(target: Any, expected: bool) -> None:
    """Validate supports_arrow_results against protocol-compliant and non-compliant objects."""
    assert supports_arrow_results(target) is expected


def test_typing_module_supported_schema_model_includes_mapping() -> None:
    """SupportedSchemaModel should include Mapping[str, Any]."""
    from sqlspec.typing import SupportedSchemaModel

    args = typing.get_args(SupportedSchemaModel)
    assert any(getattr(arg, "__origin__", None) is Mapping for arg in args)
