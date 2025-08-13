"""Type guard functions for runtime type checking in SQLSpec.

This module provides type-safe runtime checks that help the type checker
understand type narrowing, replacing defensive hasattr() and duck typing patterns.
"""

from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.typing import (
    ATTRS_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    DataclassProtocol,
    DTOData,
    Struct,
    attrs_asdict,
    attrs_has,
)

if TYPE_CHECKING:
    from dataclasses import Field

    from sqlglot import exp
    from typing_extensions import TypeGuard

    from sqlspec._typing import AttrsInstanceStub, BaseModelStub, DTODataStub, StructStub
    from sqlspec.builder import Select
    from sqlspec.core.filters import LimitOffsetFilter, StatementFilter
    from sqlspec.protocols import (
        BytesConvertibleProtocol,
        DictProtocol,
        FilterAppenderProtocol,
        FilterParameterProtocol,
        HasExpressionsProtocol,
        HasLimitProtocol,
        HasOffsetProtocol,
        HasOrderByProtocol,
        HasSQLMethodProtocol,
        HasWhereProtocol,
        IndexableRow,
        ObjectStoreItemProtocol,
        ParameterValueProtocol,
        SQLBuilderProtocol,
        WithMethodProtocol,
    )
    from sqlspec.typing import SupportedSchemaModel

__all__ = (
    "can_append_to_statement",
    "can_convert_to_schema",
    "can_extract_parameters",
    "dataclass_to_dict",
    "expression_has_limit",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "get_initial_expression",
    "get_literal_parent",
    "get_node_expressions",
    "get_node_this",
    "get_param_style_and_name",
    "get_value_attribute",
    "has_attr",
    "has_bytes_conversion",
    "has_dict_attribute",
    "has_expression_attr",
    "has_expressions",
    "has_expressions_attribute",
    "has_parameter_builder",
    "has_parameter_value",
    "has_parent_attribute",
    "has_query_builder_parameters",
    "has_sql_method",
    "has_sqlglot_expression",
    "has_this_attribute",
    "has_to_statement",
    "has_with_method",
    "is_attrs_instance",
    "is_attrs_instance_with_field",
    "is_attrs_instance_without_field",
    "is_attrs_schema",
    "is_copy_statement",
    "is_dataclass",
    "is_dataclass_instance",
    "is_dataclass_with_field",
    "is_dataclass_without_field",
    "is_dict",
    "is_dict_row",
    "is_dict_with_field",
    "is_dict_without_field",
    "is_dto_data",
    "is_expression",
    "is_indexable_row",
    "is_iterable_parameters",
    "is_limit_offset_filter",
    "is_msgspec_struct",
    "is_msgspec_struct_with_field",
    "is_msgspec_struct_without_field",
    "is_number_literal",
    "is_object_store_item",
    "is_pydantic_model",
    "is_pydantic_model_with_field",
    "is_pydantic_model_without_field",
    "is_schema",
    "is_schema_or_dict",
    "is_schema_or_dict_with_field",
    "is_schema_or_dict_without_field",
    "is_schema_with_field",
    "is_schema_without_field",
    "is_select_builder",
    "is_statement_filter",
    "is_string_literal",
    "is_typed_parameter",
    "schema_dump",
    "supports_limit",
    "supports_offset",
    "supports_order_by",
    "supports_where",
)


def is_statement_filter(obj: Any) -> "TypeGuard[StatementFilter]":
    """Check if an object implements the StatementFilter protocol.

    Args:
        obj: The object to check

    Returns:
        True if the object is a StatementFilter, False otherwise
    """
    from sqlspec.core.filters import StatementFilter as FilterProtocol

    return isinstance(obj, FilterProtocol)


def is_limit_offset_filter(obj: Any) -> "TypeGuard[LimitOffsetFilter]":
    """Check if an object is a LimitOffsetFilter.

    Args:
        obj: The object to check

    Returns:
        True if the object is a LimitOffsetFilter, False otherwise
    """
    from sqlspec.core.filters import LimitOffsetFilter

    return isinstance(obj, LimitOffsetFilter)


def is_select_builder(obj: Any) -> "TypeGuard[Select]":
    """Check if an object is a Select.

    Args:
        obj: The object to check

    Returns:
        True if the object is a Select, False otherwise
    """
    from sqlspec.builder import Select

    return isinstance(obj, Select)


def is_dict_row(row: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a row is a dictionary.

    Args:
        row: The row to check

    Returns:
        True if the row is a dictionary, False otherwise
    """
    return isinstance(row, dict)


def is_indexable_row(row: Any) -> "TypeGuard[IndexableRow]":
    """Check if a row supports index access via protocol.

    Args:
        row: The row to check

    Returns:
        True if the row is indexable, False otherwise
    """
    from sqlspec.protocols import IndexableRow

    return isinstance(row, IndexableRow)


def is_iterable_parameters(parameters: Any) -> "TypeGuard[Sequence[Any]]":
    """Check if parameters are iterable (but not string or dict).

    Args:
        parameters: The parameters to check

    Returns:
        True if the parameters are iterable, False otherwise
    """
    return isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, dict))


def has_with_method(obj: Any) -> "TypeGuard[WithMethodProtocol]":
    """Check if an object has a callable 'with_' method.

    This is a more specific check than hasattr for SQLGlot expressions.

    Args:
        obj: The object to check

    Returns:
        True if the object has a callable with_ method, False otherwise
    """
    from sqlspec.protocols import WithMethodProtocol

    return isinstance(obj, WithMethodProtocol)


def can_convert_to_schema(obj: Any) -> "TypeGuard[Any]":
    """Check if an object has the ToSchemaMixin capabilities.

    This provides better DX than isinstance checks for driver mixins.

    Args:
        obj: The object to check (typically a driver instance)

    Returns:
        True if the object has to_schema method, False otherwise
    """
    from sqlspec.driver.mixins import ToSchemaMixin

    return isinstance(obj, ToSchemaMixin)


def is_dataclass_instance(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    if isinstance(obj, type):
        return False
    try:
        _ = type(obj).__dataclass_fields__
    except AttributeError:
        return False
    else:
        return True


def is_dataclass(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if isinstance(obj, type):
        try:
            _ = obj.__dataclass_fields__  # type: ignore[attr-defined]
        except AttributeError:
            return False
        else:
            return True
    return is_dataclass_instance(obj)


def is_dataclass_with_field(obj: Any, field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_dataclass(obj):
        return False
    try:
        _ = getattr(obj, field_name)
    except AttributeError:
        return False
    else:
        return True


def is_dataclass_without_field(obj: Any, field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_dataclass(obj):
        return False
    try:
        _ = getattr(obj, field_name)
    except AttributeError:
        return True
    else:
        return False


def is_pydantic_model(obj: Any) -> "TypeGuard[Any]":
    """Check if a value is a pydantic model class or instance.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if not PYDANTIC_INSTALLED:
        return False
    if isinstance(obj, type):
        try:
            return issubclass(obj, BaseModel)
        except TypeError:
            return False
    return isinstance(obj, BaseModel)


def is_pydantic_model_with_field(obj: Any, field_name: str) -> "TypeGuard[BaseModelStub]":
    """Check if a pydantic model has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_pydantic_model(obj):
        return False
    try:
        _ = getattr(obj, field_name)
    except AttributeError:
        return False
    else:
        return True


def is_pydantic_model_without_field(obj: Any, field_name: str) -> "TypeGuard[BaseModelStub]":
    """Check if a pydantic model does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_pydantic_model(obj):
        return False
    try:
        _ = getattr(obj, field_name)
    except AttributeError:
        return True
    else:
        return False


def is_msgspec_struct(obj: Any) -> "TypeGuard[StructStub]":
    """Check if a value is a msgspec struct class or instance.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if not MSGSPEC_INSTALLED:
        return False
    if isinstance(obj, type):
        try:
            return issubclass(obj, Struct)
        except TypeError:
            return False
    return isinstance(obj, Struct)


def is_msgspec_struct_with_field(obj: Any, field_name: str) -> "TypeGuard[StructStub]":
    """Check if a msgspec struct has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_msgspec_struct(obj):
        return False
    try:
        _ = getattr(obj, field_name)

    except AttributeError:
        return False
    return True


def is_msgspec_struct_without_field(obj: Any, field_name: str) -> "TypeGuard[StructStub]":
    """Check if a msgspec struct does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_msgspec_struct(obj):
        return False
    try:
        _ = getattr(obj, field_name)
    except AttributeError:
        return True
    return False


def is_attrs_instance(obj: Any) -> "TypeGuard[AttrsInstanceStub]":
    """Check if a value is an attrs class instance.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return ATTRS_INSTALLED and attrs_has(obj.__class__)


def is_attrs_schema(cls: Any) -> "TypeGuard[type[AttrsInstanceStub]]":
    """Check if a class type is an attrs schema.

    Args:
        cls: Class to check.

    Returns:
        bool
    """
    return ATTRS_INSTALLED and attrs_has(cls)


def is_attrs_instance_with_field(obj: Any, field_name: str) -> "TypeGuard[AttrsInstanceStub]":
    """Check if an attrs instance has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_attrs_instance(obj) and hasattr(obj, field_name)


def is_attrs_instance_without_field(obj: Any, field_name: str) -> "TypeGuard[AttrsInstanceStub]":
    """Check if an attrs instance does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_attrs_instance(obj) and not hasattr(obj, field_name)


def is_dict(obj: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a value is a dictionary.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, dict)


def is_dict_with_field(obj: Any, field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name in obj


def is_dict_without_field(obj: Any, field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name not in obj


def is_schema(obj: Any) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct, Pydantic model, attrs instance, or schema class.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return (
        is_msgspec_struct(obj)
        or is_pydantic_model(obj)
        or is_attrs_instance(obj)
        or is_attrs_schema(obj)
        or is_dataclass(obj)
    )


def is_schema_or_dict(obj: Any) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_schema(obj) or is_dict(obj)


def is_schema_with_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct_with_field(obj, field_name) or is_pydantic_model_with_field(obj, field_name)


def is_schema_without_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_with_field(obj, field_name)


def is_schema_or_dict_with_field(obj: Any, field_name: str) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_schema_with_field(obj, field_name) or is_dict_with_field(obj, field_name)


def is_schema_or_dict_without_field(
    obj: Any, field_name: str
) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_or_dict_with_field(obj, field_name)


def is_dto_data(v: Any) -> "TypeGuard[DTODataStub[Any]]":
    """Check if a value is a Litestar DTOData object.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return LITESTAR_INSTALLED and isinstance(v, DTOData)


def is_expression(obj: Any) -> "TypeGuard[exp.Expression]":
    """Check if a value is a sqlglot Expression.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    from sqlglot import exp

    return isinstance(obj, exp.Expression)


def has_dict_attribute(obj: Any) -> "TypeGuard[DictProtocol]":
    """Check if an object has a __dict__ attribute.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    from sqlspec.protocols import DictProtocol

    return isinstance(obj, DictProtocol)


def extract_dataclass_fields(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "Optional[AbstractSet[str]]" = None,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "tuple[Field[Any], ...]":
    """Extract dataclass fields.

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.

    Raises:
        ValueError: If there are fields that are both included and excluded.

    Returns:
        A tuple of dataclass fields.
    """
    from dataclasses import Field, fields

    from sqlspec._typing import Empty

    include = include or set()
    exclude = exclude or set()

    if common := (include & exclude):
        msg = f"Fields {common} are both included and excluded."
        raise ValueError(msg)

    dataclass_fields: list[Field[Any]] = list(fields(obj))
    if exclude_none:
        dataclass_fields = [field for field in dataclass_fields if getattr(obj, field.name) is not None]
    if exclude_empty:
        dataclass_fields = [field for field in dataclass_fields if getattr(obj, field.name) is not Empty]
    if include:
        dataclass_fields = [field for field in dataclass_fields if field.name in include]
    if exclude:
        dataclass_fields = [field for field in dataclass_fields if field.name not in exclude]

    return tuple(dataclass_fields)


def extract_dataclass_items(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "Optional[AbstractSet[str]]" = None,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "tuple[tuple[str, Any], ...]":
    """Extract name-value pairs from a dataclass instance.

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.

    Returns:
        A tuple of key/value pairs.
    """
    dataclass_fields = extract_dataclass_fields(obj, exclude_none, exclude_empty, include, exclude)
    return tuple((field.name, getattr(obj, field.name)) for field in dataclass_fields)


def dataclass_to_dict(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "dict[str, Any]":
    """Convert a dataclass instance to a dictionary.

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        convert_nested: Whether to recursively convert nested dataclasses.
        exclude: An iterable of fields to exclude.

    Returns:
        A dictionary of key/value pairs.
    """
    ret = {}
    for field in extract_dataclass_fields(obj, exclude_none, exclude_empty, exclude=exclude):
        value = getattr(obj, field.name)
        if is_dataclass_instance(value) and convert_nested:
            ret[field.name] = dataclass_to_dict(value, exclude_none, exclude_empty)
        else:
            ret[field.name] = getattr(obj, field.name)
    return cast("dict[str, Any]", ret)


def schema_dump(data: Any, exclude_unset: bool = True) -> "dict[str, Any]":
    """Dump a data object to a dictionary.

    Args:
        data:  :type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`AttrsInstance`
        exclude_unset: :type:`bool` Whether to exclude unset values.

    Returns:
        :type:`dict[str, Any]`
    """
    from sqlspec._typing import UNSET

    if is_dict(data):
        return data
    if is_dataclass(data):
        return dataclass_to_dict(data, exclude_empty=exclude_unset)
    if is_pydantic_model(data):
        return data.model_dump(exclude_unset=exclude_unset)  # type: ignore[no-any-return]
    if is_msgspec_struct(data):
        if exclude_unset:
            return {f: val for f in data.__struct_fields__ if (val := getattr(data, f, None)) != UNSET}
        return {f: getattr(data, f, None) for f in data.__struct_fields__}
    if is_attrs_instance(data):
        return attrs_asdict(data)

    if has_dict_attribute(data):
        return data.__dict__
    return cast("dict[str, Any]", data)


def can_extract_parameters(obj: Any) -> "TypeGuard[FilterParameterProtocol]":
    """Check if an object can extract parameters."""
    from sqlspec.protocols import FilterParameterProtocol

    return isinstance(obj, FilterParameterProtocol)


def can_append_to_statement(obj: Any) -> "TypeGuard[FilterAppenderProtocol]":
    """Check if an object can append to SQL statements."""
    from sqlspec.protocols import FilterAppenderProtocol

    return isinstance(obj, FilterAppenderProtocol)


def has_parameter_value(obj: Any) -> "TypeGuard[ParameterValueProtocol]":
    """Check if an object has a value attribute (parameter wrapper)."""
    from sqlspec.protocols import ParameterValueProtocol

    return isinstance(obj, ParameterValueProtocol)


def supports_where(obj: Any) -> "TypeGuard[HasWhereProtocol]":
    """Check if an SQL expression supports WHERE clauses."""
    from sqlspec.protocols import HasWhereProtocol

    return isinstance(obj, HasWhereProtocol)


def supports_limit(obj: Any) -> "TypeGuard[HasLimitProtocol]":
    """Check if an SQL expression supports LIMIT clauses."""
    from sqlspec.protocols import HasLimitProtocol

    return isinstance(obj, HasLimitProtocol)


def supports_offset(obj: Any) -> "TypeGuard[HasOffsetProtocol]":
    """Check if an SQL expression supports OFFSET clauses."""
    from sqlspec.protocols import HasOffsetProtocol

    return isinstance(obj, HasOffsetProtocol)


def supports_order_by(obj: Any) -> "TypeGuard[HasOrderByProtocol]":
    """Check if an SQL expression supports ORDER BY clauses."""
    from sqlspec.protocols import HasOrderByProtocol

    return isinstance(obj, HasOrderByProtocol)


def has_bytes_conversion(obj: Any) -> "TypeGuard[BytesConvertibleProtocol]":
    """Check if an object can be converted to bytes."""
    from sqlspec.protocols import BytesConvertibleProtocol

    return isinstance(obj, BytesConvertibleProtocol)


def has_expressions(obj: Any) -> "TypeGuard[HasExpressionsProtocol]":
    """Check if an object has an expressions attribute."""
    from sqlspec.protocols import HasExpressionsProtocol

    return isinstance(obj, HasExpressionsProtocol)


def has_sql_method(obj: Any) -> "TypeGuard[HasSQLMethodProtocol]":
    """Check if an object has a sql() method for rendering SQL."""
    from sqlspec.protocols import HasSQLMethodProtocol

    return isinstance(obj, HasSQLMethodProtocol)


def has_query_builder_parameters(obj: Any) -> "TypeGuard[SQLBuilderProtocol]":
    """Check if an object is a query builder with parameters property."""
    return (
        hasattr(obj, "build")
        and callable(getattr(obj, "build", None))
        and hasattr(obj, "parameters")
        and hasattr(obj, "add_parameter")
        and callable(getattr(obj, "add_parameter", None))
    )


def is_object_store_item(obj: Any) -> "TypeGuard[ObjectStoreItemProtocol]":
    """Check if an object is an object store item with path/key attributes."""
    from sqlspec.protocols import ObjectStoreItemProtocol

    return isinstance(obj, ObjectStoreItemProtocol)


def has_sqlglot_expression(obj: Any) -> "TypeGuard[Any]":
    """Check if an object has a sqlglot_expression property."""
    from sqlspec.protocols import HasSQLGlotExpressionProtocol

    return isinstance(obj, HasSQLGlotExpressionProtocol)


def has_parameter_builder(obj: Any) -> "TypeGuard[Any]":
    """Check if an object has an add_parameter method."""
    from sqlspec.protocols import HasParameterBuilderProtocol

    return isinstance(obj, HasParameterBuilderProtocol)


def has_expression_attr(obj: Any) -> "TypeGuard[Any]":
    """Check if an object has an _expression attribute."""
    from sqlspec.protocols import HasExpressionProtocol

    return isinstance(obj, HasExpressionProtocol)


def has_to_statement(obj: Any) -> "TypeGuard[Any]":
    """Check if an object has a to_statement method."""
    from sqlspec.protocols import HasToStatementProtocol

    return isinstance(obj, HasToStatementProtocol)


def has_attr(obj: Any, attr: str) -> bool:
    """Safe replacement for hasattr() that works with mypyc.

    Args:
        obj: Object to check
        attr: Attribute name to look for

    Returns:
        True if attribute exists, False otherwise
    """
    try:
        getattr(obj, attr)
    except AttributeError:
        return False
    return True


def get_node_this(node: "exp.Expression", default: Optional[Any] = None) -> Any:
    """Safely get the 'this' attribute from a SQLGlot node.

    Args:
        node: The SQLGlot expression node
        default: Default value if 'this' attribute doesn't exist

    Returns:
        The value of node.this or the default value
    """
    try:
        return node.this
    except AttributeError:
        return default


def has_this_attribute(node: "exp.Expression") -> bool:
    """Check if a node has the 'this' attribute without using hasattr().

    Args:
        node: The SQLGlot expression node

    Returns:
        True if the node has a 'this' attribute, False otherwise
    """
    try:
        _ = node.this
    except AttributeError:
        return False
    return True


def get_node_expressions(node: "exp.Expression", default: Optional[Any] = None) -> Any:
    """Safely get the 'expressions' attribute from a SQLGlot node.

    Args:
        node: The SQLGlot expression node
        default: Default value if 'expressions' attribute doesn't exist

    Returns:
        The value of node.expressions or the default value
    """
    try:
        return node.expressions
    except AttributeError:
        return default


def has_expressions_attribute(node: "exp.Expression") -> bool:
    """Check if a node has the 'expressions' attribute without using hasattr().

    Args:
        node: The SQLGlot expression node

    Returns:
        True if the node has an 'expressions' attribute, False otherwise
    """
    try:
        _ = node.expressions
    except AttributeError:
        return False
    return True


def get_literal_parent(literal: "exp.Expression", default: Optional[Any] = None) -> Any:
    """Safely get the 'parent' attribute from a SQLGlot literal.

    Args:
        literal: The SQLGlot expression
        default: Default value if 'parent' attribute doesn't exist

    Returns:
        The value of literal.parent or the default value
    """
    try:
        return literal.parent
    except AttributeError:
        return default


def has_parent_attribute(literal: "exp.Expression") -> bool:
    """Check if a literal has the 'parent' attribute without using hasattr().

    Args:
        literal: The SQLGlot expression

    Returns:
        True if the literal has a 'parent' attribute, False otherwise
    """
    try:
        _ = literal.parent
    except AttributeError:
        return False
    return True


def is_string_literal(literal: "exp.Literal") -> bool:
    """Check if a literal is a string literal without using hasattr().

    Args:
        literal: The SQLGlot Literal expression

    Returns:
        True if the literal is a string, False otherwise
    """
    try:
        return bool(literal.is_string)
    except AttributeError:
        try:
            return isinstance(literal.this, str)
        except AttributeError:
            return False


def is_number_literal(literal: "exp.Literal") -> bool:
    """Check if a literal is a number literal without using hasattr().

    Args:
        literal: The SQLGlot Literal expression

    Returns:
        True if the literal is a number, False otherwise
    """
    try:
        return bool(literal.is_number)
    except AttributeError:
        try:
            if literal.this is not None:
                float(str(literal.this))
                return True
        except (AttributeError, ValueError, TypeError):
            pass
        return False


def get_initial_expression(context: Any) -> "Optional[exp.Expression]":
    """Safely get initial_expression from context.

    Args:
        context: SQL processing context

    Returns:
        The initial expression or None if not available
    """
    try:
        return context.initial_expression  # type: ignore[no-any-return]
    except AttributeError:
        return None


def expression_has_limit(expr: "Optional[exp.Expression]") -> bool:
    """Check if an expression has a limit clause.

    Args:
        expr: SQLGlot expression to check

    Returns:
        True if expression has limit in args, False otherwise
    """
    if expr is None:
        return False
    try:
        return "limit" in expr.args
    except AttributeError:
        return False


def get_value_attribute(obj: Any) -> Any:
    """Safely get the 'value' attribute from an object.

    Args:
        obj: Object to get value from

    Returns:
        The value attribute or the object itself if no value attribute
    """
    try:
        return obj.value
    except AttributeError:
        return obj


def get_param_style_and_name(param: Any) -> "tuple[Optional[str], Optional[str]]":
    """Safely get style and name attributes from a parameter.

    Args:
        param: Parameter object

    Returns:
        Tuple of (style, name) or (None, None) if attributes don't exist
    """
    try:
        style = param.style
        name = param.name
    except AttributeError:
        return None, None
    return style, name


def is_copy_statement(expression: Any) -> "TypeGuard[exp.Expression]":
    """Check if the SQL expression is a PostgreSQL COPY statement.

    Args:
        expression: The SQL expression to check

    Returns:
        True if this is a COPY statement, False otherwise
    """
    from sqlglot import exp

    if expression is None:
        return False

    if has_attr(exp, "Copy") and isinstance(expression, getattr(exp, "Copy", type(None))):
        return True

    if isinstance(expression, (exp.Command, exp.Anonymous)):
        sql_text = str(expression).strip().upper()
        return sql_text.startswith("COPY ")

    return False


def is_typed_parameter(obj: Any) -> "TypeGuard[Any]":
    """Check if an object is a typed parameter.

    Args:
        obj: The object to check

    Returns:
        True if the object is a TypedParameter, False otherwise
    """
    from sqlspec.core.parameters import TypedParameter

    return isinstance(obj, TypedParameter)
