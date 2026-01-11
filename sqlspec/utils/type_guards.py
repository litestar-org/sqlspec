"""Type guard functions for runtime type checking in SQLSpec.

This module provides type-safe runtime checks that help the type checker
understand type narrowing, replacing defensive hasattr() and duck typing patterns.
"""

from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import Field
from dataclasses import fields as dataclasses_fields
from dataclasses import is_dataclass as dataclasses_is_dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

from sqlglot import exp
from typing_extensions import is_typeddict

from sqlspec._typing import Empty
from sqlspec.protocols import (
    ArrowTableStatsProtocol,
    AsyncDeleteProtocol,
    AsyncReadableProtocol,
    AsyncReadBytesProtocol,
    AsyncWriteBytesProtocol,
    CursorMetadataProtocol,
    DictProtocol,
    HasAddListenerProtocol,
    HasConfigProtocol,
    HasConnectionConfigProtocol,
    HasDatabaseUrlAndBindKeyProtocol,
    HasErrorsProtocol,
    HasExpressionAndParametersProtocol,
    HasExpressionAndSQLProtocol,
    HasExpressionProtocol,
    HasExtensionConfigProtocol,
    HasFieldNameProtocol,
    HasFilterAttributesProtocol,
    HasGetDataProtocol,
    HasLastRowIdProtocol,
    HasMigrationConfigProtocol,
    HasNameProtocol,
    HasNotifiesProtocol,
    HasParameterBuilderProtocol,
    HasRowcountProtocol,
    HasSQLGlotExpressionProtocol,
    HasSqliteErrorProtocol,
    HasSqlStateProtocol,
    HasStatementConfigFactoryProtocol,
    HasStatementTypeProtocol,
    HasTracerProviderProtocol,
    HasTypeCodeProtocol,
    HasTypecodeProtocol,
    HasTypecodeSizedProtocol,
    HasValueProtocol,
    HasWhereProtocol,
    NotificationProtocol,
    PipelineCapableProtocol,
    QueryResultProtocol,
    ReadableProtocol,
    SpanAttributeProtocol,
    SupportsArrayProtocol,
    SupportsArrowResults,
    SupportsCloseProtocol,
    SupportsDtypeStrProtocol,
    SupportsJsonTypeProtocol,
    WithMethodProtocol,
)
from sqlspec.typing import (
    ATTRS_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    DataclassProtocol,
    DTOData,
    Struct,
    attrs_fields,
    attrs_has,
)
from sqlspec.utils.text import camelize, kebabize, pascalize

if TYPE_CHECKING:
    from typing import TypeGuard

    from sqlspec._typing import AttrsInstanceStub, BaseModelStub, DTODataStub, StructStub
    from sqlspec.core import StatementFilter
    from sqlspec.core.parameters import TypedParameter
    from sqlspec.typing import SupportedSchemaModel

__all__ = (
    "dataclass_to_dict",
    "expression_has_limit",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "get_initial_expression",
    "get_literal_parent",
    "get_msgspec_rename_config",
    "get_node_expressions",
    "get_node_this",
    "get_param_style_and_name",
    "get_value_attribute",
    "has_add_listener",
    "has_array_interface",
    "has_arrow_table_stats",
    "has_config_attribute",
    "has_connection_config",
    "has_cursor_metadata",
    "has_database_url_and_bind_key",
    "has_dict_attribute",
    "has_dtype_str",
    "has_errors",
    "has_expression_and_parameters",
    "has_expression_and_sql",
    "has_expression_attr",
    "has_expressions_attribute",
    "has_extension_config",
    "has_field_name",
    "has_filter_attributes",
    "has_get_data",
    "has_lastrowid",
    "has_migration_config",
    "has_name",
    "has_notifies",
    "has_parameter_builder",
    "has_parent_attribute",
    "has_pipeline_capability",
    "has_query_result_metadata",
    "has_rowcount",
    "has_span_attribute",
    "has_sqlglot_expression",
    "has_sqlite_error",
    "has_sqlstate",
    "has_statement_config_factory",
    "has_statement_type",
    "has_this_attribute",
    "has_tracer_provider",
    "has_type_code",
    "has_typecode",
    "has_typecode_and_len",
    "has_value_attribute",
    "has_with_method",
    "is_async_readable",
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
    "is_iterable_parameters",
    "is_local_path",
    "is_msgspec_struct",
    "is_msgspec_struct_with_field",
    "is_msgspec_struct_without_field",
    "is_notification",
    "is_number_literal",
    "is_pydantic_model",
    "is_pydantic_model_with_field",
    "is_pydantic_model_without_field",
    "is_readable",
    "is_schema",
    "is_schema_or_dict",
    "is_schema_or_dict_with_field",
    "is_schema_or_dict_without_field",
    "is_schema_with_field",
    "is_schema_without_field",
    "is_statement_filter",
    "is_string_literal",
    "is_typed_dict",
    "is_typed_parameter",
    "supports_arrow_results",
    "supports_async_delete",
    "supports_async_read_bytes",
    "supports_async_write_bytes",
    "supports_close",
    "supports_json_type",
    "supports_where",
)


def is_readable(obj: Any) -> "TypeGuard[ReadableProtocol]":
    """Check if an object is readable (has a read method)."""
    return isinstance(obj, ReadableProtocol)


def is_async_readable(obj: Any) -> "TypeGuard[AsyncReadableProtocol]":
    """Check if an object exposes an async read method."""
    return isinstance(obj, AsyncReadableProtocol)


def is_notification(obj: Any) -> "TypeGuard[NotificationProtocol]":
    """Check if an object is a database notification with channel and payload."""
    return isinstance(obj, NotificationProtocol)


def has_pipeline_capability(obj: Any) -> "TypeGuard[PipelineCapableProtocol]":
    """Check if a connection supports pipeline execution."""
    return isinstance(obj, PipelineCapableProtocol)


def has_query_result_metadata(obj: Any) -> "TypeGuard[QueryResultProtocol]":
    """Check if an object has query result metadata (tag/status)."""
    return isinstance(obj, QueryResultProtocol)


def has_array_interface(obj: Any) -> "TypeGuard[SupportsArrayProtocol]":
    """Check if an object supports the array interface (like NumPy arrays)."""
    return isinstance(obj, SupportsArrayProtocol)


def has_cursor_metadata(obj: Any) -> "TypeGuard[CursorMetadataProtocol]":
    """Check if an object has cursor metadata (description)."""
    return isinstance(obj, CursorMetadataProtocol)


def has_add_listener(obj: Any) -> "TypeGuard[HasAddListenerProtocol]":
    """Check if an object exposes add_listener()."""
    return isinstance(obj, HasAddListenerProtocol)


def has_notifies(obj: Any) -> "TypeGuard[HasNotifiesProtocol]":
    """Check if an object exposes notifies."""
    return isinstance(obj, HasNotifiesProtocol)


def has_extension_config(obj: Any) -> "TypeGuard[HasExtensionConfigProtocol]":
    """Check if an object exposes extension_config mapping."""
    return isinstance(obj, HasExtensionConfigProtocol)


def has_config_attribute(obj: Any) -> "TypeGuard[HasConfigProtocol]":
    """Check if an object exposes config attribute."""
    return isinstance(obj, HasConfigProtocol)


def has_connection_config(obj: Any) -> "TypeGuard[HasConnectionConfigProtocol]":
    """Check if an object exposes connection_config mapping."""
    return isinstance(obj, HasConnectionConfigProtocol)


def has_database_url_and_bind_key(obj: Any) -> "TypeGuard[HasDatabaseUrlAndBindKeyProtocol]":
    """Check if an object exposes database_url and bind_key."""
    return isinstance(obj, HasDatabaseUrlAndBindKeyProtocol)


def has_name(obj: Any) -> "TypeGuard[HasNameProtocol]":
    """Check if an object exposes __name__."""
    return isinstance(obj, HasNameProtocol)


def has_field_name(obj: Any) -> "TypeGuard[HasFieldNameProtocol]":
    """Check if an object exposes field_name attribute."""
    return isinstance(obj, HasFieldNameProtocol)


def has_filter_attributes(obj: Any) -> "TypeGuard[HasFilterAttributesProtocol]":
    """Check if an object exposes filter attribute set."""
    return isinstance(obj, HasFilterAttributesProtocol)


def has_get_data(obj: Any) -> "TypeGuard[HasGetDataProtocol]":
    """Check if an object exposes get_data()."""
    return isinstance(obj, HasGetDataProtocol)


def has_arrow_table_stats(obj: Any) -> "TypeGuard[ArrowTableStatsProtocol]":
    """Check if an object exposes Arrow row/byte stats."""
    return isinstance(obj, ArrowTableStatsProtocol)


def has_rowcount(obj: Any) -> "TypeGuard[HasRowcountProtocol]":
    """Check if a cursor exposes rowcount metadata."""
    return isinstance(obj, HasRowcountProtocol)


def has_lastrowid(obj: Any) -> "TypeGuard[HasLastRowIdProtocol]":
    """Check if a cursor exposes lastrowid metadata."""
    return isinstance(obj, HasLastRowIdProtocol)


def has_dtype_str(obj: Any) -> "TypeGuard[SupportsDtypeStrProtocol]":
    """Check if a dtype exposes string descriptor."""
    return isinstance(obj, SupportsDtypeStrProtocol)


def has_statement_type(obj: Any) -> "TypeGuard[HasStatementTypeProtocol]":
    """Check if a cursor exposes statement_type metadata."""
    return isinstance(obj, HasStatementTypeProtocol)


def has_typecode(obj: Any) -> "TypeGuard[HasTypecodeProtocol]":
    """Check if an array-like object exposes typecode."""
    return isinstance(obj, HasTypecodeProtocol)


def has_typecode_and_len(obj: Any) -> "TypeGuard[HasTypecodeSizedProtocol]":
    """Check if an array-like object exposes typecode and length."""
    return isinstance(obj, HasTypecodeSizedProtocol)


def has_type_code(obj: Any) -> "TypeGuard[HasTypeCodeProtocol]":
    """Check if an object exposes type_code."""
    return isinstance(obj, HasTypeCodeProtocol)


def has_sqlstate(obj: Any) -> "TypeGuard[HasSqlStateProtocol]":
    """Check if an exception exposes sqlstate."""
    return isinstance(obj, HasSqlStateProtocol)


def has_sqlite_error(obj: Any) -> "TypeGuard[HasSqliteErrorProtocol]":
    """Check if an exception exposes sqlite error details."""
    return isinstance(obj, HasSqliteErrorProtocol)


def has_value_attribute(obj: Any) -> "TypeGuard[HasValueProtocol]":
    """Check if an object exposes a value attribute."""
    return isinstance(obj, HasValueProtocol)


def has_errors(obj: Any) -> "TypeGuard[HasErrorsProtocol]":
    """Check if an exception exposes errors."""
    return isinstance(obj, HasErrorsProtocol)


def has_span_attribute(obj: Any) -> "TypeGuard[SpanAttributeProtocol]":
    """Check if a span exposes set_attribute."""
    return isinstance(obj, SpanAttributeProtocol)


def has_tracer_provider(obj: Any) -> "TypeGuard[HasTracerProviderProtocol]":
    """Check if an object exposes get_tracer."""
    return isinstance(obj, HasTracerProviderProtocol)


def supports_async_read_bytes(obj: Any) -> "TypeGuard[AsyncReadBytesProtocol]":
    """Check if backend supports async read_bytes."""
    return isinstance(obj, AsyncReadBytesProtocol)


def supports_async_write_bytes(obj: Any) -> "TypeGuard[AsyncWriteBytesProtocol]":
    """Check if backend supports async write_bytes."""
    return isinstance(obj, AsyncWriteBytesProtocol)


def supports_json_type(obj: Any) -> "TypeGuard[SupportsJsonTypeProtocol]":
    """Check if an object exposes JSON type support."""
    return isinstance(obj, SupportsJsonTypeProtocol)


def supports_close(obj: Any) -> "TypeGuard[SupportsCloseProtocol]":
    """Check if an object exposes close()."""
    return isinstance(obj, SupportsCloseProtocol)


def supports_async_delete(obj: Any) -> "TypeGuard[AsyncDeleteProtocol]":
    """Check if backend supports async delete."""
    return isinstance(obj, AsyncDeleteProtocol)


def supports_where(obj: Any) -> "TypeGuard[HasWhereProtocol]":
    """Check if an SQL expression supports WHERE clauses."""
    return isinstance(obj, HasWhereProtocol)


def is_typed_dict(obj: Any) -> "TypeGuard[type]":
    """Check if an object is a TypedDict class.

    Args:
        obj: The object to check

    Returns:
        True if the object is a TypedDict class, False otherwise
    """
    return is_typeddict(obj)


def is_statement_filter(obj: Any) -> "TypeGuard[StatementFilter]":
    """Check if an object implements the StatementFilter protocol.

    Args:
        obj: The object to check

    Returns:
        True if the object is a StatementFilter, False otherwise
    """
    from sqlspec.core.filters import StatementFilter as FilterProtocol

    return isinstance(obj, FilterProtocol)


def is_dict_row(row: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a row is a dictionary.

    Args:
        row: The row to check

    Returns:
        True if the row is a dictionary, False otherwise
    """
    return isinstance(row, dict)


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
    return isinstance(obj, WithMethodProtocol)


def is_dataclass_instance(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    if isinstance(obj, type):
        return False
    return dataclasses_is_dataclass(obj)


def is_dataclass(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return dataclasses_is_dataclass(obj)


def is_dataclass_with_field(obj: Any, field_name: str) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass and has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_dataclass(obj):
        return False
    return any(field.name == field_name for field in dataclasses_fields(obj))


def is_dataclass_without_field(obj: Any, field_name: str) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass and does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_dataclass(obj):
        return False
    return all(field.name != field_name for field in dataclasses_fields(obj))


def is_pydantic_model(obj: Any) -> "TypeGuard[BaseModelStub]":
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
        fields = obj.model_fields
    except AttributeError:
        try:
            fields = obj.__fields__  # type: ignore[attr-defined]
        except AttributeError:
            return False
    return field_name in fields


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
        fields = obj.model_fields
    except AttributeError:
        try:
            fields = obj.__fields__  # type: ignore[attr-defined]
        except AttributeError:
            return True
    return field_name not in fields


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
    from msgspec import structs

    struct_type = obj if isinstance(obj, type) else type(obj)
    fields = structs.fields(cast("Any", struct_type))
    return any(field.name == field_name for field in fields)


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
    from msgspec import structs

    struct_type = obj if isinstance(obj, type) else type(obj)
    fields = structs.fields(cast("Any", struct_type))
    return all(field.name != field_name for field in fields)


@lru_cache(maxsize=500)
def _detect_rename_pattern(field_name: str, encode_name: str) -> "str | None":
    """Detect the rename pattern by comparing field name transformations.

    Args:
        field_name: Original field name (e.g., "user_id")
        encode_name: Encoded field name (e.g., "userId")

    Returns:
        The detected rename pattern ("camel", "kebab", "pascal") or None
    """
    if encode_name == camelize(field_name) and encode_name != field_name:
        return "camel"

    if encode_name == kebabize(field_name) and encode_name != field_name:
        return "kebab"

    if encode_name == pascalize(field_name) and encode_name != field_name:
        return "pascal"
    return None


def get_msgspec_rename_config(schema_type: type) -> "str | None":
    """Extract msgspec rename configuration from a struct type.

    Analyzes field name transformations to detect the rename pattern used by msgspec.
    Since msgspec doesn't store the original rename parameter directly, we infer it
    by comparing field names with their encode_name values.

    Args:
        schema_type: The msgspec struct type to inspect.

    Returns:
        The rename configuration value ("camel", "kebab", "pascal", etc.) if detected,
        None if no rename configuration exists or if not a msgspec struct.

    Examples:
        >>> class User(msgspec.Struct, rename="camel"):
        ...     user_id: int
        >>> get_msgspec_rename_config(User)
        "camel"

        >>> class Product(msgspec.Struct):
        ...     product_id: int
        >>> get_msgspec_rename_config(Product)
        None
    """
    if not MSGSPEC_INSTALLED:
        return None

    if not is_msgspec_struct(schema_type):
        return None

    from msgspec import structs

    fields: tuple[Any, ...] = structs.fields(cast("Any", schema_type))
    if not fields:
        return None

    for field in fields:
        if field.name != field.encode_name:
            return _detect_rename_pattern(field.name, field.encode_name)

    return None


def is_attrs_instance(obj: Any) -> "TypeGuard[AttrsInstanceStub]":
    """Check if a value is an attrs class instance.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return bool(ATTRS_INSTALLED) and attrs_has(obj.__class__)


def is_attrs_schema(cls: Any) -> "TypeGuard[type[AttrsInstanceStub]]":
    """Check if a class type is an attrs schema.

    Args:
        cls: Class to check.

    Returns:
        bool
    """
    return bool(ATTRS_INSTALLED) and attrs_has(cls)


def is_attrs_instance_with_field(obj: Any, field_name: str) -> "TypeGuard[AttrsInstanceStub]":
    """Check if an attrs instance has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_attrs_instance(obj):
        return False
    return any(field.name == field_name for field in attrs_fields(obj.__class__))


def is_attrs_instance_without_field(obj: Any, field_name: str) -> "TypeGuard[AttrsInstanceStub]":
    """Check if an attrs instance does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    if not is_attrs_instance(obj):
        return False
    return all(field.name != field_name for field in attrs_fields(obj.__class__))


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


def is_schema_or_dict(obj: Any) -> "TypeGuard[SupportedSchemaModel | dict[str, Any]]":
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


def is_schema_or_dict_with_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel | dict[str, Any]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_schema_with_field(obj, field_name) or is_dict_with_field(obj, field_name)


def is_schema_or_dict_without_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel | dict[str, Any]]":
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
    return bool(LITESTAR_INSTALLED) and isinstance(v, DTOData)


def is_expression(obj: Any) -> "TypeGuard[exp.Expression]":
    """Check if a value is a sqlglot Expression.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, exp.Expression)


def has_dict_attribute(obj: Any) -> "TypeGuard[DictProtocol]":
    """Check if an object has a __dict__ attribute.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, DictProtocol)


def extract_dataclass_fields(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "AbstractSet[str] | None" = None,
    exclude: "AbstractSet[str] | None" = None,
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
    include = include or set()
    exclude = exclude or set()

    if common := (include & exclude):
        msg = f"Fields {common} are both included and excluded."
        raise ValueError(msg)

    dataclass_fields: list[Field[Any]] = list(dataclasses_fields(obj))
    if exclude_none:
        dataclass_fields = [field for field in dataclass_fields if object.__getattribute__(obj, field.name) is not None]
    if exclude_empty:
        dataclass_fields = [
            field for field in dataclass_fields if object.__getattribute__(obj, field.name) is not Empty
        ]
    if include:
        dataclass_fields = [field for field in dataclass_fields if field.name in include]
    if exclude:
        dataclass_fields = [field for field in dataclass_fields if field.name not in exclude]

    return tuple(dataclass_fields)


def extract_dataclass_items(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "AbstractSet[str] | None" = None,
    exclude: "AbstractSet[str] | None" = None,
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
    return tuple((field.name, object.__getattribute__(obj, field.name)) for field in dataclass_fields)


def dataclass_to_dict(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
    exclude: "AbstractSet[str] | None" = None,
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
        value = object.__getattribute__(obj, field.name)
        if is_dataclass_instance(value) and convert_nested:
            ret[field.name] = dataclass_to_dict(value, exclude_none, exclude_empty)
        else:
            ret[field.name] = value
    return cast("dict[str, Any]", ret)


def get_node_this(node: "exp.Expression", default: Any | None = None) -> Any:
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


def get_node_expressions(node: "exp.Expression", default: Any | None = None) -> Any:
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


def get_literal_parent(literal: "exp.Expression", default: Any | None = None) -> Any:
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


def get_initial_expression(context: Any) -> "exp.Expression | None":
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


def expression_has_limit(expr: "exp.Expression | None") -> bool:
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
    if isinstance(obj, HasValueProtocol):
        return obj.value
    return obj


def get_param_style_and_name(param: Any) -> "tuple[str | None, str | None]":
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
    if expression is None:
        return False

    try:
        copy_expr = exp.Copy
    except AttributeError:
        copy_expr = None
    if copy_expr is not None and isinstance(expression, copy_expr):
        return True

    if isinstance(expression, (exp.Command, exp.Anonymous)):
        sql_text = str(expression).strip().upper()
        return sql_text.startswith("COPY ")

    return False


def is_typed_parameter(obj: Any) -> "TypeGuard[TypedParameter]":
    """Check if an object is a typed parameter.

    Args:
        obj: The object to check

    Returns:
        True if the object is a TypedParameter, False otherwise
    """
    from sqlspec.core.parameters import TypedParameter

    return isinstance(obj, TypedParameter)


def has_expression_and_sql(obj: Any) -> "TypeGuard[HasExpressionAndSQLProtocol]":
    """Check if an object has both 'expression' and 'sql' attributes.

    This is commonly used to identify SQL objects in the builder system.

    Args:
        obj: The object to check

    Returns:
        True if the object has both attributes, False otherwise
    """
    return isinstance(obj, HasExpressionAndSQLProtocol)


def has_expression_and_parameters(obj: Any) -> "TypeGuard[HasExpressionAndParametersProtocol]":
    """Check if an object has both 'expression' and 'parameters' attributes.

    This is used to identify objects that contain both SQL expressions
    and parameter mappings.

    Args:
        obj: The object to check

    Returns:
        True if the object has both attributes, False otherwise
    """
    return isinstance(obj, HasExpressionAndParametersProtocol)


WINDOWS_DRIVE_PATTERN_LENGTH = 3


def is_local_path(uri: str) -> bool:
    r"""Check if URI represents a local filesystem path.

    Detects local paths including:
    - file:// URIs
    - Absolute paths (Unix: /, Windows: C:\\)
    - Relative paths (., .., ~)

    Args:
        uri: URI or path string to check.

    Returns:
        True if uri is a local path, False for remote URIs.

    Examples:
        >>> is_local_path("file:///data/file.txt")
        True
        >>> is_local_path("/absolute/path")
        True
        >>> is_local_path("s3://bucket/key")
        False
    """
    if not uri:
        return False

    if "://" in uri and not uri.startswith("file://"):
        return False

    if uri.startswith("file://"):
        return True

    if uri.startswith("/"):
        return True

    if uri.startswith((".", "~")):
        return True

    if len(uri) >= WINDOWS_DRIVE_PATTERN_LENGTH and uri[1:3] == ":\\":
        return True

    return "/" in uri or "\\" in uri


def supports_arrow_results(obj: Any) -> "TypeGuard[SupportsArrowResults]":
    """Check if object supports Arrow result format.

    Use this type guard to check if a driver or adapter supports returning
    query results in Apache Arrow format via select_to_arrow() method.

    Args:
        obj: Object to check for Arrow results support.

    Returns:
        True if object implements SupportsArrowResults protocol.

    Examples:
        >>> from sqlspec.adapters.duckdb import DuckDBDriver
        >>> driver = DuckDBDriver(...)
        >>> supports_arrow_results(driver)
        True
    """
    return isinstance(obj, SupportsArrowResults)


def has_parameter_builder(obj: Any) -> "TypeGuard[HasParameterBuilderProtocol]":
    """Check if an object has an add_parameter method."""
    return isinstance(obj, HasParameterBuilderProtocol)


def has_expression_attr(obj: Any) -> "TypeGuard[HasExpressionProtocol]":
    """Check if an object has an _expression attribute."""
    return isinstance(obj, HasExpressionProtocol)


def has_sqlglot_expression(obj: Any) -> "TypeGuard[HasSQLGlotExpressionProtocol]":
    """Check if an object has a sqlglot_expression property."""
    return isinstance(obj, HasSQLGlotExpressionProtocol)


def has_statement_config_factory(obj: Any) -> "TypeGuard[HasStatementConfigFactoryProtocol]":
    """Check if an object has a _create_statement_config method.

    Used to check if a config object can create statement configs dynamically.

    Args:
        obj: The object to check.

    Returns:
        True if the object has a _create_statement_config method.
    """
    return isinstance(obj, HasStatementConfigFactoryProtocol)


def has_migration_config(obj: Any) -> "TypeGuard[HasMigrationConfigProtocol]":
    """Check if an object has a migration_config attribute.

    Used to check if a database config supports migrations.

    Args:
        obj: The object to check.

    Returns:
        True if the object has a migration_config attribute.
    """
    return isinstance(obj, HasMigrationConfigProtocol)
