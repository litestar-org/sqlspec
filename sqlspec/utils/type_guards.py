"""Type guard functions for runtime type checking in SQLSpec.

This module provides type-safe runtime checks that help the type checker
understand type narrowing, replacing defensive hasattr() and duck typing patterns.

NOTE: Some sqlspec imports are nested inside functions to prevent circular
imports where necessary. This module is imported by core sqlspec modules,
so imports that would create cycles are deferred.
"""

from collections.abc import Iterable, Sequence
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.typing import (
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    DataclassProtocol,
    DTOData,
    Struct,
)

if TYPE_CHECKING:
    from dataclasses import Field

    from sqlglot import exp
    from typing_extensions import TypeGuard

    from sqlspec.protocols import (
        AsyncCloseableConnectionProtocol,
        AsyncCopyCapableConnectionProtocol,
        AsyncPipelineCapableDriverProtocol,
        AsyncTransactionCapableConnectionProtocol,
        AsyncTransactionStateConnectionProtocol,
        BytesConvertibleProtocol,
        DictProtocol,
        FilterAppenderProtocol,
        FilterParameterProtocol,
        HasExpressionsProtocol,
        HasLimitProtocol,
        HasOffsetProtocol,
        HasOrderByProtocol,
        HasRiskLevelProtocol,
        HasSQLMethodProtocol,
        HasWhereProtocol,
        IndexableRow,
        ObjectStoreItemProtocol,
        ParameterValueProtocol,
        SQLBuilderProtocol,
        SyncCloseableConnectionProtocol,
        SyncCopyCapableConnectionProtocol,
        SyncPipelineCapableDriverProtocol,
        SyncTransactionCapableConnectionProtocol,
        SyncTransactionStateConnectionProtocol,
        WithMethodProtocol,
    )
    from sqlspec.statement.builder import Select
    from sqlspec.statement.filters import LimitOffsetFilter, StatementFilter
    from sqlspec.typing import SupportedSchemaModel

__all__ = (
    "can_append_to_statement",
    "can_convert_to_schema",
    "can_extract_parameters",
    "dataclass_to_dict",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "has_bytes_conversion",
    "has_dict_attribute",
    "has_expression_attr",
    "has_expressions",
    "has_parameter_builder",
    "has_parameter_value",
    "has_query_builder_parameters",
    "has_risk_level",
    "has_sql_method",
    "has_sqlglot_expression",
    "has_to_statement",
    "has_with_method",
    "is_async_closeable_connection",
    "is_async_copy_capable",
    "is_async_pipeline_capable_driver",
    "is_async_transaction_capable",
    "is_async_transaction_state_capable",
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
    "is_sync_closeable_connection",
    "is_sync_copy_capable",
    "is_sync_pipeline_capable_driver",
    "is_sync_transaction_capable",
    "is_sync_transaction_state_capable",
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
    from sqlspec.statement.filters import StatementFilter as FilterProtocol

    return isinstance(obj, FilterProtocol)


def is_limit_offset_filter(obj: Any) -> "TypeGuard[LimitOffsetFilter]":
    """Check if an object is a LimitOffsetFilter.

    Args:
        obj: The object to check

    Returns:
        True if the object is a LimitOffsetFilter, False otherwise
    """
    from sqlspec.statement.filters import LimitOffsetFilter

    return isinstance(obj, LimitOffsetFilter)


def is_select_builder(obj: Any) -> "TypeGuard[Select]":
    """Check if an object is a Select.

    Args:
        obj: The object to check

    Returns:
        True if the object is a Select, False otherwise
    """
    from sqlspec.statement.builder import Select

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


def is_iterable_parameters(params: Any) -> "TypeGuard[Sequence[Any]]":
    """Check if parameters are iterable (but not string or dict).

    Args:
        params: The parameters to check

    Returns:
        True if the parameters are iterable, False otherwise
    """
    return isinstance(params, Sequence) and not isinstance(params, (str, bytes, dict))


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


# Type guards migrated from typing.py


def is_dataclass_instance(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    # and that its type is a dataclass.
    return not isinstance(obj, type) and hasattr(type(obj), "__dataclass_fields__")


def is_dataclass(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if isinstance(obj, type) and hasattr(obj, "__dataclass_fields__"):
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
    return is_dataclass(obj) and hasattr(obj, field_name)


def is_dataclass_without_field(obj: Any, field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(obj) and not hasattr(obj, field_name)


def is_pydantic_model(obj: Any) -> "TypeGuard[BaseModel]":
    """Check if a value is a pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return PYDANTIC_INSTALLED and isinstance(obj, BaseModel)


def is_pydantic_model_with_field(obj: Any, field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and hasattr(obj, field_name)


def is_pydantic_model_without_field(obj: Any, field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and not hasattr(obj, field_name)


def is_msgspec_struct(obj: Any) -> "TypeGuard[Struct]":
    """Check if a value is a msgspec struct.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return MSGSPEC_INSTALLED and isinstance(obj, Struct)


def is_msgspec_struct_with_field(obj: Any, field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and hasattr(obj, field_name)


def is_msgspec_struct_without_field(obj: Any, field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and not hasattr(obj, field_name)


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
    """Check if a value is a msgspec Struct or Pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) or is_pydantic_model(obj)


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


def is_dto_data(v: Any) -> "TypeGuard[DTOData[Any]]":
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


def is_sync_transaction_capable(obj: Any) -> "TypeGuard[SyncTransactionCapableConnectionProtocol]":
    """Check if a connection supports sync transactions.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has commit and rollback methods.
    """
    from sqlspec.protocols import SyncTransactionCapableConnectionProtocol

    return isinstance(obj, SyncTransactionCapableConnectionProtocol)


def is_async_transaction_capable(obj: Any) -> "TypeGuard[AsyncTransactionCapableConnectionProtocol]":
    """Check if a connection supports async transactions.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has async commit and rollback methods.
    """
    from sqlspec.protocols import AsyncTransactionCapableConnectionProtocol

    return isinstance(obj, AsyncTransactionCapableConnectionProtocol)


def is_sync_transaction_state_capable(obj: Any) -> "TypeGuard[SyncTransactionStateConnectionProtocol]":
    """Check if a connection can report sync transaction state.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has in_transaction and begin methods.
    """
    from sqlspec.protocols import SyncTransactionStateConnectionProtocol

    return isinstance(obj, SyncTransactionStateConnectionProtocol)


def is_async_transaction_state_capable(obj: Any) -> "TypeGuard[AsyncTransactionStateConnectionProtocol]":
    """Check if a connection can report async transaction state.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has in_transaction and async begin methods.
    """
    from sqlspec.protocols import AsyncTransactionStateConnectionProtocol

    return isinstance(obj, AsyncTransactionStateConnectionProtocol)


def is_sync_closeable_connection(obj: Any) -> "TypeGuard[SyncCloseableConnectionProtocol]":
    """Check if a connection can be closed synchronously.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has a close method.
    """
    from sqlspec.protocols import SyncCloseableConnectionProtocol

    return isinstance(obj, SyncCloseableConnectionProtocol)


def is_async_closeable_connection(obj: Any) -> "TypeGuard[AsyncCloseableConnectionProtocol]":
    """Check if a connection can be closed asynchronously.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has an async close method.
    """
    from sqlspec.protocols import AsyncCloseableConnectionProtocol

    return isinstance(obj, AsyncCloseableConnectionProtocol)


def is_sync_copy_capable(obj: Any) -> "TypeGuard[SyncCopyCapableConnectionProtocol]":
    """Check if a connection supports sync COPY operations.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has copy_from and copy_to methods.
    """
    from sqlspec.protocols import SyncCopyCapableConnectionProtocol

    return isinstance(obj, SyncCopyCapableConnectionProtocol)


def is_async_copy_capable(obj: Any) -> "TypeGuard[AsyncCopyCapableConnectionProtocol]":
    """Check if a connection supports async COPY operations.

    Args:
        obj: Connection object to check.

    Returns:
        True if the connection has async copy_from and copy_to methods.
    """
    from sqlspec.protocols import AsyncCopyCapableConnectionProtocol

    return isinstance(obj, AsyncCopyCapableConnectionProtocol)


def is_sync_pipeline_capable_driver(obj: Any) -> "TypeGuard[SyncPipelineCapableDriverProtocol]":
    """Check if a driver supports sync native pipeline execution.

    Args:
        obj: Driver object to check.

    Returns:
        True if the driver has _execute_pipeline_native method.
    """
    from sqlspec.protocols import SyncPipelineCapableDriverProtocol

    return isinstance(obj, SyncPipelineCapableDriverProtocol)


def is_async_pipeline_capable_driver(obj: Any) -> "TypeGuard[AsyncPipelineCapableDriverProtocol]":
    """Check if a driver supports async native pipeline execution.

    Args:
        obj: Driver object to check.

    Returns:
        True if the driver has async _execute_pipeline_native method.
    """
    from sqlspec.protocols import AsyncPipelineCapableDriverProtocol

    return isinstance(obj, AsyncPipelineCapableDriverProtocol)


# Dataclass utility functions


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

    dataclass_fields: Iterable[Field[Any]] = fields(obj)
    if exclude_none:
        dataclass_fields = (field for field in dataclass_fields if getattr(obj, field.name) is not None)
    if exclude_empty:
        dataclass_fields = (field for field in dataclass_fields if getattr(obj, field.name) is not Empty)
    if include:
        dataclass_fields = (field for field in dataclass_fields if field.name in include)
    if exclude:
        dataclass_fields = (field for field in dataclass_fields if field.name not in exclude)

    return tuple(dataclass_fields)


def extract_dataclass_items(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "Optional[AbstractSet[str]]" = None,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "tuple[tuple[str, Any], ...]":
    """Extract dataclass name, value pairs.

    Unlike the 'asdict' method exports by the stdlib, this function does not pickle values.

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
    """Convert a dataclass to a dictionary.

    This method has important differences to the standard library version:
    - it does not deepcopy values
    - it does not recurse into collections

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


def schema_dump(
    data: "Union[dict[str, Any], DataclassProtocol, Struct, BaseModel]", exclude_unset: bool = True
) -> "dict[str, Any]":
    """Dump a data object to a dictionary.

    Args:
        data:  :type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`
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
        return data.model_dump(exclude_unset=exclude_unset)
    if is_msgspec_struct(data):
        if exclude_unset:
            return {f: val for f in data.__struct_fields__ if (val := getattr(data, f, None)) != UNSET}
        return {f: getattr(data, f, None) for f in data.__struct_fields__}

    if has_dict_attribute(data):
        return data.__dict__
    return cast("dict[str, Any]", data)


# New type guards for hasattr() pattern replacement


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


def has_risk_level(obj: Any) -> "TypeGuard[HasRiskLevelProtocol]":
    """Check if an object has a risk_level attribute."""
    from sqlspec.protocols import HasRiskLevelProtocol

    return isinstance(obj, HasRiskLevelProtocol)


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
    from sqlspec.protocols import SQLBuilderProtocol

    return isinstance(obj, SQLBuilderProtocol)


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
