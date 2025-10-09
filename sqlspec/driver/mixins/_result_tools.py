"""Result handling and schema conversion mixins for database drivers."""

from typing import TYPE_CHECKING, Any, overload

from mypy_extensions import trait

from sqlspec.utils.schema import to_schema

if TYPE_CHECKING:
    from sqlspec.typing import SchemaT

__all__ = ("ToSchemaMixin",)


@trait
class ToSchemaMixin:
    """Mixin providing data transformation methods for various schema types."""

    __slots__ = ()

    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: None = None) -> "list[dict[str, Any]]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[SchemaT]") -> "SchemaT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: None = None) -> "dict[str, Any]": ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[SchemaT]") -> Any: ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: None = None) -> Any: ...

    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[Any] | None" = None) -> Any:
        """Convert data to a specified schema type.

        Supports transformation to various schema types including:
        - TypedDict
        - dataclasses
        - msgspec Structs
        - Pydantic models
        - attrs classes

        Args:
            data: Input data to convert (dict, list of dicts, or other)
            schema_type: Target schema type for conversion. If None, returns data unchanged.

        Returns:
            Converted data in the specified schema type, or original data if schema_type is None

        Raises:
            SQLSpecError: If schema_type is not a supported type
        """
        return to_schema(data, schema_type=schema_type)
