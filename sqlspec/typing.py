# pyright: ignore[reportAttributeAccessIssue]
from collections.abc import Iterator
from functools import lru_cache
from typing import Annotated, Any, Literal, Protocol, TypeAlias, TypedDict, _TypedDict  # pyright: ignore

from typing_extensions import TypeVar

from sqlspec._typing import (
    ALLOYDB_CONNECTOR_INSTALLED,
    ATTRS_INSTALLED,
    CATTRS_INSTALLED,
    CLOUD_SQL_CONNECTOR_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    NANOID_INSTALLED,
    NUMPY_INSTALLED,
    OBSTORE_INSTALLED,
    OPENTELEMETRY_INSTALLED,
    ORJSON_INSTALLED,
    PANDAS_INSTALLED,
    PGVECTOR_INSTALLED,
    POLARS_INSTALLED,
    PROMETHEUS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    UUID_UTILS_INSTALLED,
    ArrowRecordBatch,
    ArrowRecordBatchReader,
    ArrowRecordBatchReaderProtocol,
    ArrowSchema,
    ArrowSchemaProtocol,
    ArrowTable,
    AttrsInstance,
    AttrsInstanceStub,
    BaseModel,
    BaseModelStub,
    Counter,
    DataclassProtocol,
    DTOData,
    Empty,
    EmptyEnum,
    EmptyType,
    FailFast,
    Gauge,
    Histogram,
    NumpyArray,
    PandasDataFrame,
    PolarsDataFrame,
    Span,
    Status,
    StatusCode,
    Struct,
    StructStub,
    Tracer,
    TypeAdapter,
    UnsetType,
    attrs_asdict,
    attrs_define,
    attrs_field,
    attrs_fields,
    attrs_has,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    module_available,
    trace,
)


class DictLike(Protocol):
    """A protocol for objects that behave like a dictionary for reading."""

    def __getitem__(self, key: str) -> Any: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...


PYDANTIC_USE_FAILFAST = False


class ForeignKeyMetadata:
    """Metadata for a foreign key constraint."""

    __slots__ = (
        "column_name",
        "constraint_name",
        "referenced_column",
        "referenced_schema",
        "referenced_table",
        "schema",
        "table_name",
    )

    def __init__(
        self,
        table_name: str,
        column_name: str,
        referenced_table: str,
        referenced_column: str,
        constraint_name: str | None = None,
        schema: str | None = None,
        referenced_schema: str | None = None,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.referenced_table = referenced_table
        self.referenced_column = referenced_column
        self.constraint_name = constraint_name
        self.schema = schema
        self.referenced_schema = referenced_schema

    def __repr__(self) -> str:
        return (
            f"ForeignKeyMetadata(table_name={self.table_name!r}, column_name={self.column_name!r}, "
            f"referenced_table={self.referenced_table!r}, referenced_column={self.referenced_column!r}, "
            f"constraint_name={self.constraint_name!r}, schema={self.schema!r}, "
            f"referenced_schema={self.referenced_schema!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ForeignKeyMetadata):
            return NotImplemented
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.referenced_table == other.referenced_table
            and self.referenced_column == other.referenced_column
            and self.constraint_name == other.constraint_name
            and self.schema == other.schema
            and self.referenced_schema == other.referenced_schema
        )

    def __hash__(self) -> int:
        return hash((
            self.table_name,
            self.column_name,
            self.referenced_table,
            self.referenced_column,
            self.constraint_name,
            self.schema,
            self.referenced_schema,
        ))


class ColumnMetadata(TypedDict, total=False):
    """Metadata for a database column."""

    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: str | bool | None
    column_default: str | None
    ordinal_position: int
    max_length: int
    numeric_precision: int
    numeric_scale: int
    is_primary: bool | int
    is_unique: bool | int
    extra: str


class TableMetadata(TypedDict, total=False):
    """Metadata for a database table."""

    schema_name: str
    table_name: str
    table_type: str
    table_catalog: str
    table_schema: str
    dependency_level: int
    level: int


class IndexMetadata(TypedDict, total=False):
    """Metadata for a database index."""

    schema_name: str
    table_name: str
    index_name: str
    columns: list[str] | str | None
    is_unique: bool | int
    is_primary: bool | int


class VersionInfo:
    """Parsed database version info."""

    def __init__(self, major: int, minor: int = 0, patch: int = 0) -> None:
        """Initialize version info.

        Args:
            major: Major version number
            minor: Minor version number
            patch: Patch version number
        """
        self.major = major
        self.minor = minor
        self.patch = patch

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version as tuple for comparison."""
        return (self.major, self.minor, self.patch)

    def __str__(self) -> str:
        """String representation of version info."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"VersionInfo({self.major}, {self.minor}, {self.patch})"

    def __eq__(self, other: object) -> bool:
        """Check version equality."""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return self.version_tuple == other.version_tuple

    def __lt__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than another."""
        return self.version_tuple < other.version_tuple

    def __le__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than or equal to another."""
        return self.version_tuple <= other.version_tuple

    def __gt__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than another."""
        return self.version_tuple > other.version_tuple

    def __ge__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than or equal to another."""
        return self.version_tuple >= other.version_tuple

    def __hash__(self) -> int:
        """Make VersionInfo hashable based on version tuple."""
        return hash(self.version_tuple)


VersionCacheResult: TypeAlias = "tuple[bool, VersionInfo | None]"
"""Return type for version cache lookup methods.

The tuple contains:

- First element (``bool``): Whether a cache lookup was attempted

  - ``True``: A lookup was attempted; check second element for result
  - ``False``: No lookup was attempted yet; second element is always ``None``

- Second element (``VersionInfo | None``): The cached version info

  - ``VersionInfo``: Successfully detected and cached version
  - ``None``: Version not yet fetched, or detection failed
"""


T = TypeVar("T")
ConnectionT = TypeVar("ConnectionT")
"""Type variable for connection types.

:class:`~sqlspec.typing.ConnectionT`
"""
PoolT = TypeVar("PoolT")
"""Type variable for pool types.

:class:`~sqlspec.typing.PoolT`
"""
SchemaT = TypeVar("SchemaT", default=dict[str, Any])
"""Type variable for schema types (models, TypedDict, dataclasses, etc.).

Unbounded TypeVar for use with schema_type parameter in driver methods.
Supports all schema types including TypedDict which cannot be bounded to a class hierarchy.
"""


SupportedSchemaModel: TypeAlias = (
    DictLike | StructStub | BaseModelStub | DataclassProtocol | AttrsInstanceStub | _TypedDict
)
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol` | :class:`AttrsInstance`
"""
StatementParameters: TypeAlias = "dict[str, object] | list[object] | tuple[object, ...] | object | None"
"""Type alias for statement parameters.

Represents:
- :type:`dict[str, object]`
- :type:`list[object]`
- :type:`tuple[object, ...]`
- :type:`object`
- :type:`None`
"""
ArrowReturnFormat: TypeAlias = Literal["table", "reader", "batch", "batches"]
"""Type alias for Apache Arrow return format options.

Represents:
- :literal:`"table"` - Return PyArrow Table
- :literal:`"reader"` - Return PyArrow RecordBatchReader
- :literal:`"batch"` - Return single PyArrow RecordBatch
- :literal:`"batches"` - Return list of PyArrow RecordBatches
"""


@lru_cache(typed=True)
def get_type_adapter(f: "type[T]") -> Any:
    """Caches and returns a pydantic type adapter.

    Args:
        f: Type to create a type adapter for.

    Returns:
        :class:`pydantic.TypeAdapter`[:class:`typing.TypeVar`[T]]
    """
    if PYDANTIC_USE_FAILFAST:
        return TypeAdapter(Annotated[f, FailFast()])
    return TypeAdapter(f)


__all__ = (
    "ALLOYDB_CONNECTOR_INSTALLED",
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
    "CLOUD_SQL_CONNECTOR_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NANOID_INSTALLED",
    "NUMPY_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "ORJSON_INSTALLED",
    "PANDAS_INSTALLED",
    "PGVECTOR_INSTALLED",
    "POLARS_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "PYDANTIC_USE_FAILFAST",
    "UNSET",
    "UUID_UTILS_INSTALLED",
    "ArrowRecordBatch",
    "ArrowRecordBatchReader",
    "ArrowRecordBatchReaderProtocol",
    "ArrowReturnFormat",
    "ArrowSchema",
    "ArrowSchemaProtocol",
    "ArrowTable",
    "AttrsInstance",
    "BaseModel",
    "ColumnMetadata",
    "ConnectionT",
    "Counter",
    "DTOData",
    "DataclassProtocol",
    "DictLike",
    "Empty",
    "EmptyEnum",
    "EmptyType",
    "FailFast",
    "ForeignKeyMetadata",
    "Gauge",
    "Histogram",
    "IndexMetadata",
    "NumpyArray",
    "PandasDataFrame",
    "PolarsDataFrame",
    "PoolT",
    "SchemaT",
    "Span",
    "StatementParameters",
    "Status",
    "StatusCode",
    "Struct",
    "SupportedSchemaModel",
    "TableMetadata",
    "Tracer",
    "TypeAdapter",
    "UnsetType",
    "VersionCacheResult",
    "VersionInfo",
    "attrs_asdict",
    "attrs_define",
    "attrs_field",
    "attrs_fields",
    "attrs_has",
    "cattrs_structure",
    "cattrs_unstructure",
    "convert",
    "get_type_adapter",
    "module_available",
    "trace",
)
