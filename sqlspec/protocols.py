"""Runtime-checkable protocols for type safety and runtime checks.

This module provides protocols that can be used for static type checking
and runtime isinstance() checks.
"""

from typing import TYPE_CHECKING, Any, Protocol, overload, runtime_checkable

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
    from pathlib import Path

    from sqlglot import exp
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.typing import (
        ArrowRecordBatch,
        ArrowTable,
        ColumnMetadata,
        ForeignKeyMetadata,
        IndexMetadata,
        TableMetadata,
        VersionInfo,
    )

__all__ = (
    "ArrowTableStatsProtocol",
    "AsyncDataDictionaryProtocol",
    "AsyncDeleteProtocol",
    "AsyncReadBytesProtocol",
    "AsyncReadableProtocol",
    "AsyncWriteBytesProtocol",
    "CursorMetadataProtocol",
    "DictProtocol",
    "HasAddListenerProtocol",
    "HasConfigProtocol",
    "HasConnectionConfigProtocol",
    "HasDataProtocol",
    "HasDatabaseUrlAndBindKeyProtocol",
    "HasErrorsProtocol",
    "HasExecuteProtocol",
    "HasExpressionAndParametersProtocol",
    "HasExpressionAndSQLProtocol",
    "HasExpressionProtocol",
    "HasExtensionConfigProtocol",
    "HasFieldNameProtocol",
    "HasFilterAttributesProtocol",
    "HasGetDataProtocol",
    "HasLastRowIdProtocol",
    "HasMigrationConfigProtocol",
    "HasNameProtocol",
    "HasNotifiesProtocol",
    "HasParameterBuilderProtocol",
    "HasRowcountProtocol",
    "HasSQLGlotExpressionProtocol",
    "HasSqlStateProtocol",
    "HasSqliteErrorProtocol",
    "HasStatementConfigFactoryProtocol",
    "HasStatementConfigProtocol",
    "HasStatementTypeProtocol",
    "HasTracerProviderProtocol",
    "HasTypeCodeProtocol",
    "HasTypecodeProtocol",
    "HasTypecodeSizedProtocol",
    "HasValueProtocol",
    "HasWhereProtocol",
    "NotificationProtocol",
    "ObjectStoreProtocol",
    "PipelineCapableProtocol",
    "QueryResultProtocol",
    "ReadableProtocol",
    "SQLBuilderProtocol",
    "SpanAttributeProtocol",
    "SpannerParamTypesProtocol",
    "StatementProtocol",
    "SupportsArrayProtocol",
    "SupportsArrowResults",
    "SupportsCloseProtocol",
    "SupportsDtypeStrProtocol",
    "SupportsJsonTypeProtocol",
    "SyncDataDictionaryProtocol",
    "WithMethodProtocol",
)


@runtime_checkable
class ReadableProtocol(Protocol):
    """Protocol for objects that have a read method (e.g., LOBs)."""

    def read(self, size: "int | None" = None) -> "bytes | str":
        """Read content from the object."""
        ...


@runtime_checkable
class AsyncReadableProtocol(Protocol):
    """Protocol for objects that have an async read method."""

    async def read(self, size: "int | None" = None) -> "bytes | str":
        """Read content from the object."""
        ...


@runtime_checkable
class SupportsArrayProtocol(Protocol):
    """Protocol for NumPy-like arrays."""

    dtype: Any
    shape: tuple[int, ...]
    __array__: Any

    def __len__(self) -> int:
        """Return the length of the array."""
        ...


@runtime_checkable
class CursorMetadataProtocol(Protocol):
    """Protocol for cursor metadata access."""

    @property
    def description(self) -> "Sequence[Any] | None": ...


@runtime_checkable
class NotificationProtocol(Protocol):
    """Protocol for database event notifications."""

    channel: str
    payload: str


@runtime_checkable
class QueryResultProtocol(Protocol):
    """Protocol for query execution results."""

    tag: "str | None"
    status: "str | None"


@runtime_checkable
class HasStatementConfigProtocol(Protocol):
    """Protocol for objects holding statement configuration."""

    statement_config: "StatementConfig"


@runtime_checkable
class PipelineCapableProtocol(Protocol):
    """Protocol for connections supporting pipeline execution."""

    def run_pipeline(self, pipeline: Any, *, continue_on_error: bool = False) -> Any: ...


@runtime_checkable
class HasStatementTypeProtocol(Protocol):
    """Protocol for cursors exposing statement_type metadata."""

    statement_type: "str | None"


@runtime_checkable
class HasTypecodeProtocol(Protocol):
    """Protocol for array-like objects exposing typecode."""

    typecode: Any


@runtime_checkable
class HasTypecodeSizedProtocol(Protocol):
    """Protocol for array-like objects exposing typecode and length."""

    typecode: Any

    def __len__(self) -> int:
        """Return the length of the array-like object."""
        ...


@runtime_checkable
class HasTypeCodeProtocol(Protocol):
    """Protocol for objects exposing type_code metadata."""

    type_code: Any


@runtime_checkable
class HasRowcountProtocol(Protocol):
    """Protocol for cursors exposing rowcount metadata."""

    rowcount: int


@runtime_checkable
class HasLastRowIdProtocol(Protocol):
    """Protocol for cursors exposing lastrowid metadata."""

    lastrowid: int | None


@runtime_checkable
class HasSqlStateProtocol(Protocol):
    """Protocol for exceptions exposing sqlstate."""

    sqlstate: "str | None"


@runtime_checkable
class HasSqliteErrorProtocol(Protocol):
    """Protocol for sqlite errors exposing sqlite error details."""

    sqlite_errorcode: "int | None"
    sqlite_errorname: "str | None"


@runtime_checkable
class HasValueProtocol(Protocol):
    """Protocol for wrapper objects exposing a value attribute."""

    value: Any


@runtime_checkable
class HasErrorsProtocol(Protocol):
    """Protocol for exceptions exposing structured errors."""

    errors: "list[dict[str, Any]] | None"


@runtime_checkable
class HasDataProtocol(Protocol):
    """Protocol for results exposing a data attribute."""

    data: "Sequence[object] | None"


@runtime_checkable
class HasExecuteProtocol(Protocol):
    """Protocol for drivers exposing execute method returning data results."""

    def execute(self, sql: str, *parameters: object, **kwargs: object) -> "HasDataProtocol": ...


@runtime_checkable
class HasNameProtocol(Protocol):
    """Protocol for objects exposing a __name__ attribute."""

    __name__: str


@runtime_checkable
class HasNotifiesProtocol(Protocol):
    """Protocol for asyncpg-like connections exposing notifications."""

    notifies: Any

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a SQL command on the connection."""
        ...


@runtime_checkable
class HasAddListenerProtocol(Protocol):
    """Protocol for asyncpg-like connections exposing add_listener."""

    def add_listener(self, channel: str, callback: Any) -> Any: ...


@runtime_checkable
class SupportsJsonTypeProtocol(Protocol):
    """Protocol for parameter type modules exposing JSON."""

    JSON: Any


@runtime_checkable
class SpannerParamTypesProtocol(SupportsJsonTypeProtocol, Protocol):
    """Protocol for Google Spanner param_types module."""

    BOOL: Any
    INT64: Any
    FLOAT64: Any
    STRING: Any
    BYTES: Any
    TIMESTAMP: Any
    DATE: Any
    Array: "Callable[[Any], Any]"


@runtime_checkable
class SupportsCloseProtocol(Protocol):
    """Protocol for objects exposing close()."""

    def close(self) -> None: ...


@runtime_checkable
class SupportsDtypeStrProtocol(Protocol):
    """Protocol for dtype objects exposing string descriptor."""

    str: str


@runtime_checkable
class ArrowTableStatsProtocol(Protocol):
    """Protocol for Arrow objects exposing row and byte counts."""

    num_rows: int
    nbytes: int


@runtime_checkable
class SpanAttributeProtocol(Protocol):
    """Protocol for span objects supporting attribute mutation."""

    def set_attribute(self, key: str, value: Any) -> None: ...


@runtime_checkable
class HasTracerProviderProtocol(Protocol):
    """Protocol for tracer providers exposing get_tracer."""

    def get_tracer(self, name: str) -> Any: ...


@runtime_checkable
class AsyncReadBytesProtocol(Protocol):
    """Protocol for async read_bytes support."""

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes: ...


@runtime_checkable
class AsyncWriteBytesProtocol(Protocol):
    """Protocol for async write_bytes support."""

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None: ...


@runtime_checkable
class AsyncDeleteProtocol(Protocol):
    """Protocol for async delete support."""

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None: ...


@runtime_checkable
class StatementProtocol(Protocol):
    """Protocol for statement attribute access."""

    @property
    def raw_sql(self) -> "str | None": ...

    @property
    def sql(self) -> str: ...

    @property
    def operation_type(self) -> str: ...


@runtime_checkable
class WithMethodProtocol(Protocol):
    """Protocol for objects with a with_ method (SQLGlot expressions)."""

    def with_(self, *args: Any, **kwargs: Any) -> Any:
        """Add WITH clause to expression."""
        ...


@runtime_checkable
class HasWhereProtocol(Protocol):
    """Protocol for SQL expressions that support WHERE clauses."""

    def where(self, *args: Any, **kwargs: Any) -> Any:
        """Add WHERE clause to expression."""
        ...


@runtime_checkable
class DictProtocol(Protocol):
    """Protocol for objects with a __dict__ attribute."""

    __dict__: dict[str, Any]


@runtime_checkable
class HasConfigProtocol(Protocol):
    """Protocol for wrapper objects exposing a config attribute."""

    config: Any


@runtime_checkable
class HasConnectionConfigProtocol(Protocol):
    """Protocol for configs exposing connection_config mapping."""

    connection_config: "Mapping[str, object]"


@runtime_checkable
class HasDatabaseUrlAndBindKeyProtocol(Protocol):
    """Protocol for configs exposing database_url and bind_key."""

    database_url: str
    bind_key: "str | None"


@runtime_checkable
class HasExtensionConfigProtocol(Protocol):
    """Protocol for configs exposing extension_config mapping."""

    @property
    def extension_config(self) -> "ExtensionConfigs":
        """Return extension configuration mapping."""
        ...


@runtime_checkable
class HasFieldNameProtocol(Protocol):
    """Protocol for objects exposing field_name attribute."""

    field_name: Any


@runtime_checkable
class HasFilterAttributesProtocol(Protocol):
    """Protocol for filter-like objects exposing field attributes."""

    field_name: Any
    operation: Any
    value: Any


@runtime_checkable
class HasGetDataProtocol(Protocol):
    """Protocol for results exposing get_data()."""

    def get_data(self) -> Any: ...


@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Protocol for object storage operations."""

    protocol: str
    backend_type: str

    def __init__(self, uri: str, **kwargs: Any) -> None:
        return

    def read_bytes(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        return b""

    def write_bytes(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        return

    def read_text(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        return ""

    def write_text(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        return

    def exists(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if an object exists."""
        return False

    def delete(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete an object."""
        return

    def copy(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy an object."""
        return

    def move(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move an object."""
        return

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        return []

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        return []

    def is_object(self, path: "str | Path") -> bool:
        """Check if path points to an object."""
        return False

    def is_path(self, path: "str | Path") -> bool:
        """Check if path points to a prefix (directory-like)."""
        return False

    def get_metadata(self, path: "str | Path", **kwargs: Any) -> dict[str, object]:
        """Get object metadata."""
        return {}

    def read_arrow(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        msg = "Arrow reading not implemented"
        raise NotImplementedError(msg)

    def write_arrow(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        msg = "Arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects."""
        msg = "Arrow streaming not implemented"
        raise NotImplementedError(msg)

    def stream_read(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> "Iterator[bytes]":
        """Stream bytes from an object."""
        msg = "Stream reading not implemented"
        raise NotImplementedError(msg)

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Async read bytes from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Async write bytes to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_text_async(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> "AsyncIterator[bytes]":
        """Stream bytes from an object asynchronously."""
        msg = "Async stream reading not implemented"
        raise NotImplementedError(msg)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:
        """Async check if an object exists."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:
        """Async delete an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects with optional prefix."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Async copy an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Async move an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> dict[str, object]:
        """Async get object metadata."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Async read an Arrow table from storage."""
        msg = "Async arrow reading not implemented"
        raise NotImplementedError(msg)

    async def write_arrow_async(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Async write an Arrow table to storage."""
        msg = "Async arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches from matching objects."""
        msg = "Async arrow streaming not implemented"
        raise NotImplementedError(msg)

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Returns:
            True if the backend supports generating signed URLs, False otherwise.
            Only S3, GCS, and Azure backends via obstore support signing.
        """
        return False

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    def sign_sync(self, paths: list[str], expires_in: int = 3600, for_upload: bool = False) -> list[str]: ...

    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) for object(s).

        Args:
            paths: Single object path or list of paths to sign.
            expires_in: URL expiration time in seconds (default: 3600, max: 604800 = 7 days).
            for_upload: Whether the URL is for upload (PUT) vs download (GET).

        Returns:
            Single signed URL string if paths is a string, or list of signed URLs
            if paths is a list. Preserves input type for convenience.

        Raises:
            NotImplementedError: If the backend does not support URL signing.
        """
        msg = "URL signing not supported by this backend"
        raise NotImplementedError(msg)

    @overload
    async def sign_async(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    async def sign_async(self, paths: list[str], expires_in: int = 3600, for_upload: bool = False) -> list[str]: ...

    async def sign_async(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) asynchronously.

        Args:
            paths: Single object path or list of paths to sign.
            expires_in: URL expiration time in seconds (default: 3600, max: 604800 = 7 days).
            for_upload: Whether the URL is for upload (PUT) vs download (GET).

        Returns:
            Single signed URL string if paths is a string, or list of signed URLs
            if paths is a list. Preserves input type for convenience.

        Raises:
            NotImplementedError: If the backend does not support URL signing.
        """
        msg = "URL signing not supported by this backend"
        raise NotImplementedError(msg)


@runtime_checkable
class HasSQLGlotExpressionProtocol(Protocol):
    """Protocol for objects with a sqlglot_expression property."""

    @property
    def sqlglot_expression(self) -> "exp.Expression | None":
        """Return the SQLGlot expression for this object."""
        ...


@runtime_checkable
class HasParameterBuilderProtocol(Protocol):
    """Protocol for objects that can add parameters and build queries."""

    @property
    def parameters(self) -> dict[str, Any]:
        """Return the current parameters dictionary."""
        ...

    def add_parameter(self, value: Any, name: "str | None" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...

    def get_expression(self) -> "exp.Expression | None":
        """Return the underlying SQLGlot expression."""
        ...

    def set_expression(self, expression: "exp.Expression") -> None:
        """Replace the underlying SQLGlot expression."""
        ...

    def build(self, dialect: Any = None) -> Any:
        """Build the SQL query and return a BuiltQuery-like object."""
        ...


@runtime_checkable
class HasExpressionProtocol(Protocol):
    """Protocol for objects with an _expression attribute."""

    _expression: "exp.Expression | None"


@runtime_checkable
class SQLBuilderProtocol(Protocol):
    """Protocol for SQL query builders."""

    _expression: "exp.Expression | None"
    _parameters: dict[str, Any]
    _parameter_counter: int
    _parameter_name_counters: dict[str, int]
    _columns: Any  # Optional attribute for some builders
    _with_ctes: Any  # Optional attribute for some builders
    dialect: Any
    dialect_name: "str | None"

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        ...

    def get_expression(self) -> "exp.Expression | None":
        """Return the current SQLGlot expression."""
        ...

    def add_parameter(self, value: Any, name: "str | None" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate a unique parameter name."""
        ...

    def _create_placeholder(self, value: Any, base_name: str) -> "tuple[exp.Placeholder, str]":
        """Create placeholder expression with bound parameter."""
        ...

    def create_placeholder(self, value: Any, base_name: str) -> "tuple[exp.Placeholder, str]":
        """Create placeholder expression with bound parameter (public)."""
        ...

    def _parameterize_expression(self, expression: "exp.Expression") -> "exp.Expression":
        """Replace literal values in an expression with bound parameters."""
        ...

    def build(self) -> "exp.Expression | Any":
        """Build and return the final expression."""
        ...

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        """Merge parameters from SQL objects into the builder."""
        ...

    def _build_final_expression(self, *, copy: bool = False) -> "exp.Expression":
        """Return the expression with attached CTEs."""
        ...

    def _spawn_like_self(self) -> "Self":
        """Create a new builder with matching configuration."""
        ...

    def set_expression(self, expression: "exp.Expression") -> None:
        """Replace the underlying SQLGlot expression."""
        ...

    def generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate a unique parameter name exposed via public API."""
        ...

    def build_static_expression(
        self,
        expression: "exp.Expression | None" = None,
        parameters: dict[str, Any] | None = None,
        *,
        cache_key: str | None = None,
        expression_factory: "Callable[[], exp.Expression] | None" = None,
        copy: bool = True,
        optimize_expression: bool | None = None,
        dialect: "DialectType | None" = None,
    ) -> Any:
        """Compile a pre-built expression with optional caching and parameters."""
        ...


@runtime_checkable
class SupportsArrowResults(Protocol):
    """Protocol for adapters that support Arrow result format.

    Adapters implementing this protocol can return query results in Apache Arrow
    format via the select_to_arrow() method, enabling zero-copy data transfer and
    efficient integration with data science tools.
    """

    def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> "ArrowTable | ArrowRecordBatch":
        """Execute query and return results as Apache Arrow Table or RecordBatch.

        Args:
            statement: SQL statement to execute.
            *parameters: Query parameters and filters.
            statement_config: Optional statement configuration override.
            return_format: Output format - "table", "reader", or "batches".
            native_only: If True, raise error when native Arrow path unavailable.
            batch_size: Chunk size for streaming modes.
            arrow_schema: Optional target Arrow schema for type casting.
            **kwargs: Additional keyword arguments.

        Returns:
            ArrowResult containing Arrow data.
        """
        ...


@runtime_checkable
class HasExpressionAndSQLProtocol(Protocol):
    """Protocol for objects with both expression and sql attributes (like SQL class)."""

    expression: Any
    sql: str


@runtime_checkable
class HasExpressionAndParametersProtocol(Protocol):
    """Protocol for objects with both expression and parameters attributes."""

    expression: Any
    parameters: Any


@runtime_checkable
class HasStatementConfigFactoryProtocol(Protocol):
    """Protocol for objects that can create a StatementConfig.

    Used for config objects that have a factory method to create statement configs.
    """

    def _create_statement_config(self) -> "StatementConfig":
        """Create a new StatementConfig instance."""
        ...


@runtime_checkable
class HasMigrationConfigProtocol(Protocol):
    """Protocol for database configurations that support migrations.

    Used to check if a config object has migration_config attribute.
    """

    migration_config: "Mapping[str, Any] | None"


class SyncDataDictionaryProtocol(Protocol):
    """Protocol for sync data dictionary implementations."""

    dialect: str

    def get_version(self, driver: Any) -> "VersionInfo | None": ...

    def get_feature_flag(self, driver: Any, feature: str) -> bool: ...

    def get_optimal_type(self, driver: Any, type_category: str) -> str: ...

    def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]": ...

    def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]": ...

    def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]": ...

    def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]": ...

    def list_available_features(self) -> "list[str]": ...


class AsyncDataDictionaryProtocol(Protocol):
    """Protocol for async data dictionary implementations."""

    dialect: str

    async def get_version(self, driver: Any) -> "VersionInfo | None": ...

    async def get_feature_flag(self, driver: Any, feature: str) -> bool: ...

    async def get_optimal_type(self, driver: Any, type_category: str) -> str: ...

    async def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]": ...

    async def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]": ...

    async def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]": ...

    async def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]": ...

    def list_available_features(self) -> "list[str]": ...
