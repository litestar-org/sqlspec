"""Unified storage operations for database drivers.

This module provides the new simplified storage architecture that replaces
the complex web of Arrow, Export, Copy, and ResultConverter mixins with
just two comprehensive mixins: SyncStorageMixin and AsyncStorageMixin.

These mixins provide intelligent routing between native database capabilities
and storage backend operations for optimal performance.
"""

# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import csv
import json
import logging
import tempfile
from abc import ABC
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast
from urllib.parse import urlparse

from sqlspec.exceptions import MissingDependencyError
from sqlspec.statement import SQL, ArrowResult, StatementFilter
from sqlspec.statement.sql import SQLConfig
from sqlspec.storage import storage_registry
from sqlspec.typing import ArrowTable, RowT, StatementParameters
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement import SQLResult, Statement
    from sqlspec.storage.protocol import ObjectStoreProtocol
    from sqlspec.typing import ConnectionT

__all__ = ("AsyncStorageMixin", "SyncStorageMixin")

logger = logging.getLogger(__name__)

# Constants
WINDOWS_PATH_MIN_LENGTH = 3


def _separate_filters_from_parameters(
    parameters: "tuple[Any, ...]",
) -> "tuple[list[StatementFilter], Optional[StatementParameters]]":
    """Separate filters from parameters in positional args."""
    filters: list[StatementFilter] = []
    params: list[Any] = []

    for arg in parameters:
        if isinstance(arg, StatementFilter):
            filters.append(arg)
        else:
            # Everything else is treated as parameters
            params.append(arg)

    # Convert to appropriate parameter format
    if len(params) == 0:
        return filters, None
    if len(params) == 1:
        return filters, params[0]
    return filters, params


class StorageMixinBase(ABC):
    """Base class with common storage functionality."""

    __slots__ = ()

    # These attributes are expected to be provided by the driver class
    config: Any  # Driver config - drivers use 'config' not '_config'
    _connection: Any  # Database connection
    dialect: "DialectType"
    supports_native_parquet_export: "ClassVar[bool]"
    supports_native_parquet_import: "ClassVar[bool]"

    @staticmethod
    def _ensure_pyarrow_installed() -> None:
        """Ensure PyArrow is installed for Arrow operations."""
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            msg = "pyarrow is required for Arrow operations. Install with: pip install pyarrow"
            raise MissingDependencyError(msg)

    @staticmethod
    def _get_storage_backend(uri_or_key: str) -> "ObjectStoreProtocol":
        """Get storage backend by URI or key with intelligent routing."""
        return storage_registry.get(uri_or_key)

    @staticmethod
    def _is_uri(path_or_uri: str) -> bool:
        """Check if input is a URI rather than a relative path."""
        schemes = {"s3", "gs", "gcs", "az", "azure", "abfs", "abfss", "file", "http", "https"}
        if "://" in path_or_uri:
            scheme = path_or_uri.split("://", maxsplit=1)[0].lower()
            return scheme in schemes
        if len(path_or_uri) >= WINDOWS_PATH_MIN_LENGTH and path_or_uri[1:3] == ":\\":
            return True
        return bool(path_or_uri.startswith("/"))

    @staticmethod
    def _detect_format(uri: str) -> str:
        """Detect file format from URI extension."""
        parsed = urlparse(uri)
        path = Path(parsed.path)
        extension = path.suffix.lower().lstrip(".")

        format_map = {
            "csv": "csv",
            "tsv": "csv",
            "txt": "csv",
            "parquet": "parquet",
            "pq": "parquet",
            "json": "json",
            "jsonl": "jsonl",
            "ndjson": "jsonl",
        }

        return format_map.get(extension, "csv")

    def _resolve_backend_and_path(self, uri: str) -> "tuple[ObjectStoreProtocol, str]":
        """Resolve backend and path from URI with Phase 3 URI-first routing.

        Args:
            uri: URI to resolve (e.g., "s3://bucket/path", "file:///local/path")

        Returns:
            Tuple of (backend, path) where path is relative to the backend's base path
        """
        # Convert Path objects to string
        uri = str(uri)
        original_path = uri

        # Convert absolute paths to file:// URIs if needed
        if self._is_uri(uri) and "://" not in uri:
            # It's an absolute path without scheme
            uri = f"file://{uri}"

        backend = self._get_storage_backend(uri)

        # For file:// URIs, return just the path part for the backend
        path = uri[7:] if uri.startswith("file://") else original_path

        return backend, path

    @staticmethod
    def _rows_to_arrow_table(rows: "list[RowT]", columns: "list[str]") -> ArrowTable:
        """Convert rows to Arrow table."""
        import pyarrow as pa

        if not rows:
            # Empty table with column names
            # Create empty arrays for each column
            empty_data: dict[str, list[Any]] = {col: [] for col in columns}
            return pa.table(empty_data)

        # Convert rows to columnar format
        if isinstance(rows[0], dict):
            # Dict rows
            data = {col: [cast("dict[str, Any]", row).get(col) for row in rows] for col in columns}
        else:
            # Tuple/list rows
            data = {col: [cast("tuple[Any, ...]", row)[i] for row in rows] for i, col in enumerate(columns)}

        return pa.table(data)


class SyncStorageMixin(StorageMixinBase):
    """Unified storage operations for synchronous drivers."""

    __slots__ = ()

    def ingest_arrow_table(self, table: "ArrowTable", table_name: str, mode: str = "create", **options: Any) -> int:
        """Ingest an Arrow table into the database.

        This public method provides a consistent entry point and can be used for
        instrumentation, logging, etc., while delegating the actual work to the
        driver-specific `_ingest_arrow_table` implementation.
        """
        return self._ingest_arrow_table(table, table_name, mode, **options)

    def _ingest_arrow_table(self, table: "ArrowTable", table_name: str, mode: str = "create", **options: Any) -> int:
        """Generic fallback for ingesting an Arrow table.

        This implementation writes the Arrow table to a temporary Parquet file
        and then uses the driver's generic `_bulk_load_file` capability.
        Drivers with more efficient, native Arrow ingestion methods should override this.
        """
        import pyarrow.parquet as pq

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            pq.write_table(table, tmp_path)  # pyright: ignore

        try:
            # Use database's bulk load capabilities for Parquet
            return self._bulk_load_file(tmp_path, table_name, "parquet", mode, **options)
        finally:
            tmp_path.unlink(missing_ok=True)

    # ============================================================================
    # Core Arrow Operations
    # ============================================================================

    def fetch_arrow_table(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Fetch query results as Arrow table with intelligent routing.

        Args:
            statement: SQL statement (string, SQL object, or sqlglot Expression)
            *parameters: Mixed parameters and filters
            _connection: Optional connection override
            _config: Optional SQL config override
            **kwargs: Additional options

        Returns:
            ArrowResult wrapping the Arrow table
        """
        self._ensure_pyarrow_installed()

        filters, params = _separate_filters_from_parameters(parameters)
        # Convert to SQL object for processing
        # Use a custom config if transformations will add parameters
        if _config is None:
            _config = self.config

        # If no parameters provided but we have transformations enabled,
        # disable parameter validation entirely to allow transformer-added parameters
        if params is None and _config and _config.enable_transformations:
            # Disable validation entirely for transformer-generated parameters
            _config = replace(_config, strict_mode=False, enable_validation=False)

        # Only pass params if it's not None to avoid adding None as a parameter
        if params is not None:
            sql = SQL(statement, params, *filters, _config=_config, _dialect=self.dialect, **kwargs)
        else:
            sql = SQL(statement, *filters, _config=_config, _dialect=self.dialect, **kwargs)

        return self._fetch_arrow_table(sql, connection=_connection, **kwargs)

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[ConnectionT]" = None, **kwargs: Any) -> "ArrowResult":
        """Generic fallback for Arrow table fetching.

        This method executes a regular query and converts the results to Arrow format.
        Drivers can call this method when they don't have native Arrow support.

        Args:
            sql: SQL object to execute
            connection: Optional connection override
            **kwargs: Additional options (unused in fallback)

        Returns:
            ArrowResult with converted data
        """
        # Check if this SQL object has validation issues due to transformer-generated parameters
        try:
            result = cast("SQLResult", self.execute(sql, _connection=connection))  # type: ignore[attr-defined]
        except Exception:
            # Get the compiled SQL and parameters
            compiled_sql, compiled_params = sql.compile("qmark")

            # Execute directly via the driver's _execute method
            driver_result = self._execute(compiled_sql, compiled_params, sql, connection=connection)  # type: ignore[attr-defined]

            # Wrap the result as a SQLResult
            if "data" in driver_result:
                # It's a SELECT result
                result = self._wrap_select_result(sql, driver_result)  # type: ignore[attr-defined]
            else:
                # It's a DML result
                result = self._wrap_execute_result(sql, driver_result)  # type: ignore[attr-defined]

        data = result.data or []
        columns = result.column_names or []
        arrow_table = self._rows_to_arrow_table(data, columns)
        return ArrowResult(statement=sql, data=arrow_table)

    # ============================================================================
    # Storage Integration Operations
    # ============================================================================

    def export_to_storage(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        destination_uri: str,
        format: "Optional[str]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **options: Any,
    ) -> int:
        """Export query results to storage with intelligent routing.

        Provides instrumentation and delegates to _export_to_storage() for consistent operation.

        Args:
            statement: SQL query to execute and export
            *parameters: Mixed parameters and filters
            destination_uri: URI to export data to
            format: Optional format override (auto-detected from URI if not provided)
            _connection: Optional connection override
            _config: Optional SQL config override
            **options: Additional export options AND named parameters for query

        Returns:
            Number of rows exported
        """
        # Create SQL object with proper parameter handling
        filters, params = _separate_filters_from_parameters(parameters)

        # For storage operations, disable transformations that might add unwanted parameters
        if _config is None:
            _config = self.config
        if _config and _config.enable_transformations:
            from dataclasses import replace

            _config = replace(_config, enable_transformations=False)

        if params is not None:
            sql = SQL(statement, params, *filters, _config=_config, _dialect=self.dialect)
        else:
            sql = SQL(statement, *filters, _config=_config, _dialect=self.dialect)

        return self._export_to_storage(
            sql, destination_uri=destination_uri, format=format, _connection=_connection, **options
        )

    def _export_to_storage(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        destination_uri: str,
        format: "Optional[str]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> int:
        # Convert query to string for format detection
        if hasattr(statement, "to_sql"):  # SQL object
            query_str = cast("SQL", statement).to_sql()
        elif isinstance(statement, str):
            query_str = statement
        else:  # sqlglot Expression
            query_str = str(statement)

        # Auto-detect format if not provided
        # If no format is specified and detection fails (returns "csv" as default),
        # default to "parquet" for export operations as it's the most common use case
        detected_format = self._detect_format(destination_uri)
        if format:
            file_format = format
        elif detected_format == "csv" and not destination_uri.endswith((".csv", ".tsv", ".txt")):
            # Detection returned default "csv" but file doesn't actually have CSV extension
            # Default to parquet for better compatibility with tests and common usage
            file_format = "parquet"
        else:
            file_format = detected_format

        # Special handling for parquet format - if we're exporting to parquet but the
        # destination doesn't have .parquet extension, add it to ensure compatibility
        # with pyarrow.parquet.read_table() which requires the extension
        if file_format == "parquet" and not destination_uri.endswith(".parquet"):
            destination_uri = f"{destination_uri}.parquet"

        # Use storage backend - resolve AFTER modifying destination_uri
        backend, path = self._resolve_backend_and_path(destination_uri)

        # Try native database export first
        if file_format == "parquet" and self.supports_native_parquet_export:
            # If we have a SQL object with parameters, compile it first
            if hasattr(statement, "compile") and hasattr(statement, "parameters") and statement.parameters:
                _compiled_sql, _compiled_params = statement.compile(placeholder_style=self.default_parameter_style)  # type: ignore[attr-defined]
            else:
                try:
                    return self._export_native(query_str, destination_uri, file_format, **kwargs)
                except NotImplementedError:
                    # Fall through to use storage backend
                    pass

        if file_format == "parquet":
            # Use Arrow for efficient transfer - if statement is already a SQL object, use it directly
            if hasattr(statement, "compile"):  # It's already a SQL object from export_to_storage
                # For parquet export via Arrow, just use the SQL object directly
                sql_obj = cast("SQL", statement)
                # Pass connection parameter correctly
                arrow_result = self._fetch_arrow_table(sql_obj, connection=_connection, **kwargs)
            else:
                # Create SQL object if it's still a string
                arrow_result = self.fetch_arrow_table(statement, *parameters, _connection=_connection, _config=_config)

            # ArrowResult.data is never None according to the type definition
            arrow_table = arrow_result.data
            num_rows = arrow_table.num_rows
            backend.write_arrow(path, arrow_table, **kwargs)
            return num_rows
        # Pass the SQL object if available, otherwise create one
        if isinstance(statement, str):
            sql_obj = SQL(statement, _config=_config, _dialect=self.dialect)
        else:
            sql_obj = cast("SQL", statement)
        return self._export_via_backend(sql_obj, backend, path, file_format, **kwargs)

    def import_from_storage(
        self, source_uri: str, table_name: str, format: "Optional[str]" = None, mode: str = "create", **options: Any
    ) -> int:
        """Import data from storage with intelligent routing.

        Provides instrumentation and delegates to _import_from_storage() for consistent operation.

        Args:
            source_uri: URI to import data from
            table_name: Target table name
            format: Optional format override (auto-detected from URI if not provided)
            mode: Import mode ('create', 'append', 'replace')
            **options: Additional import options

        Returns:
            Number of rows imported
        """
        return self._import_from_storage(source_uri, table_name, format, mode, **options)

    def _import_from_storage(
        self, source_uri: str, table_name: str, format: "Optional[str]" = None, mode: str = "create", **options: Any
    ) -> int:
        """Protected method for import operation implementation.

        Args:
            source_uri: URI to import data from
            table_name: Target table name
            format: Optional format override (auto-detected from URI if not provided)
            mode: Import mode ('create', 'append', 'replace')
            **options: Additional import options

        Returns:
            Number of rows imported
        """
        # Auto-detect format if not provided
        file_format = format or self._detect_format(source_uri)

        # Try native database import first
        if file_format == "parquet" and self.supports_native_parquet_import:
            return self._import_native(source_uri, table_name, file_format, mode, **options)

        # Use storage backend
        backend, path = self._resolve_backend_and_path(source_uri)

        if file_format == "parquet":
            try:
                # Use Arrow for efficient transfer
                arrow_table = backend.read_arrow(path, **options)
                return self.ingest_arrow_table(arrow_table, table_name, mode=mode)
            except AttributeError:
                pass

        # Use traditional import through temporary file
        return self._import_via_backend(backend, path, table_name, file_format, mode, **options)

    # ============================================================================
    # Database-Specific Implementation Hooks
    # ============================================================================

    def _read_parquet_native(
        self, source_uri: str, columns: "Optional[list[str]]" = None, **options: Any
    ) -> "SQLResult":
        """Database-specific native Parquet reading. Override in drivers."""
        msg = "Driver should implement _read_parquet_native"
        raise NotImplementedError(msg)

    def _write_parquet_native(self, data: Union[str, ArrowTable], destination_uri: str, **options: Any) -> None:
        """Database-specific native Parquet writing. Override in drivers."""
        msg = "Driver should implement _write_parquet_native"
        raise NotImplementedError(msg)

    def _export_native(self, query: str, destination_uri: str, format: str, **options: Any) -> int:
        """Database-specific native export. Override in drivers."""
        msg = "Driver should implement _export_native"
        raise NotImplementedError(msg)

    def _import_native(self, source_uri: str, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific native import. Override in drivers."""
        msg = "Driver should implement _import_native"
        raise NotImplementedError(msg)

    def _export_via_backend(
        self, sql_obj: "SQL", backend: "ObjectStoreProtocol", path: str, format: str, **options: Any
    ) -> int:
        """Export via storage backend using temporary file."""

        # Execute query and get results - use the SQL object directly
        try:
            result = cast("SQLResult", self.execute(sql_obj))  # type: ignore[attr-defined]
        except Exception:
            # Fall back to direct execution
            compiled_sql, compiled_params = sql_obj.compile("qmark")
            driver_result = self._execute(compiled_sql, compiled_params, sql_obj)  # type: ignore[attr-defined]
            if "data" in driver_result:
                result = self._wrap_select_result(sql_obj, driver_result)  # type: ignore[attr-defined]
            else:
                result = self._wrap_execute_result(sql_obj, driver_result)  # type: ignore[attr-defined]

        # For parquet format, convert through Arrow
        if format == "parquet":
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            backend.write_arrow(path, arrow_table, **options)
            return len(result.data or [])

        # Convert to appropriate format and write to backend
        compression = options.get("compression")

        # Create temp file with appropriate suffix
        suffix = f".{format}"
        if compression == "gzip":
            suffix += ".gz"

        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)

        # Handle compression and writing
        if compression == "gzip":
            import gzip

            with gzip.open(tmp_path, "wt", encoding="utf-8") as file_to_write:
                if format == "csv":
                    self._write_csv(result, file_to_write, **options)
                elif format == "json":
                    self._write_json(result, file_to_write, **options)
                else:
                    msg = f"Unsupported format for backend export: {format}"
                    raise ValueError(msg)
        else:
            with tmp_path.open("w", encoding="utf-8") as file_to_write:
                if format == "csv":
                    self._write_csv(result, file_to_write, **options)
                elif format == "json":
                    self._write_json(result, file_to_write, **options)
                else:
                    msg = f"Unsupported format for backend export: {format}"
                    raise ValueError(msg)

        try:
            # Upload to storage backend
            # Adjust path if compression was used
            final_path = path
            if compression == "gzip" and not path.endswith(".gz"):
                final_path = path + ".gz"

            backend.write_bytes(final_path, tmp_path.read_bytes())
            return result.rows_affected or len(result.data or [])
        finally:
            tmp_path.unlink(missing_ok=True)

    def _import_via_backend(
        self, backend: "ObjectStoreProtocol", path: str, table_name: str, format: str, mode: str, **options: Any
    ) -> int:
        """Import via storage backend using temporary file."""
        # Download from storage backend
        data = backend.read_bytes(path)

        with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{format}", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)

        try:
            # Use database's bulk load capabilities
            return self._bulk_load_file(tmp_path, table_name, format, mode, **options)
        finally:
            tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _write_csv(result: "SQLResult", file: Any, **options: Any) -> None:
        """Write result to CSV file."""
        # Remove options that csv.writer doesn't understand
        csv_options = options.copy()
        csv_options.pop("compression", None)  # Handle compression separately
        csv_options.pop("partition_by", None)  # Not applicable to CSV

        writer = csv.writer(file, **csv_options)  # TODO: anything better?
        if result.column_names:
            writer.writerow(result.column_names)
        if result.data:
            # Handle dict rows by extracting values in column order
            if result.data and isinstance(result.data[0], dict):
                rows = []
                for row_dict in result.data:
                    # Extract values in the same order as column_names
                    row_values = [row_dict.get(col) for col in result.column_names or []]
                    rows.append(row_values)
                writer.writerows(rows)
            else:
                writer.writerows(result.data)

    @staticmethod
    def _write_json(result: "SQLResult", file: Any, **options: Any) -> None:
        """Write result to JSON file."""

        if result.data and result.column_names:
            # Check if data is already in dict format
            if result.data and isinstance(result.data[0], dict):
                # Data is already dictionaries, use as-is
                rows = result.data
            else:
                # Convert tuples/lists to list of dicts
                rows = [dict(zip(result.column_names, row)) for row in result.data]
            json.dump(rows, file, **options)  # TODO: use sqlspec.utils.serializer
        else:
            json.dump([], file)  # TODO: use sqlspec.utils.serializer

    def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific bulk load implementation. Override in drivers."""
        msg = "Driver should implement _bulk_load_file"
        raise NotImplementedError(msg)


class AsyncStorageMixin(StorageMixinBase):
    """Unified storage operations for asynchronous drivers."""

    __slots__ = ()

    async def ingest_arrow_table(
        self, table: "ArrowTable", table_name: str, mode: str = "create", **options: Any
    ) -> int:
        """Ingest an Arrow table into the database asynchronously.

        This public method provides a consistent entry point and can be used for
        instrumentation, logging, etc., while delegating the actual work to the
        driver-specific `_ingest_arrow_table` implementation.
        """
        self._ensure_pyarrow_installed()
        return await self._ingest_arrow_table(table, table_name, mode, **options)

    async def _ingest_arrow_table(
        self, table: "ArrowTable", table_name: str, mode: str = "create", **options: Any
    ) -> int:
        """Generic async fallback for ingesting an Arrow table.

        This implementation writes the Arrow table to a temporary Parquet file
        and then uses the driver's generic `_bulk_load_file` capability.
        Drivers with more efficient, native Arrow ingestion methods should override this.
        """
        import pyarrow.parquet as pq

        # Use an async-friendly way to handle the temporary file if possible,
        # but for simplicity, standard tempfile is acceptable here as it's a fallback.
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            await async_(pq.write_table)(table, tmp_path)  # pyright: ignore

            try:
                # Use database's async bulk load capabilities for Parquet
                return await self._bulk_load_file(tmp_path, table_name, "parquet", mode, **options)
            finally:
                tmp_path.unlink(missing_ok=True)

    # ============================================================================
    # Core Arrow Operations (Async)
    # ============================================================================

    async def fetch_arrow_table(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Async fetch query results as Arrow table with intelligent routing.

        Args:
            statement: SQL statement (string, SQL object, or sqlglot Expression)
            *parameters: Mixed parameters and filters
            _connection: Optional connection override
            _config: Optional SQL config override
            **kwargs: Additional options

        Returns:
            ArrowResult wrapping the Arrow table
        """
        self._ensure_pyarrow_installed()

        filters, params = _separate_filters_from_parameters(parameters)
        # Convert to SQL object for processing
        # Use a custom config if transformations will add parameters
        if _config is None:
            _config = self.config

        # If no parameters provided but we have transformations enabled,
        # disable parameter validation entirely to allow transformer-added parameters
        if params is None and _config and _config.enable_transformations:
            from dataclasses import replace

            # Disable validation entirely for transformer-generated parameters
            _config = replace(_config, strict_mode=False, enable_validation=False)

        # Only pass params if it's not None to avoid adding None as a parameter
        if params is not None:
            sql = SQL(statement, params, *filters, _config=_config, _dialect=self.dialect, **kwargs)
        else:
            sql = SQL(statement, *filters, _config=_config, _dialect=self.dialect, **kwargs)

        # Delegate to protected method that drivers can override
        return await self._fetch_arrow_table(sql, connection=_connection, **kwargs)

    async def _fetch_arrow_table(
        self, sql: SQL, connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> "ArrowResult":
        """Generic async fallback for Arrow table fetching.

        This method executes a regular query and converts the results to Arrow format.
        Drivers should override this method to provide native Arrow support if available.
        If a driver has partial native support, it can call `super()._fetch_arrow_table(...)`
        to use this fallback implementation.

        Args:
            sql: SQL object to execute
            connection: Optional connection override
            **kwargs: Additional options (unused in fallback)

        Returns:
            ArrowResult with converted data
        """
        # Execute regular query
        result = await self.execute(sql, _connection=connection)  # type: ignore[attr-defined]

        # Convert to Arrow table
        arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])

        return ArrowResult(statement=sql, data=arrow_table)

    async def export_to_storage(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        destination_uri: str,
        format: "Optional[str]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **options: Any,
    ) -> int:
        # Create SQL object with proper parameter handling
        filters, params = _separate_filters_from_parameters(parameters)

        # For storage operations, disable transformations that might add unwanted parameters
        if _config is None:
            _config = self.config
        if _config and _config.enable_transformations:
            from dataclasses import replace

            _config = replace(_config, enable_transformations=False)

        if params is not None:
            sql = SQL(statement, params, *filters, _config=_config, _dialect=self.dialect, **options)
        else:
            sql = SQL(statement, *filters, _config=_config, _dialect=self.dialect, **options)

        return await self._export_to_storage(sql, destination_uri, format, connection=_connection, **options)

    async def _export_to_storage(
        self,
        query: "SQL",
        destination_uri: str,
        format: "Optional[str]" = None,
        connection: "Optional[ConnectionT]" = None,
        **options: Any,
    ) -> int:
        """Protected async method for export operation implementation.

        Args:
            query: SQL query to execute and export
            destination_uri: URI to export data to
            format: Optional format override (auto-detected from URI if not provided)
            connection: Optional connection override
            **options: Additional export options

        Returns:
            Number of rows exported
        """
        # Auto-detect format if not provided
        # If no format is specified and detection fails (returns "csv" as default),
        # default to "parquet" for export operations as it's the most common use case
        detected_format = self._detect_format(destination_uri)
        if format:
            file_format = format
        elif detected_format == "csv" and not destination_uri.endswith((".csv", ".tsv", ".txt")):
            # Detection returned default "csv" but file doesn't actually have CSV extension
            # Default to parquet for better compatibility with tests and common usage
            file_format = "parquet"
        else:
            file_format = detected_format

        # Special handling for parquet format - if we're exporting to parquet but the
        # destination doesn't have .parquet extension, add it to ensure compatibility
        # with pyarrow.parquet.read_table() which requires the extension
        if file_format == "parquet" and not destination_uri.endswith(".parquet"):
            destination_uri = f"{destination_uri}.parquet"

        # Use storage backend - resolve AFTER modifying destination_uri
        backend, path = self._resolve_backend_and_path(destination_uri)

        # Try native database export first
        if file_format == "parquet" and self.supports_native_parquet_export:
            return await self._export_native(query.as_script().sql, destination_uri, file_format, **options)

        if file_format == "parquet":
            # For parquet export via Arrow, we need to ensure no unwanted parameter transformations
            # If the query already has parameters from transformations, create a fresh SQL object
            if hasattr(query, "parameters") and query.parameters and hasattr(query, "_raw_sql"):
                # Create fresh SQL object from raw SQL without transformations
                fresh_sql = SQL(
                    query._raw_sql,
                    _config=replace(self.config, enable_transformations=False)
                    if self.config
                    else SQLConfig(enable_transformations=False),
                    _dialect=self.dialect,
                )
                arrow_result = await self._fetch_arrow_table(fresh_sql, connection=connection, **options)
            else:
                # query is already a SQL object, call _fetch_arrow_table directly
                arrow_result = await self._fetch_arrow_table(query, connection=connection, **options)
            arrow_table = arrow_result.data
            if arrow_table is not None:
                await backend.write_arrow_async(path, arrow_table, **options)
                return arrow_table.num_rows
            return 0

        return await self._export_via_backend(query, backend, path, file_format, **options)

    async def import_from_storage(
        self, source_uri: str, table_name: str, format: "Optional[str]" = None, mode: str = "create", **options: Any
    ) -> int:
        """Async import data from storage with intelligent routing.

        Provides instrumentation and delegates to _import_from_storage() for consistent operation.

        Args:
            source_uri: URI to import data from
            table_name: Target table name
            format: Optional format override (auto-detected from URI if not provided)
            mode: Import mode ('create', 'append', 'replace')
            **options: Additional import options

        Returns:
            Number of rows imported
        """
        return await self._import_from_storage(source_uri, table_name, format, mode, **options)

    async def _import_from_storage(
        self, source_uri: str, table_name: str, format: "Optional[str]" = None, mode: str = "create", **options: Any
    ) -> int:
        """Protected async method for import operation implementation.

        Args:
            source_uri: URI to import data from
            table_name: Target table name
            format: Optional format override (auto-detected from URI if not provided)
            mode: Import mode ('create', 'append', 'replace')
            **options: Additional import options

        Returns:
            Number of rows imported
        """
        file_format = format or self._detect_format(source_uri)
        backend, path = self._resolve_backend_and_path(source_uri)

        if file_format == "parquet":
            arrow_table = await backend.read_arrow_async(path, **options)
            return await self.ingest_arrow_table(arrow_table, table_name, mode=mode)

        return await self._import_via_backend(backend, path, table_name, file_format, mode, **options)

    # ============================================================================
    # Async Database-Specific Implementation Hooks
    # ============================================================================

    async def _export_native(self, query: str, destination_uri: str, format: str, **options: Any) -> int:
        """Async database-specific native export."""
        msg = "Driver should implement _export_native"
        raise NotImplementedError(msg)

    async def _import_native(self, source_uri: str, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Async database-specific native import."""
        msg = "Driver should implement _import_native"
        raise NotImplementedError(msg)

    async def _export_via_backend(
        self, sql_obj: "SQL", backend: "ObjectStoreProtocol", path: str, format: str, **options: Any
    ) -> int:
        """Async export via storage backend."""

        # Execute query and get results - use the SQL object directly
        try:
            result = await self.execute(sql_obj)  # type: ignore[attr-defined]
        except Exception:
            # Fall back to direct execution
            compiled_sql, compiled_params = sql_obj.compile("qmark")
            driver_result = await self._execute(compiled_sql, compiled_params, sql_obj)  # type: ignore[attr-defined]
            if "data" in driver_result:
                result = self._wrap_select_result(sql_obj, driver_result)  # type: ignore[attr-defined]
            else:
                result = self._wrap_execute_result(sql_obj, driver_result)  # type: ignore[attr-defined]

        # For parquet format, convert through Arrow
        if format == "parquet":
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            await backend.write_arrow_async(path, arrow_table, **options)
            return len(result.data or [])

        # Convert to appropriate format and write to backend
        with tempfile.NamedTemporaryFile(mode="w", suffix=f".{format}", delete=False, encoding="utf-8") as tmp:
            if format == "csv":
                self._write_csv(result, tmp, **options)
            elif format == "json":
                self._write_json(result, tmp, **options)
            else:
                msg = f"Unsupported format for backend export: {format}"
                raise ValueError(msg)

            tmp_path = Path(tmp.name)

        try:
            # Upload to storage backend (async if supported)
            await backend.write_bytes_async(path, tmp_path.read_bytes())
            return result.rows_affected or len(result.data or [])
        finally:
            tmp_path.unlink(missing_ok=True)

    async def _import_via_backend(
        self, backend: "ObjectStoreProtocol", path: str, table_name: str, format: str, mode: str, **options: Any
    ) -> int:
        """Async import via storage backend."""
        # Download from storage backend (async if supported)
        data = await backend.read_bytes_async(path)

        with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{format}", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)

        try:
            return await self._bulk_load_file(tmp_path, table_name, format, mode, **options)
        finally:
            tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _write_csv(result: "SQLResult", file: Any, **options: Any) -> None:
        """Reuse sync implementation."""

        writer = csv.writer(file, **options)
        if result.column_names:
            writer.writerow(result.column_names)
        if result.data:
            # Handle dict rows by extracting values in column order
            if result.data and isinstance(result.data[0], dict):
                rows = []
                for row_dict in result.data:
                    # Extract values in the same order as column_names
                    row_values = [row_dict.get(col) for col in result.column_names or []]
                    rows.append(row_values)
                writer.writerows(rows)
            else:
                writer.writerows(result.data)

    @staticmethod
    def _write_json(result: "SQLResult", file: Any, **options: Any) -> None:
        """Reuse sync implementation."""

        if result.data and result.column_names:
            # Check if data is already in dict format
            if result.data and isinstance(result.data[0], dict):
                # Data is already dictionaries, use as-is
                rows = result.data
            else:
                # Convert tuples/lists to list of dicts
                rows = [dict(zip(result.column_names, row)) for row in result.data]
            json.dump(rows, file, **options)
        else:
            json.dump([], file)

    async def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Async database-specific bulk load implementation."""
        msg = "Driver should implement _bulk_load_file"
        raise NotImplementedError(msg)
