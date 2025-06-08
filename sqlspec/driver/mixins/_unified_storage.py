"""Unified storage operations for database drivers.

This module provides the new simplified storage architecture that replaces
the complex web of Arrow, Export, Copy, and ResultConverter mixins with
just two comprehensive mixins: SyncStorageMixin and AsyncStorageMixin.

These mixins provide intelligent routing between native database capabilities
and storage backend operations for optimal performance.
"""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import logging
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from urllib.parse import urlparse

from sqlspec.exceptions import MissingDependencyError, wrap_exceptions
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQL
from sqlspec.storage import storage_registry
from sqlspec.typing import ArrowTable, RowT, SQLParameterType

if TYPE_CHECKING:
    from sqlspec.config import StorageConfig
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQLConfig, Statement
    from sqlspec.storage.protocol import ObjectStoreProtocol
    from sqlspec.typing import ConnectionT

__all__ = ("AsyncStorageMixin", "SyncStorageMixin")

logger = logging.getLogger(__name__)

# Constants
WINDOWS_PATH_MIN_LENGTH = 3


class StorageMixinBase:
    """Base class with common storage functionality."""

    # These attributes are expected to be provided by the driver class
    config: Any  # Driver config - drivers use 'config' not '_config'
    _connection: Any  # Database connection

    @staticmethod
    def _ensure_pyarrow_installed() -> None:
        """Ensure PyArrow is installed for Arrow operations."""
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            msg = "pyarrow is required for Arrow operations. Install with: pip install pyarrow"
            raise MissingDependencyError(msg)

    def _get_storage_config(self) -> "Optional[StorageConfig]":
        """Get storage configuration from driver config."""
        with wrap_exceptions(wrap_exceptions=True, suppress=(AttributeError, TypeError)):
            return self.config.storage  # type: ignore[no-any-return]
        return None

    @staticmethod
    def _get_storage_backend(uri_or_key: str) -> "ObjectStoreProtocol":
        """Get storage backend by URI or key with intelligent routing."""
        return storage_registry.get(uri_or_key)

    @staticmethod
    def _is_uri(path_or_uri: str) -> bool:
        """Check if input is a URI rather than a relative path."""
        cloud_schemes = {"s3", "gs", "gcs", "az", "azure", "abfs", "abfss", "file", "http", "https"}

        if "://" in path_or_uri:
            scheme = path_or_uri.split("://", maxsplit=1)[0].lower()
            return scheme in cloud_schemes

        # Windows drive letters (C:\path)
        if len(path_or_uri) >= WINDOWS_PATH_MIN_LENGTH and path_or_uri[1:3] == ":\\":
            return True

        # Unix absolute paths starting with /
        return bool(path_or_uri.startswith("/"))

    # TODO: we need to add more dunder methods to the mixin like __native_arrow__, __native_parquet__, etc.  The driver itself should have something that also says what kind of operations it supports for the type
    def _has_native_capability(self, operation: str, uri: str = "", format: str = "") -> bool:
        """Check if database has native capability for operation."""
        # Use driver capability flags for accurate detection
        supports_parquet = getattr(self, "__supports_parquet__", False)
        supports_arrow = getattr(self, "__supports_arrow__", False)

        # Check for Parquet-specific operations
        if operation in {"parquet", "import", "export"} and format.lower() == "parquet" and supports_parquet:
            # Additional URI-specific checks for databases with conditional support
            driver_class_name = self.__class__.__name__

            # BigQuery: only supports GCS URIs natively
            if "BigQuery" in driver_class_name:
                return uri.startswith("gs://")

            # For now, require explicit native method implementation
            # Future: drivers can implement _export_native, _import_native etc.
            return False

        # Check for Arrow operations
        if operation == "arrow":
            return supports_arrow

        # For now, only return True if the driver actually implements the methods
        # Future: drivers can override this method for native capabilities
        return False

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


class SyncStorageMixin(StorageMixinBase):
    """Unified storage operations for synchronous drivers."""

    # ============================================================================
    # Core Arrow Operations
    # ============================================================================

    def fetch_arrow_table(
        self,
        statement: "Statement",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Fetch query results as Arrow table with intelligent routing.

        Args:
            statement: SQL statement (string, SQL object, or sqlglot Expression)
            parameters: Optional query parameters
            *filters: Statement filters to apply
            connection: Optional connection override
            config: Optional SQL config override
            **kwargs: Additional options

        Returns:
            ArrowResult wrapping the Arrow table
        """
        self._ensure_pyarrow_installed()

        # Convert to SQL object for processing
        if isinstance(statement, str):
            sql_obj = SQL(statement, parameters=parameters, config=config or self.config)
        elif hasattr(statement, "to_sql"):  # SQL object
            sql_obj = statement
            if parameters is not None:
                # Create a new SQL object with the provided parameters
                sql_obj = SQL(statement.sql, parameters=parameters, config=config or sql_obj._config)
        else:  # sqlglot Expression
            sql_obj = SQL(statement, parameters=parameters, config=config or self.config)

        # Apply filters
        for filter_func in filters:
            sql_obj = filter_func(sql_obj)

        # Use static parameter conversion (like execute_script)
        final_sql = sql_obj.to_sql()

        # Use provided connection or default
        conn = connection or self._connection

        with wrap_exceptions():
            # Try native Arrow support first (fastest)
            try:
                # DuckDB-style native Arrow
                arrow_table = conn.execute(final_sql).arrow()  # type: ignore[no-any-return]
                return ArrowResult(statement=sql_obj, data=arrow_table)
            except AttributeError:
                pass

            try:
                # Driver-specific Arrow method
                arrow_table = conn.fetch_arrow_table(final_sql)  # type: ignore[no-any-return]
                return ArrowResult(statement=sql_obj, data=arrow_table)
            except AttributeError:
                pass

            # Fallback: execute regular query and convert to Arrow
            result = self.execute(sql_obj, connection=connection)  # type: ignore[attr-defined]
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            return ArrowResult(statement=sql_obj, data=arrow_table)

    def ingest_arrow_table(self, table: ArrowTable, target_table: str, mode: str = "create", **options: Any) -> int:
        """Ingest Arrow table into database with intelligent routing."""
        self._ensure_pyarrow_installed()

        with wrap_exceptions():
            # Try native Arrow ingestion first
            try:
                # DuckDB-style registration
                temp_name = f"_arrow_temp_{uuid.uuid4().hex[:8]}"
                self._connection.register(temp_name, table)

                try:
                    if mode == "create":
                        # Use sqlglot to build safe SQL
                        from sqlglot import exp

                        create = exp.Create(
                            this=exp.Table(this=exp.Identifier(this=target_table)),
                            expression=exp.Select().from_(temp_name).select("*"),
                            kind="TABLE",
                        )
                        sql = create.sql()
                    elif mode == "append":
                        # Use sqlglot to build safe SQL
                        from sqlglot import exp

                        insert = exp.Insert(
                            this=exp.Table(this=exp.Identifier(this=target_table)),
                            expression=exp.Select().from_(temp_name).select("*"),
                        )
                        sql = insert.sql()
                    else:
                        msg = f"Unsupported mode: {mode}"
                        raise ValueError(msg)

                    from sqlspec.statement.sql import SQL

                    result = self.execute(SQL(sql))  # type: ignore[attr-defined]
                    return result.rows_affected or table.num_rows
                finally:
                    self._connection.unregister(temp_name)
            except AttributeError:
                pass

            try:
                # ADBC native ingestion
                return self._connection.adbc_ingest(target_table, table, mode=mode)  # type: ignore[no-any-return]
            except AttributeError:
                pass

            # Convert to temporary file and use bulk load
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                import pyarrow.parquet as pq

                pq.write_table(table, tmp.name)
                tmp_path = Path(tmp.name)

            try:
                return self.import_from_storage(
                    f"file://{tmp_path}", target_table, format="parquet", mode=mode, **options
                )
            finally:
                tmp_path.unlink(missing_ok=True)

    # ============================================================================
    # Native Database Operations
    # ============================================================================

    def read_parquet_direct(
        self, source_uri: str, columns: "Optional[list[str]]" = None, **options: Any
    ) -> "SQLResult":
        """Read Parquet file directly using database's native capabilities."""
        if not self._has_native_capability("parquet", source_uri, "parquet"):
            msg = (
                f"{self.__class__.__name__} does not support direct Parquet reading. Use import_from_storage() instead."
            )
            raise NotImplementedError(msg)

        # Database-specific implementations
        return self._read_parquet_native(source_uri, columns, **options)

    def write_parquet_direct(self, data: Union[str, ArrowTable], destination_uri: str, **options: Any) -> None:
        """Write Parquet file directly using database's native capabilities."""
        if not self._has_native_capability("parquet", destination_uri, "parquet"):
            msg = f"{self.__class__.__name__} does not support direct Parquet writing. Use export_to_storage() instead."
            raise NotImplementedError(msg)

        # Database-specific implementations
        self._write_parquet_native(data, destination_uri, **options)

    # ============================================================================
    # Storage Integration Operations
    # ============================================================================

    def export_to_storage(
        self,
        query: "Union[str, Statement]",
        destination_uri: str,
        format: "Optional[str]" = None,
        storage_key: "Optional[str]" = None,
        **options: Any,
    ) -> int:
        """Export query results to storage with intelligent routing."""
        # Convert query to string

        if hasattr(query, "to_sql"):  # SQL object
            query_str = query.to_sql()
        elif isinstance(query, str):
            query_str = query
        else:  # sqlglot Expression
            query_str = str(query)

        # Log storage operation for instrumentation
        logger.info(
            "Exporting query results to storage",
            extra={
                "operation": "export_to_storage",
                "destination_uri": destination_uri,
                "format": format,
                "storage_key": storage_key,
            },
        )

        # Auto-detect format if not provided
        file_format = format or self._detect_format(destination_uri)

        # Try native database export first
        if self._has_native_capability("export", destination_uri, file_format):
            return self._export_native(query_str, destination_uri, file_format, **options)

        # Use storage backend
        backend, path = self._resolve_backend_and_path(destination_uri, storage_key)

        with wrap_exceptions(suppress=(AttributeError,)):
            if file_format == "parquet":
                # Use Arrow for efficient transfer
                arrow_result = self.fetch_arrow_table(query_str)
                arrow_table = arrow_result.data
                backend.write_arrow(path, arrow_table, **options)
                return arrow_table.num_rows

        # Use traditional export through temporary file
        return self._export_via_backend(query_str, backend, path, file_format, **options)

    def import_from_storage(
        self,
        source_uri: str,
        table_name: str,
        format: "Optional[str]" = None,
        storage_key: "Optional[str]" = None,
        mode: str = "create",
        **options: Any,
    ) -> int:
        """Import data from storage with intelligent routing."""
        # Log storage operation for instrumentation
        logger.info(
            "Importing data from storage",
            extra={
                "operation": "import_from_storage",
                "source_uri": source_uri,
                "table_name": table_name,
                "format": format,
                "storage_key": storage_key,
                "mode": mode,
            },
        )

        # Auto-detect format if not provided
        file_format = format or self._detect_format(source_uri)

        # Try native database import first
        if self._has_native_capability("import", source_uri, file_format):
            return self._import_native(source_uri, table_name, file_format, mode, **options)

        # Use storage backend
        backend, path = self._resolve_backend_and_path(source_uri, storage_key)

        with wrap_exceptions():
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
    # Helper Methods
    # ============================================================================

    def _resolve_backend_and_path(
        self, uri_or_key: str, storage_key: "Optional[str]" = None
    ) -> "tuple[ObjectStoreProtocol, str]":
        """Resolve backend and path from URI or key.

        Args:
            uri_or_key: Either a URI (e.g., "s3://bucket/path") or a path to use with storage_key
            storage_key: Optional storage backend key to use (overrides URI-based resolution)

        Returns:
            Tuple of (backend, path) where path is relative to the backend's base path
        """
        if storage_key:
            # Use the specified storage key to get backend
            backend = self._get_storage_backend(storage_key)
            # uri_or_key is the path in this case
            path = uri_or_key
        else:
            # Treat uri_or_key as either a registered key or a URI
            original_path = uri_or_key

            # Convert absolute paths to file:// URIs if needed
            if self._is_uri(uri_or_key) and "://" not in uri_or_key:
                # It's an absolute path without scheme
                uri_or_key = f"file://{uri_or_key}"

            backend = self._get_storage_backend(uri_or_key)

            # For file:// URIs, return just the path part for the backend
            path = uri_or_key[7:] if uri_or_key.startswith("file://") else original_path

        return backend, path

    @staticmethod
    def _rows_to_arrow_table(rows: "list[RowT]", columns: "list[str]") -> ArrowTable:
        """Convert rows to Arrow table."""
        import pyarrow as pa

        if not rows:
            # Empty table with column names
            # Create empty arrays for each column
            empty_data = {col: [] for col in columns}
            return pa.table(empty_data)

        # Convert rows to columnar format
        if isinstance(rows[0], dict):
            # Dict rows
            data = {col: [cast("dict", row).get(col) for row in rows] for col in columns}
        else:
            # Tuple/list rows
            data = {col: [cast("tuple[Any, ...]", row)[i] for row in rows] for i, col in enumerate(columns)}

        return pa.table(data)

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
        self, query: str, backend: "ObjectStoreProtocol", path: str, format: str, **options: Any
    ) -> int:
        """Export via storage backend using temporary file."""
        from sqlspec.statement.sql import SQL

        # Execute query and get results
        result = self.execute(SQL(query))  # type: ignore[attr-defined]

        # For parquet format, convert through Arrow
        if format == "parquet":
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            backend.write_arrow(path, arrow_table, **options)
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
            # Upload to storage backend
            backend.write_bytes(path, tmp_path.read_bytes())
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
        import csv

        writer = csv.writer(file, **options)
        if result.column_names:
            writer.writerow(result.column_names)
        if result.data:
            writer.writerows(result.data)

    @staticmethod
    def _write_json(result: "SQLResult", file: Any, **options: Any) -> None:
        """Write result to JSON file."""
        import json

        if result.data and result.column_names:
            # Convert to list of dicts
            rows = [dict(zip(result.column_names, row)) for row in result.data]
            json.dump(rows, file, **options)
        else:
            json.dump([], file)

    def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific bulk load implementation. Override in drivers."""
        msg = "Driver should implement _bulk_load_file"
        raise NotImplementedError(msg)


class AsyncStorageMixin(StorageMixinBase):
    """Unified storage operations for asynchronous drivers."""

    # ============================================================================
    # Core Arrow Operations (Async)
    # ============================================================================

    async def fetch_arrow_table(
        self,
        statement: "Statement",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Async fetch query results as Arrow table with intelligent routing.

        Args:
            statement: SQL statement (string, SQL object, or sqlglot Expression)
            parameters: Optional query parameters
            *filters: Statement filters to apply
            connection: Optional connection override
            config: Optional SQL config override
            **kwargs: Additional options

        Returns:
            ArrowResult wrapping the Arrow table
        """
        self._ensure_pyarrow_installed()

        from sqlspec.statement.result import ArrowResult
        from sqlspec.statement.sql import SQL

        # Convert to SQL object for processing
        if isinstance(statement, str):
            sql_obj = SQL(statement, parameters=parameters, config=config or self.config)
        elif hasattr(statement, "to_sql"):  # SQL object
            sql_obj = statement
            if parameters is not None:
                # Create a new SQL object with the provided parameters
                sql_obj = SQL(statement.sql, parameters=parameters, config=config or sql_obj._config)
        else:  # sqlglot Expression
            sql_obj = SQL(statement, parameters=parameters, config=config or self.config)

        # Apply filters
        for filter_func in filters:
            sql_obj = filter_func(sql_obj)

        # Use static parameter conversion (like execute_script)
        final_sql = sql_obj.to_sql()

        # Use provided connection or default
        conn = connection or self._connection

        with wrap_exceptions():
            # Try native Arrow support first
            try:
                result = await conn.execute(final_sql)
                arrow_table = result.arrow()  # type: ignore[no-any-return]
                return ArrowResult(statement=sql_obj, data=arrow_table)
            except AttributeError:
                pass

            try:
                arrow_table = await conn.fetch_arrow_table(final_sql)  # type: ignore[no-any-return]
                return ArrowResult(statement=sql_obj, data=arrow_table)
            except AttributeError:
                pass

            # Fallback: execute regular query and convert to Arrow
            result = await self.execute(sql_obj)  # type: ignore[attr-defined]
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            return ArrowResult(statement=sql_obj, data=arrow_table)

    async def ingest_arrow_table(
        self, table: ArrowTable, target_table: str, mode: str = "create", **options: Any
    ) -> int:
        """Async version of ingest_arrow_table."""
        self._ensure_pyarrow_installed()

        with wrap_exceptions():
            # Try native Arrow ingestion
            try:
                temp_name = f"_arrow_temp_{uuid.uuid4().hex[:8]}"
                await self._connection.register(temp_name, table)

                try:
                    if mode == "create":
                        # Use sqlglot to build safe SQL
                        from sqlglot import exp

                        create = exp.Create(
                            this=exp.Table(this=exp.Identifier(this=target_table)),
                            expression=exp.Select().from_(temp_name).select("*"),
                            kind="TABLE",
                        )
                        sql = create.sql()
                    elif mode == "append":
                        # Use sqlglot to build safe SQL
                        from sqlglot import exp

                        insert = exp.Insert(
                            this=exp.Table(this=exp.Identifier(this=target_table)),
                            expression=exp.Select().from_(temp_name).select("*"),
                        )
                        sql = insert.sql()
                    else:
                        msg = f"Unsupported mode: {mode}"
                        raise ValueError(msg)

                    from sqlspec.statement.sql import SQL

                    result = await self.execute(SQL(sql))  # type: ignore[attr-defined]
                    return result.rows_affected or table.num_rows
                finally:
                    await self._connection.unregister(temp_name)
            except AttributeError:
                pass

            try:
                return await self._connection.adbc_ingest(target_table, table, mode=mode)  # type: ignore[no-any-return]
            except AttributeError:
                pass

            # Use temporary file
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                import pyarrow.parquet as pq

                pq.write_table(table, tmp.name)
                tmp_path = Path(tmp.name)

            try:
                return await self.import_from_storage(
                    f"file://{tmp_path}", target_table, format="parquet", mode=mode, **options
                )
            finally:
                tmp_path.unlink(missing_ok=True)

    # ============================================================================
    # Storage Integration Operations (Async)
    # ============================================================================

    async def export_to_storage(
        self,
        query: "Union[str, Statement]",
        destination_uri: str,
        format: "Optional[str]" = None,
        storage_key: "Optional[str]" = None,
        **options: Any,
    ) -> int:
        """Async version of export_to_storage."""
        # Convert query to string
        if hasattr(query, "to_sql"):  # SQL object
            query_str = query.to_sql()
        elif isinstance(query, str):
            query_str = query
        else:  # sqlglot Expression
            query_str = str(query)

        file_format = format or self._detect_format(destination_uri)

        # Try native database export first
        if self._has_native_capability("export", destination_uri, file_format):
            return await self._export_native(query_str, destination_uri, file_format, **options)

        # Use storage backend
        backend, path = self._resolve_backend_and_path(destination_uri, storage_key)

        with wrap_exceptions(suppress=(AttributeError,)):
            if file_format == "parquet":
                arrow_result = await self.fetch_arrow_table(query_str)
                arrow_table = arrow_result.data
                await backend.write_arrow_async(path, arrow_table, **options)
                return arrow_table.num_rows

        return await self._export_via_backend(query_str, backend, path, file_format, **options)

    async def import_from_storage(
        self,
        source_uri: str,
        table_name: str,
        format: "Optional[str]" = None,
        storage_key: "Optional[str]" = None,
        mode: str = "create",
        **options: Any,
    ) -> int:
        """Async version of import_from_storage."""

        file_format = format or self._detect_format(source_uri)

        # Try native database import first
        if self._has_native_capability("import", source_uri, file_format):
            return await self._import_native(source_uri, table_name, file_format, mode, **options)

        # Use storage backend
        backend, path = self._resolve_backend_and_path(source_uri, storage_key)

        with wrap_exceptions():
            if file_format == "parquet":
                arrow_table = await backend.read_arrow_async(path, **options)  # type: ignore[misc]

                if arrow_table is not None:
                    return await self.ingest_arrow_table(arrow_table, table_name, mode=mode)

        return await self._import_via_backend(backend, path, table_name, file_format, mode, **options)

    # ============================================================================
    # Async Helper Methods
    # ============================================================================

    def _resolve_backend_and_path(
        self, uri_or_key: str, storage_key: "Optional[str]" = None
    ) -> "tuple[ObjectStoreProtocol, str]":
        """Resolve backend and path from URI or key.

        Args:
            uri_or_key: Either a URI (e.g., "s3://bucket/path") or a path to use with storage_key
            storage_key: Optional storage backend key to use (overrides URI-based resolution)

        Returns:
            Tuple of (backend, path) where path is relative to the backend's base path
        """
        if storage_key:
            # Use the specified storage key to get backend
            backend = self._get_storage_backend(storage_key)
            # uri_or_key is the path in this case
            path = uri_or_key
        else:
            # Treat uri_or_key as either a registered key or a URI
            original_path = uri_or_key

            # Convert absolute paths to file:// URIs if needed
            if self._is_uri(uri_or_key) and "://" not in uri_or_key:
                # It's an absolute path without scheme
                uri_or_key = f"file://{uri_or_key}"

            backend = self._get_storage_backend(uri_or_key)

            # For file:// URIs, return just the path part for the backend
            path = uri_or_key[7:] if uri_or_key.startswith("file://") else original_path

        return backend, path

    @staticmethod
    def _rows_to_arrow_table(rows: "list[RowT]", columns: "list[str]") -> ArrowTable:
        """Convert rows to Arrow table - reuse base implementation."""
        import pyarrow as pa

        if not rows:
            # Empty table with column names
            # Create empty arrays for each column
            empty_data = {col: [] for col in columns}
            return pa.table(empty_data)

        # Convert rows to columnar format
        if isinstance(rows[0], dict):
            # Dict rows
            data = {col: [cast("dict", row).get(col) for row in rows] for col in columns}
        else:
            # Tuple/list rows
            data = {col: [cast("tuple[Any, ...]", row)[i] for row in rows] for i, col in enumerate(columns)}

        return pa.table(data)

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
        self, query: str, backend: "ObjectStoreProtocol", path: str, format: str, **options: Any
    ) -> int:
        """Async export via storage backend."""
        from sqlspec.statement.sql import SQL

        # Execute query and get results
        result = await self.execute(SQL(query))  # type: ignore[attr-defined]

        # For parquet format, convert through Arrow
        if format == "parquet":
            arrow_table = self._rows_to_arrow_table(result.data or [], result.column_names or [])
            with wrap_exceptions():
                try:
                    await backend.write_arrow_async(path, arrow_table, **options)  # type: ignore[misc]
                except AttributeError:
                    backend.write_arrow(path, arrow_table, **options)
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
            with wrap_exceptions():
                await backend.write_bytes_async(path, tmp_path.read_bytes())
            return result.rows_affected or len(result.data or [])
        finally:
            tmp_path.unlink(missing_ok=True)

    async def _import_via_backend(
        self, backend: "ObjectStoreProtocol", path: str, table_name: str, format: str, mode: str, **options: Any
    ) -> int:
        """Async import via storage backend."""
        # Download from storage backend (async if supported)
        with wrap_exceptions():
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
        import csv

        writer = csv.writer(file, **options)
        if result.column_names:
            writer.writerow(result.column_names)
        if result.data:
            writer.writerows(result.data)

    @staticmethod
    def _write_json(result: "SQLResult", file: Any, **options: Any) -> None:
        """Reuse sync implementation."""
        import json  # TODO: this should use the sqlspec._serialization encoders.  We should add another win if the json isn't dump to the right format (str vs binary).

        if result.data and result.column_names:
            # Convert to list of dicts
            rows = [dict(zip(result.column_names, row)) for row in result.data]
            json.dump(rows, file, **options)
        else:
            json.dump([], file)

    async def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Async database-specific bulk load implementation."""
        msg = "Driver should implement _bulk_load_file"
        raise NotImplementedError(msg)
