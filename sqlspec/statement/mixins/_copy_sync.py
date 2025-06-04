import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

if TYPE_CHECKING:
    import pyarrow as pa

    from sqlspec.config import StorageConfig


logger = logging.getLogger(__name__)


class SyncCopyOperationsMixin:
    """Mixin providing sync copy and export operations using storage backends."""

    if TYPE_CHECKING:
        config: Any  # This will be an SQLConfig instance with storage attribute

    def copy_from_path(
        self,
        table_name: str,
        file_path: Union[str, Path],
        *,
        strategy: str = "append",
        format: Optional[str] = None,
        **options: Any,
    ) -> "Any":
        """Copy data from a local file path to a database table.

        Args:
            table_name: Target table name
            file_path: Local file path
            strategy: Strategy for loading (append, truncate, replace)
            format: File format (auto-detected if None)
            **options: Additional copy options

        Returns:
            Result of the copy operation
        """
        file_path = Path(file_path)

        if not file_path.exists():
            msg = f"File not found: {file_path}"
            raise FileNotFoundError(msg)

        # Auto-detect format if not provided
        if format is None:
            format = self._detect_format(file_path.suffix)

        # Use storage config to get copy options
        storage_config: Optional[StorageConfig] = getattr(self.config, "storage", None)
        copy_options = storage_config.get_copy_options(format, **options) if storage_config else options

        return self._copy_from_local_file(table_name, file_path, strategy, format, copy_options)

    def copy_from_uri(
        self,
        table_name: str,
        uri: str,
        *,
        strategy: str = "append",
        format: Optional[str] = None,
        storage_options: Optional[dict[str, Any]] = None,
        **options: Any,
    ) -> "Any":
        """Copy data from a remote URI to a database table.

        Args:
            table_name: Target table name
            uri: Remote URI (s3://, gs://, http://, etc.)
            strategy: Strategy for loading (append, truncate, replace)
            format: File format (auto-detected if None)
            storage_options: Storage backend options (uses config defaults if None)
            **options: Additional copy options

        Returns:
            Result of the copy operation
        """
        # Auto-detect format if not provided
        if format is None:
            format = self._detect_format_from_uri(uri)

        # Use storage config to get both storage options and copy options
        storage_config: Optional[StorageConfig] = getattr(self.config, "storage", None)
        if storage_config:
            # Merge storage options with config defaults
            final_storage_options = storage_config.get_storage_options(uri, **(storage_options or {}))
            copy_options = storage_config.get_copy_options(format, **options)
        else:
            final_storage_options = storage_options or {}
            copy_options = options

        return self._copy_from_remote_uri(table_name, uri, strategy, format, final_storage_options, copy_options)

    def copy_from_arrow(
        self,
        table_name: str,
        arrow_table: "pa.Table",
        *,
        strategy: str = "append",
        **options: Any,
    ) -> "Any":
        """Copy data from a PyArrow table to a database table.

        Args:
            table_name: Target table name
            arrow_table: PyArrow table
            strategy: Strategy for loading (append, truncate, replace)
            **options: Additional copy options

        Returns:
            Result of the copy operation
        """
        # Use storage config to get copy options for arrow format
        storage_config: Optional[StorageConfig] = getattr(self.config, "storage", None)
        copy_options = storage_config.get_copy_options("arrow", **options) if storage_config else options

        return self._copy_from_arrow_table(table_name, arrow_table, strategy, copy_options)

    def export_to_uri(
        self,
        query_or_table: str,
        uri: str,
        *,
        format: str = "parquet",
        storage_options: Optional[dict[str, Any]] = None,
        **options: Any,
    ) -> "Any":
        """Export query results or table data to a remote URI.

        Args:
            query_or_table: SQL query or table name
            uri: Target URI for export
            format: Export format (parquet, csv, etc.)
            storage_options: Storage backend options (uses config defaults if None)
            **options: Additional export options

        Returns:
            Result of the export operation
        """
        # Use storage config to get both storage options and export options
        storage_config: Optional[StorageConfig] = getattr(self.config, "storage", None)
        if storage_config:
            # Merge storage options with config defaults
            final_storage_options = storage_config.get_storage_options(uri, **(storage_options or {}))
            export_options = storage_config.get_export_options(format, **options)
        else:
            final_storage_options = storage_options or {}
            export_options = options

        return self._export_to_remote_uri(query_or_table, uri, format, final_storage_options, export_options)

    def copy_from_storage(
        self,
        table_name: str,
        storage_key: str,
        file_path: str,
        *,
        strategy: str = "append",
        format: Optional[str] = None,
        copy_options: Optional[dict[str, Any]] = None,
    ) -> "Any":
        from sqlspec.storage.registry import storage_registry

        backend = storage_registry.get_backend(storage_key)
        if format is None:
            format = self._detect_format(Path(file_path).suffix)
        # Compose full URI or path for backend
        full_path = f"{backend.base_uri.rstrip('/')}/{file_path.lstrip('/')}"
        return self.copy_from_uri(
            table_name, full_path, strategy=strategy, format=format, storage_options=copy_options or {}
        )

    def export_to_storage(
        self,
        source: Union[str, "Any"],
        storage_key: str,
        file_path: str,
        *,
        format: str = "parquet",
        export_options: Optional[dict[str, Any]] = None,
    ) -> "Any":
        from sqlspec.storage.registry import storage_registry

        backend = storage_registry.get_backend(storage_key)
        full_path = f"{backend.base_uri.rstrip('/')}/{file_path.lstrip('/')}"
        return self.export_to_uri(source, full_path, format=format, storage_options=export_options or {})

    def _detect_format(self, file_extension: str) -> str:
        """Detect file format from extension."""
        extension = file_extension.lower().lstrip(".")

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

        return format_map.get(extension, "csv")  # Default to CSV

    def _detect_format_from_uri(self, uri: str) -> str:
        """Detect file format from URI path."""
        parsed = urlparse(uri)
        path = Path(parsed.path)
        return self._detect_format(path.suffix)

    # Abstract methods that drivers must implement
    def _copy_from_local_file(
        self,
        table_name: str,
        file_path: Path,
        strategy: str,
        format: str,
        options: dict[str, Any],
    ) -> "Any":
        """Driver-specific implementation for local file copy."""
        msg = "Driver must implement _copy_from_local_file"
        raise NotImplementedError(msg)

    def _copy_from_remote_uri(
        self,
        table_name: str,
        uri: str,
        strategy: str,
        format: str,
        storage_options: dict[str, Any],
        copy_options: dict[str, Any],
    ) -> "Any":
        """Driver-specific implementation for remote URI copy."""
        msg = "Driver must implement _copy_from_remote_uri"
        raise NotImplementedError(msg)

    def _copy_from_arrow_table(
        self,
        table_name: str,
        arrow_table: "pa.Table",
        strategy: str,
        options: dict[str, Any],
    ) -> "Any":
        """Driver-specific implementation for Arrow table copy."""
        msg = "Driver must implement _copy_from_arrow_table"
        raise NotImplementedError(msg)

    def _export_to_remote_uri(
        self,
        query_or_table: str,
        uri: str,
        format: str,
        storage_options: dict[str, Any],
        export_options: dict[str, Any],
    ) -> "Any":
        """Driver-specific implementation for export to remote URI."""
        msg = "Driver must implement _export_to_remote_uri"
        raise NotImplementedError(msg)
