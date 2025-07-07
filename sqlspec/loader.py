"""SQL file loader module for managing SQL statements from files.

This module provides functionality to load, cache, and manage SQL statements
from files using aiosql-style named queries.
"""

import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.statement.sql import SQL
from sqlspec.storage import storage_registry
from sqlspec.storage.registry import StorageRegistry
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

__all__ = ("SQLFile", "SQLFileLoader")

logger = get_logger("loader")

# Matches: -- name: query_name (supports hyphens and special suffixes)
# We capture the name plus any trailing special characters
QUERY_NAME_PATTERN = re.compile(r"^\s*--\s*name\s*:\s*([\w-]+[^\w\s]*)\s*$", re.MULTILINE | re.IGNORECASE)
TRIM_TRAILING_SPECIAL_CHARS = re.compile(r"[^\w-]+$")
MIN_QUERY_PARTS = 3


def _normalize_query_name(name: str) -> str:
    """Normalize query name to be a valid Python identifier.

    - Strips trailing special characters (like $, !, etc from aiosql)
    - Replaces hyphens with underscores

    Args:
        name: Raw query name from SQL file

    Returns:
        converted query name suitable as Python identifier
    """
    # Strip trailing non-alphanumeric characters (excluding underscore) and replace hyphens
    return TRIM_TRAILING_SPECIAL_CHARS.sub("", name).replace("-", "_")


@dataclass
class SQLFile:
    """Represents a loaded SQL file with metadata.

    This class holds the SQL content along with metadata about the file
    such as its location, timestamps, and content hash.
    """

    content: str
    """The raw SQL content from the file."""

    path: str
    """Path where the SQL file was loaded from."""

    metadata: "dict[str, Any]" = field(default_factory=dict)
    """Optional metadata associated with the SQL file."""

    checksum: str = field(init=False)
    """MD5 checksum of the SQL content for cache invalidation."""

    loaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Timestamp when the file was loaded."""

    def __post_init__(self) -> None:
        """Calculate checksum after initialization."""
        self.checksum = hashlib.md5(self.content.encode(), usedforsecurity=False).hexdigest()


class SQLFileLoader:
    """Loads and parses SQL files with aiosql-style named queries.

    This class provides functionality to load SQL files containing
    named queries (using -- name: syntax) and retrieve them by name.

    Example:
        ```python
        # Initialize loader
        loader = SQLFileLoader()

        # Load SQL files
        loader.load_sql("queries/users.sql")
        loader.load_sql(
            "queries/products.sql", "queries/orders.sql"
        )

        # Get SQL by query name
        sql = loader.get_sql("get_user_by_id", user_id=123)
        ```
    """

    def __init__(self, *, encoding: str = "utf-8", storage_registry: StorageRegistry = storage_registry) -> None:
        """Initialize the SQL file loader.

        Args:
            encoding: Text encoding for reading SQL files.
            storage_registry: Storage registry for handling file URIs.
        """
        self.encoding = encoding
        self.storage_registry = storage_registry
        # Instance-level storage for loaded queries and files
        self._queries: dict[str, str] = {}
        self._files: dict[str, SQLFile] = {}
        self._query_to_file: dict[str, str] = {}  # Maps query name to file path

    def _read_file_content(self, path: Union[str, Path]) -> str:
        """Read file content using storage backend.

        Args:
            path: File path (can be local path or URI).

        Returns:
            File content as string.

        Raises:
            SQLFileParseError: If file cannot be read.
        """
        path_str = str(path)

        try:
            backend = self.storage_registry.get(path)
            return backend.read_text(path_str, encoding=self.encoding)
        except KeyError as e:
            raise SQLFileNotFoundError(path_str) from e
        except Exception as e:
            raise SQLFileParseError(path_str, path_str, e) from e

    @staticmethod
    def _strip_leading_comments(sql_text: str) -> str:
        """Remove leading comment lines from a SQL string."""
        lines = sql_text.strip().split("\n")
        first_sql_line_index = -1
        for i, line in enumerate(lines):
            if line.strip() and not line.strip().startswith("--"):
                first_sql_line_index = i
                break
        if first_sql_line_index == -1:
            return ""  # All comments or empty
        return "\n".join(lines[first_sql_line_index:]).strip()

    @staticmethod
    def _parse_sql_content(content: str, file_path: str) -> dict[str, str]:
        """Parse SQL content and extract named queries."""
        queries: dict[str, str] = {}
        matches = list(QUERY_NAME_PATTERN.finditer(content))
        if not matches:
            raise SQLFileParseError(
                file_path, file_path, ValueError("No named SQL statements found (-- name: query_name)")
            )

        for i, match in enumerate(matches):
            raw_query_name = match.group(1).strip()
            start_pos = match.end()
            end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(content)

            sql_text = content[start_pos:end_pos].strip()
            if not raw_query_name or not sql_text:
                continue

            clean_sql = SQLFileLoader._strip_leading_comments(sql_text)
            if clean_sql:
                query_name = _normalize_query_name(raw_query_name)
                if query_name in queries:
                    raise SQLFileParseError(file_path, file_path, ValueError(f"Duplicate query name: {raw_query_name}"))
                queries[query_name] = clean_sql

        if not queries:
            raise SQLFileParseError(file_path, file_path, ValueError("No valid SQL queries found after parsing"))

        return queries

    def load_sql(self, *paths: Union[str, Path]) -> None:
        """Load SQL files and parse named queries.

        Supports both individual files and directories. When loading directories,
        automatically namespaces queries based on subdirectory structure.

        Args:
            *paths: One or more file paths or directory paths to load.
        """
        correlation_id = CorrelationContext.get()
        start_time = time.perf_counter()

        logger.info("Loading SQL files", extra={"file_count": len(paths), "correlation_id": correlation_id})

        loaded_count = 0
        query_count_before = len(self._queries)

        try:
            for path in paths:
                path_str = str(path)
                if "://" in path_str:
                    self._load_single_file(path, None)
                    loaded_count += 1
                else:
                    path_obj = Path(path)
                    if path_obj.is_dir():
                        loaded_count += self._load_directory(path_obj)
                    else:
                        self._load_single_file(path_obj, None)
                        loaded_count += 1

            duration = time.perf_counter() - start_time
            new_queries = len(self._queries) - query_count_before

            logger.info(
                "Loaded %d SQL files with %d new queries in %.3fms",
                loaded_count,
                new_queries,
                duration * 1000,
                extra={
                    "files_loaded": loaded_count,
                    "new_queries": new_queries,
                    "duration_ms": duration * 1000,
                    "correlation_id": correlation_id,
                },
            )

        except Exception as e:
            duration = time.perf_counter() - start_time
            logger.exception(
                "Failed to load SQL files after %.3fms",
                duration * 1000,
                extra={
                    "error_type": type(e).__name__,
                    "duration_ms": duration * 1000,
                    "correlation_id": correlation_id,
                },
            )
            raise

    def _load_directory(self, dir_path: Path) -> int:
        """Load all SQL files from a directory with namespacing."""
        sql_files = list(dir_path.rglob("*.sql"))
        if not sql_files:
            return 0

        for file_path in sql_files:
            relative_path = file_path.relative_to(dir_path)
            namespace_parts = relative_path.parent.parts
            namespace = ".".join(namespace_parts) if namespace_parts else None
            self._load_single_file(file_path, namespace)
        return len(sql_files)

    def _load_single_file(self, file_path: Union[str, Path], namespace: Optional[str]) -> None:
        """Load a single SQL file with optional namespace.

        Args:
            file_path: Path to the SQL file (can be string for URIs or Path for local files).
            namespace: Optional namespace prefix for queries.
        """
        path_str = str(file_path)

        if path_str in self._files:
            return  # Already loaded

        content = self._read_file_content(file_path)
        sql_file = SQLFile(content=content, path=path_str)
        self._files[path_str] = sql_file

        queries = self._parse_sql_content(content, path_str)
        for name, sql in queries.items():
            namespaced_name = f"{namespace}.{name}" if namespace else name
            if namespaced_name in self._queries:
                existing_file = self._query_to_file.get(namespaced_name, "unknown")
                if existing_file != path_str:
                    raise SQLFileParseError(
                        path_str,
                        path_str,
                        ValueError(f"Query name '{namespaced_name}' already exists in file: {existing_file}"),
                    )
            self._queries[namespaced_name] = sql
            self._query_to_file[namespaced_name] = path_str

    def add_named_sql(self, name: str, sql: str) -> None:
        """Add a named SQL query directly without loading from a file.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.

        Raises:
            ValueError: If query name already exists.
        """
        if name in self._queries:
            existing_source = self._query_to_file.get(name, "<directly added>")
            msg = f"Query name '{name}' already exists (source: {existing_source})"
            raise ValueError(msg)

        self._queries[name] = sql.strip()
        self._query_to_file[name] = "<directly added>"

    def get_sql(self, name: str, parameters: "Optional[Any]" = None, **kwargs: "Any") -> "SQL":
        """Get a SQL object by query name.

        Args:
            name: Name of the query (from -- name: in SQL file).
                  Hyphens in names are automatically converted to underscores.
            parameters: Parameters for the SQL query (aiosql-compatible).
            **kwargs: Additional parameters to pass to the SQL object.

        Returns:
            SQL object ready for execution.

        Raises:
            SQLFileNotFoundError: If query name not found.
        """
        correlation_id = CorrelationContext.get()

        # Normalize query name for lookup
        safe_name = _normalize_query_name(name)

        logger.debug(
            "Retrieving SQL query: %s",
            name,
            extra={
                "query_name": name,
                "safe_name": safe_name,
                "has_parameters": parameters is not None,
                "correlation_id": correlation_id,
            },
        )

        if safe_name not in self._queries:
            available = ", ".join(sorted(self._queries.keys())) if self._queries else "none"
            logger.error(
                "Query not found: %s",
                name,
                extra={
                    "query_name": name,
                    "safe_name": safe_name,
                    "available_queries": len(self._queries),
                    "correlation_id": correlation_id,
                },
            )
            raise SQLFileNotFoundError(name, path=f"Query '{name}' not found. Available queries: {available}")

        sql_kwargs = dict(kwargs)
        if parameters is not None:
            sql_kwargs["parameters"] = parameters

        source_file = self._query_to_file.get(safe_name, "unknown")

        logger.debug(
            "Found query %s from %s",
            name,
            source_file,
            extra={
                "query_name": name,
                "safe_name": safe_name,
                "source_file": source_file,
                "sql_length": len(self._queries[safe_name]),
                "correlation_id": correlation_id,
            },
        )

        return SQL(self._queries[safe_name], **sql_kwargs)

    def get_file(self, path: Union[str, Path]) -> "Optional[SQLFile]":
        """Get a loaded SQLFile object by path.

        Args:
            path: Path of the file.

        Returns:
            SQLFile object if loaded, None otherwise.
        """
        return self._files.get(str(path))

    def get_file_for_query(self, name: str) -> "Optional[SQLFile]":
        """Get the SQLFile object that contains a query.

        Args:
            name: Query name (hyphens are converted to underscores).

        Returns:
            SQLFile object if query exists, None otherwise.
        """
        safe_name = _normalize_query_name(name)
        if safe_name in self._query_to_file:
            file_path = self._query_to_file[safe_name]
            return self._files.get(file_path)
        return None

    def list_queries(self) -> "list[str]":
        """List all available query names.

        Returns:
            Sorted list of query names.
        """
        return sorted(self._queries.keys())

    def list_files(self) -> "list[str]":
        """List all loaded file paths.

        Returns:
            Sorted list of file paths.
        """
        return sorted(self._files.keys())

    def has_query(self, name: str) -> bool:
        """Check if a query exists.

        Args:
            name: Query name to check (hyphens are converted to underscores).

        Returns:
            True if query exists.
        """
        safe_name = _normalize_query_name(name)
        return safe_name in self._queries

    def clear_cache(self) -> None:
        """Clear all cached files and queries."""
        self._files.clear()
        self._queries.clear()
        self._query_to_file.clear()

    def get_query_text(self, name: str) -> str:
        """Get raw SQL text for a query.

        Args:
            name: Query name (hyphens are converted to underscores).

        Returns:
            Raw SQL text.

        Raises:
            SQLFileNotFoundError: If query not found.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            raise SQLFileNotFoundError(name)
        return self._queries[safe_name]
