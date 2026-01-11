"""SQL file loader for managing SQL statements from files.

Provides functionality to load, cache, and manage SQL statements
from files using named SQL queries.

SQL files declare query metadata with comment directives like ``-- name: query_name`` (hyphens and suffixes allowed)
and ``-- dialect: dialect_name``.
"""

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import unquote, urlparse

from sqlspec.core import SQL, get_cache, get_cache_config
from sqlspec.exceptions import (
    FileNotFoundInStorageError,
    SQLFileNotFoundError,
    SQLFileParseError,
    StorageOperationFailedError,
)
from sqlspec.storage.registry import storage_registry as default_storage_registry
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.text import slugify

if TYPE_CHECKING:
    from sqlspec.observability import ObservabilityRuntime
    from sqlspec.storage.registry import StorageRegistry

__all__ = ("NamedStatement", "SQLFile", "SQLFileCacheEntry", "SQLFileLoader")

logger = get_logger("sqlspec.loader")

QUERY_NAME_PATTERN = re.compile(r"^\s*--\s*name\s*:\s*([\w-]+[^\w\s]*)\s*$", re.MULTILINE | re.IGNORECASE)
TRIM_SPECIAL_CHARS = re.compile(r"[^\w.-]")

DIALECT_PATTERN = re.compile(r"^\s*--\s*dialect\s*:\s*(?P<dialect>[a-zA-Z0-9_]+)\s*$", re.IGNORECASE | re.MULTILINE)


DIALECT_ALIASES: Final = {
    "postgresql": "postgres",
    "pg": "postgres",
    "pgplsql": "postgres",
    "plsql": "oracle",
    "oracledb": "oracle",
    "tsql": "mssql",
}

MIN_QUERY_PARTS: Final = 3


def _normalize_query_name(name: str) -> str:
    """Normalize query name to be a valid Python identifier.

    Convert hyphens to underscores, preserve dots for namespacing,
    and remove invalid characters.

    Args:
        name: Raw query name from SQL file.

    Returns:
        Normalized query name suitable as Python identifier.
    """
    parts = name.split(".")
    normalized_parts = []

    for part in parts:
        normalized_part = slugify(part, separator="_")
        normalized_parts.append(normalized_part)

    return ".".join(normalized_parts)


def _normalize_dialect(dialect: str) -> str:
    """Normalize dialect name with aliases.

    Args:
        dialect: Raw dialect name from SQL file.

    Returns:
        Normalized dialect name.
    """
    normalized = dialect.lower().strip()
    return DIALECT_ALIASES.get(normalized, normalized)


class NamedStatement:
    """Represents a parsed SQL statement with metadata.

    Contains individual SQL statements extracted from files with their
    normalized names, SQL content, optional dialect specifications,
    and line position for error reporting.
    """

    __slots__ = ("dialect", "name", "sql", "start_line")

    def __init__(self, name: str, sql: str, dialect: "str | None" = None, start_line: int = 0) -> None:
        self.name = name
        self.sql = sql
        self.dialect = dialect
        self.start_line = start_line


class SQLFile:
    """Represents a loaded SQL file with metadata.

    Contains SQL content and associated metadata including file location,
    timestamps, and content hash.
    """

    __slots__ = ("checksum", "content", "loaded_at", "metadata", "path")

    def __init__(
        self, content: str, path: str, metadata: "dict[str, Any] | None" = None, loaded_at: "datetime | None" = None
    ) -> None:
        """Initialize SQLFile.

        Args:
            content: Raw SQL content from the file.
            path: Path where the SQL file was loaded from.
            metadata: Optional metadata associated with the SQL file.
            loaded_at: Timestamp when the file was loaded.
        """
        self.content = content
        self.path = path
        self.metadata = metadata or {}
        self.loaded_at = loaded_at or datetime.now(timezone.utc)
        self.checksum = hashlib.md5(self.content.encode(), usedforsecurity=False).hexdigest()


class SQLFileCacheEntry:
    """Cached SQL file with parsed statements.

    Stored in the file cache to avoid re-parsing SQL files when their
    content hasn't changed.
    """

    __slots__ = ("parsed_statements", "sql_file", "statement_names")

    def __init__(self, sql_file: SQLFile, parsed_statements: "dict[str, NamedStatement]") -> None:
        """Initialize cached SQL file.

        Args:
            sql_file: Original SQLFile with content and metadata.
            parsed_statements: Named statements from the file.
        """
        self.sql_file = sql_file
        self.parsed_statements = parsed_statements
        self.statement_names = tuple(parsed_statements.keys())


class SQLFileLoader:
    """Loads and parses SQL files with named SQL queries.

    Loads SQL files containing named queries (using -- name: syntax)
    and retrieves them by name.
    """

    __slots__ = ("_files", "_queries", "_query_to_file", "_runtime", "encoding", "storage_registry")

    def __init__(
        self,
        *,
        encoding: str = "utf-8",
        storage_registry: "StorageRegistry | None" = None,
        runtime: "ObservabilityRuntime | None" = None,
    ) -> None:
        """Initialize the SQL file loader.

        Args:
            encoding: Text encoding for reading SQL files.
            storage_registry: Storage registry for handling file URIs.
            runtime: Observability runtime for instrumentation.
        """
        self.encoding = encoding

        self.storage_registry = storage_registry or default_storage_registry
        self._queries: dict[str, NamedStatement] = {}
        self._files: dict[str, SQLFile] = {}
        self._query_to_file: dict[str, str] = {}
        self._runtime = runtime

    def set_observability_runtime(self, runtime: "ObservabilityRuntime | None") -> None:
        """Attach an observability runtime used for instrumentation."""

        self._runtime = runtime

    def _metric(self, name: str, amount: float = 1.0) -> None:
        if self._runtime is not None:
            self._runtime.increment_metric(name, amount)

    def _raise_file_not_found(self, path: str) -> None:
        """Raise SQLFileNotFoundError for nonexistent file.

        Args:
            path: File path that was not found.

        Raises:
            SQLFileNotFoundError: Always raised.
        """
        raise SQLFileNotFoundError(path)

    def _generate_file_cache_key(self, path: str | Path) -> str:
        """Generate cache key for a file path.

        Args:
            path: File path to generate key for.

        Returns:
            Cache key string for the file.
        """
        path_str = str(path)
        path_hash = hashlib.md5(path_str.encode(), usedforsecurity=False).hexdigest()
        return f"file:{path_hash[:16]}"

    def _calculate_file_checksum(self, path: str | Path) -> str:
        """Calculate checksum for file content validation.

        Args:
            path: File path to calculate checksum for.

        Returns:
            MD5 checksum of file content.

        Raises:
            SQLFileParseError: If file cannot be read.
        """
        try:
            return hashlib.md5(self._read_file_content(path).encode(), usedforsecurity=False).hexdigest()
        except Exception as e:
            raise SQLFileParseError(str(path), str(path), e) from e

    def _is_file_unchanged(self, path: str | Path, cached_file: SQLFileCacheEntry) -> bool:
        """Check if file has changed since caching.

        Args:
            path: File path to check.
            cached_file: Cached file data.

        Returns:
            True if file is unchanged, False otherwise.
        """
        try:
            current_checksum = self._calculate_file_checksum(path)
        except Exception:
            return False
        else:
            return current_checksum == cached_file.sql_file.checksum

    def _read_file_content(self, path: str | Path) -> str:
        """Read file content using storage backend.

        Args:
            path: File path (can be local path or URI).

        Returns:
            File content as string.

        Raises:
            SQLFileNotFoundError: If file does not exist.
            SQLFileParseError: If file cannot be read or parsed.

        Notes:
            File:// URIs are normalized before delegation to the backend, including trimming Windows-style leading slashes so filenames resolve correctly.
        """
        path_str = str(path)

        try:
            backend = self.storage_registry.get(path)
            if path_str.startswith("file://"):
                parsed = urlparse(path_str)
                file_path = unquote(parsed.path)
                if file_path and len(file_path) > 2 and file_path[2] == ":":  # noqa: PLR2004
                    file_path = file_path[1:]
                filename = Path(file_path).name
                return backend.read_text(filename, encoding=self.encoding)
            return backend.read_text(path_str, encoding=self.encoding)
        except KeyError as e:
            raise SQLFileNotFoundError(path_str) from e
        except FileNotFoundInStorageError as e:
            raise SQLFileNotFoundError(path_str) from e
        except FileNotFoundError as e:
            raise SQLFileNotFoundError(path_str) from e
        except StorageOperationFailedError as e:
            raise SQLFileParseError(path_str, path_str, e) from e
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
            return ""
        return "\n".join(lines[first_sql_line_index:]).strip()

    @staticmethod
    def _parse_sql_content(content: str, file_path: str) -> "dict[str, NamedStatement]":
        """Parse SQL content and extract named statements with dialect specifications.

        Files without any named statement markers are gracefully skipped by returning
        an empty dictionary. The caller is responsible for handling empty results
        appropriately.

        Args:
            content: Raw SQL file content to parse.
            file_path: File path for error reporting.

        Returns:
            Dictionary mapping normalized statement names to NamedStatement objects.
            Empty dict if no named statement markers found in the content.

        Raises:
            SQLFileParseError: If named statements are malformed (duplicate names or
                              invalid content after parsing).
        """
        statements: dict[str, NamedStatement] = {}

        name_matches = list(QUERY_NAME_PATTERN.finditer(content))
        if not name_matches:
            return {}

        for i, match in enumerate(name_matches):
            raw_statement_name = match.group(1).strip()
            statement_start_line = content[: match.start()].count("\n")

            start_pos = match.end()
            end_pos = name_matches[i + 1].start() if i + 1 < len(name_matches) else len(content)

            statement_section = content[start_pos:end_pos].strip()
            if not raw_statement_name or not statement_section:
                continue

            dialect = None
            statement_sql = statement_section

            section_lines = [line.strip() for line in statement_section.split("\n") if line.strip()]
            if section_lines:
                first_line = section_lines[0]
                dialect_match = DIALECT_PATTERN.match(first_line)
                if dialect_match:
                    declared_dialect = dialect_match.group("dialect").lower()

                    dialect = _normalize_dialect(declared_dialect)
                    remaining_lines = section_lines[1:]
                    statement_sql = "\n".join(remaining_lines)

            clean_sql = SQLFileLoader._strip_leading_comments(statement_sql)
            if clean_sql:
                normalized_name = _normalize_query_name(raw_statement_name)
                if normalized_name in statements:
                    raise SQLFileParseError(
                        file_path, file_path, ValueError(f"Duplicate statement name: {raw_statement_name}")
                    )

                statements[normalized_name] = NamedStatement(
                    name=normalized_name, sql=clean_sql, dialect=dialect, start_line=statement_start_line
                )
                log_with_context(
                    logger, logging.DEBUG, "sql.parse", file_path=file_path, query_name=normalized_name, dialect=dialect
                )

        if not statements:
            raise SQLFileParseError(file_path, file_path, ValueError("No valid SQL statements found after parsing"))

        return statements

    def load_sql(self, *paths: str | Path) -> None:
        """Load SQL files and parse named queries.

        Args:
            *paths: One or more file paths or directory paths to load.
        """
        runtime = self._runtime
        span = None
        error: Exception | None = None
        start_time = time.perf_counter()
        path_count = len(paths)
        previous_correlation_id = CorrelationContext.get()
        if runtime is not None:
            runtime.increment_metric("loader.load.invocations")
            runtime.increment_metric("loader.paths.requested", path_count)
            span = runtime.start_span(
                "sqlspec.loader.load",
                attributes={"sqlspec.loader.path_count": path_count, "sqlspec.loader.encoding": self.encoding},
            )

        try:
            for path in paths:
                path_str = str(path)
                if "://" in path_str:
                    self._load_single_file(path, None)
                else:
                    path_obj = Path(path)
                    if path_obj.is_dir():
                        self._load_directory(path_obj)
                    elif path_obj.exists():
                        self._load_single_file(path_obj, None)
                    elif path_obj.suffix:
                        self._raise_file_not_found(str(path))

        except Exception as exc:
            error = exc
            if runtime is not None:
                runtime.increment_metric("loader.load.errors")
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            if runtime is not None:
                runtime.record_metric("loader.last_load_ms", duration_ms)
                runtime.increment_metric("loader.load.duration_ms", duration_ms)
                runtime.end_span(span, error=error)
            CorrelationContext.set(previous_correlation_id)

    def _load_directory(self, dir_path: Path) -> None:
        """Load all SQL files from a directory.

        Args:
            dir_path: Directory path to load SQL files from.
        """
        runtime = self._runtime
        if runtime is not None:
            runtime.increment_metric("loader.directories.scanned")

        sql_files = list(dir_path.rglob("*.sql"))
        if not sql_files:
            return

        for file_path in sql_files:
            relative_path = file_path.relative_to(dir_path)
            namespace_parts = relative_path.parent.parts
            self._load_single_file(file_path, ".".join(namespace_parts) if namespace_parts else None)

    def _load_single_file(self, file_path: str | Path, namespace: str | None) -> bool:
        """Load a single SQL file with optional namespace.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.

        Returns:
            True if file was newly loaded, False if already cached.
        """
        path_str = str(file_path)
        runtime = self._runtime
        if runtime is not None:
            runtime.increment_metric("loader.files.considered")

        if path_str in self._files:
            if runtime is not None:
                runtime.increment_metric("loader.cache.hit")
            return False

        cache_config = get_cache_config()
        if not cache_config.compiled_cache_enabled:
            self._load_file_without_cache(file_path, namespace)
            if runtime is not None:
                runtime.increment_metric("loader.cache.miss")
            return True

        cache_key_str = self._generate_file_cache_key(file_path)
        cache = get_cache()
        cached_file = cache.get_file(cache_key_str)

        if (
            cached_file is not None
            and isinstance(cached_file, SQLFileCacheEntry)
            and self._is_file_unchanged(file_path, cached_file)
        ):
            self._files[path_str] = cached_file.sql_file
            for name, statement in cached_file.parsed_statements.items():
                namespaced_name = f"{namespace}.{name}" if namespace else name
                if namespaced_name in self._queries:
                    existing_file = self._query_to_file.get(namespaced_name, "unknown")
                    if existing_file != path_str:
                        raise SQLFileParseError(
                            path_str,
                            path_str,
                            ValueError(f"Query name '{namespaced_name}' already exists in file: {existing_file}"),
                        )
                self._queries[namespaced_name] = statement
                self._query_to_file[namespaced_name] = path_str
            if runtime is not None:
                runtime.increment_metric("loader.cache.hit")
            return True

        self._load_file_without_cache(file_path, namespace)

        if path_str in self._files:
            sql_file = self._files[path_str]
            file_statements: dict[str, NamedStatement] = {}
            for query_name, query_path in self._query_to_file.items():
                if query_path == path_str:
                    stored_name = query_name
                    if namespace and query_name.startswith(f"{namespace}."):
                        stored_name = query_name[len(namespace) + 1 :]
                    file_statements[stored_name] = self._queries[query_name]

            cached_file_data = SQLFileCacheEntry(sql_file=sql_file, parsed_statements=file_statements)
            cache.put_file(cache_key_str, cached_file_data)
            if runtime is not None:
                runtime.increment_metric("loader.cache.miss")
                runtime.increment_metric("loader.files.loaded")
                runtime.increment_metric("loader.statements.loaded", len(file_statements))

        return True

    def _load_file_without_cache(self, file_path: str | Path, namespace: str | None) -> None:
        """Load a single SQL file without using cache.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.
        """
        path_str = str(file_path)
        runtime = self._runtime
        content = self._read_file_content(file_path)
        statements = self._parse_sql_content(content, path_str)

        if not statements:
            log_with_context(
                logger, logging.DEBUG, "sql.load", file_path=path_str, status="skipped", reason="no_named_statements"
            )
            return

        sql_file = SQLFile(content=content, path=path_str)
        self._files[path_str] = sql_file

        for name, statement in statements.items():
            namespaced_name = f"{namespace}.{name}" if namespace else name
            if namespaced_name in self._queries:
                existing_file = self._query_to_file.get(namespaced_name, "unknown")
                if existing_file != path_str:
                    raise SQLFileParseError(
                        path_str,
                        path_str,
                        ValueError(f"Query name '{namespaced_name}' already exists in file: {existing_file}"),
                    )
            self._queries[namespaced_name] = statement
            self._query_to_file[namespaced_name] = path_str
        log_with_context(
            logger, logging.DEBUG, "sql.load", file_path=path_str, statement_count=len(statements), status="loaded"
        )
        if runtime is not None:
            runtime.increment_metric("loader.files.loaded")
            runtime.increment_metric("loader.statements.loaded", len(statements))

    def add_named_sql(self, name: str, sql: str, dialect: "str | None" = None) -> None:
        """Add a named SQL query directly without loading from a file.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.
            dialect: Optional dialect for the SQL statement.

        Raises:
            ValueError: If query name already exists.
        """

        normalized_name = _normalize_query_name(name)

        if normalized_name in self._queries:
            existing_source = self._query_to_file.get(normalized_name, "<directly added>")
            msg = f"Query name '{name}' already exists (source: {existing_source})"
            raise ValueError(msg)

        if dialect is not None:
            dialect = _normalize_dialect(dialect)

        statement = NamedStatement(name=normalized_name, sql=sql.strip(), dialect=dialect, start_line=0)
        self._queries[normalized_name] = statement
        self._query_to_file[normalized_name] = "<directly added>"

    def get_file(self, path: str | Path) -> "SQLFile | None":
        """Get a loaded SQLFile object by path.

        Args:
            path: Path of the file.

        Returns:
            SQLFile object if loaded, None otherwise.
        """
        return self._files.get(str(path))

    def get_file_for_query(self, name: str) -> "SQLFile | None":
        """Get the SQLFile object containing a query.

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
            name: Query name to check.

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

        cache_config = get_cache_config()
        if cache_config.compiled_cache_enabled:
            cache = get_cache()
            cache.clear()

    def clear_file_cache(self) -> None:
        """Clear the file cache only, keeping loaded queries."""
        cache_config = get_cache_config()
        if cache_config.compiled_cache_enabled:
            cache = get_cache()
            cache.clear()

    def get_query_text(self, name: str) -> str:
        """Get raw SQL text for a query.

        Args:
            name: Query name.

        Returns:
            Raw SQL text.

        Raises:
            SQLFileNotFoundError: If query not found.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            raise SQLFileNotFoundError(name)
        return self._queries[safe_name].sql

    def get_sql(self, name: str) -> "SQL":
        """Get a SQL object by statement name.

        Args:
            name: Name of the statement (from -- name: in SQL file).
                  Hyphens in names are converted to underscores.

        Returns:
            SQL object ready for execution.

        Raises:
            SQLFileNotFoundError: If statement name not found.
        """
        safe_name = _normalize_query_name(name)

        if safe_name not in self._queries:
            available = ", ".join(sorted(self._queries.keys())) if self._queries else "none"
            raise SQLFileNotFoundError(name, path=f"Statement '{name}' not found. Available statements: {available}")

        parsed_statement = self._queries[safe_name]
        sqlglot_dialect = None
        if parsed_statement.dialect:
            sqlglot_dialect = _normalize_dialect(parsed_statement.dialect)

        sql = SQL(parsed_statement.sql, dialect=sqlglot_dialect)
        try:
            sql.compile()
        except Exception as exc:
            raise SQLFileParseError(name=name, path="<statement>", original_error=exc) from exc
        return sql
