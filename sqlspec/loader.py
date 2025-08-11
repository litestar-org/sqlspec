"""SQL file loader module for managing SQL statements from files.

This module provides functionality to load, cache, and manage SQL statements
from files using aiosql-style named queries.
"""

import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Optional, Union

from sqlspec.core.cache import CacheKey, get_cache_config, get_default_cache
from sqlspec.core.parameters import ParameterStyleConfig, ParameterValidator
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError, StorageOperationFailedError
from sqlspec.storage import storage_registry
from sqlspec.storage.registry import StorageRegistry
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

__all__ = ("CachedSQLFile", "NamedStatement", "SQLFile", "SQLFileLoader")

logger = get_logger("loader")

# Matches: -- name: query_name (supports hyphens and special suffixes)
# We capture the name plus any trailing special characters
QUERY_NAME_PATTERN = re.compile(r"^\s*--\s*name\s*:\s*([\w-]+[^\w\s]*)\s*$", re.MULTILINE | re.IGNORECASE)
TRIM_SPECIAL_CHARS = re.compile(r"[^\w-]")

# Matches: -- dialect: dialect_name (optional dialect specification)
DIALECT_PATTERN = re.compile(r"^\s*--\s*dialect\s*:\s*(?P<dialect>[a-zA-Z0-9_]+)\s*$", re.IGNORECASE | re.MULTILINE)

# Supported SQL dialects (based on SQLGlot's available dialects)
SUPPORTED_DIALECTS = {
    # Core databases
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "oracle",
    "mssql",
    "tsql",
    # Cloud platforms
    "bigquery",
    "snowflake",
    "redshift",
    "athena",
    "fabric",
    # Analytics engines
    "clickhouse",
    "duckdb",
    "databricks",
    "spark",
    "spark2",
    "trino",
    "presto",
    # Specialized
    "hive",
    "drill",
    "druid",
    "materialize",
    "teradata",
    "dremio",
    "doris",
    "risingwave",
    "singlestore",
    "starrocks",
    "tableau",
    "exasol",
    "dune",
}

# Dialect aliases for common variants
DIALECT_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "pgplsql": "postgres",
    "plsql": "oracle",
    "oracledb": "oracle",
    "tsql": "mssql",
}

MIN_QUERY_PARTS = 3


def _normalize_query_name(name: str) -> str:
    """Normalize query name to be a valid Python identifier.

    Args:
        name: Raw query name from SQL file

    Returns:
        Normalized query name suitable as Python identifier
    """
    return TRIM_SPECIAL_CHARS.sub("", name).replace("-", "_")


def _normalize_dialect(dialect: str) -> str:
    """Normalize dialect name with aliases.

    Args:
        dialect: Raw dialect name from SQL file

    Returns:
        Normalized dialect name
    """
    normalized = dialect.lower().strip()
    return DIALECT_ALIASES.get(normalized, normalized)


def _normalize_dialect_for_sqlglot(dialect: str) -> str:
    """Normalize dialect name for SQLGlot compatibility.

    Args:
        dialect: Dialect name from SQL file or parameter

    Returns:
        SQLGlot-compatible dialect name
    """
    normalized = dialect.lower().strip()
    return DIALECT_ALIASES.get(normalized, normalized)


def _get_dialect_suggestions(invalid_dialect: str) -> "list[str]":
    """Get dialect suggestions using fuzzy matching.

    Args:
        invalid_dialect: Invalid dialect name that was provided

    Returns:
        List of suggested dialect names (up to 3 suggestions)
    """

    return get_close_matches(invalid_dialect, SUPPORTED_DIALECTS, n=3, cutoff=0.6)


class NamedStatement:
    """Represents a parsed SQL statement with metadata.

    Contains individual SQL statements extracted from files with their
    normalized names, SQL content, optional dialect specifications,
    and line position for error reporting.
    """

    __slots__ = ("dialect", "name", "sql", "start_line")

    def __init__(self, name: str, sql: str, dialect: "Optional[str]" = None, start_line: int = 0) -> None:
        self.name = name
        self.sql = sql
        self.dialect = dialect
        self.start_line = start_line


@dataclass
class SQLFile:
    """Represents a loaded SQL file with metadata.

    Contains SQL content and associated metadata including file location,
    timestamps, and content hash.
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


class CachedSQLFile:
    """Cached SQL file with parsed statements for efficient reloading.

    Stored in the file cache to avoid re-parsing SQL files when their
    content hasn't changed.
    """

    __slots__ = ("parsed_statements", "sql_file", "statement_names")

    def __init__(self, sql_file: SQLFile, parsed_statements: "dict[str, NamedStatement]") -> None:
        """Initialize cached SQL file.

        Args:
            sql_file: The original SQLFile with content and metadata.
            parsed_statements: Named statements from the file.
        """
        self.sql_file = sql_file
        self.parsed_statements = parsed_statements
        self.statement_names = list(parsed_statements.keys())


class SQLFileLoader:
    """Loads and parses SQL files with aiosql-style named queries.

    Provides functionality to load SQL files containing named queries
    (using -- name: syntax) and retrieve them by name.
    """

    def __init__(self, *, encoding: str = "utf-8", storage_registry: StorageRegistry = storage_registry) -> None:
        """Initialize the SQL file loader.

        Args:
            encoding: Text encoding for reading SQL files.
            storage_registry: Storage registry for handling file URIs.
        """
        self.encoding = encoding
        self.storage_registry = storage_registry
        self._queries: dict[str, NamedStatement] = {}
        self._files: dict[str, SQLFile] = {}
        self._query_to_file: dict[str, str] = {}

    def _raise_file_not_found(self, path: str) -> None:
        """Raise SQLFileNotFoundError for nonexistent file.

        Args:
            path: File path that was not found.

        Raises:
            SQLFileNotFoundError: Always raised.
        """
        raise SQLFileNotFoundError(path)

    def _generate_file_cache_key(self, path: Union[str, Path]) -> str:
        """Generate cache key for a file path.

        Args:
            path: File path to generate key for.

        Returns:
            Cache key string for the file.
        """
        path_str = str(path)
        path_hash = hashlib.md5(path_str.encode(), usedforsecurity=False).hexdigest()
        return f"file:{path_hash[:16]}"

    def _calculate_file_checksum(self, path: Union[str, Path]) -> str:
        """Calculate checksum for file content validation.

        Args:
            path: File path to calculate checksum for.

        Returns:
            MD5 checksum of file content.

        Raises:
            SQLFileParseError: If file cannot be read.
        """
        try:
            content = self._read_file_content(path)
            return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()
        except Exception as e:
            raise SQLFileParseError(str(path), str(path), e) from e

    def _is_file_unchanged(self, path: Union[str, Path], cached_file: CachedSQLFile) -> bool:
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

    def _read_file_content(self, path: Union[str, Path]) -> str:
        """Read file content using storage backend.

        Args:
            path: File path (can be local path or URI).

        Returns:
            File content as string.

        Raises:
            SQLFileNotFoundError: If file does not exist.
            SQLFileParseError: If file cannot be read or parsed.
        """

        path_str = str(path)

        try:
            backend = self.storage_registry.get(path)
            return backend.read_text(path_str, encoding=self.encoding)
        except KeyError as e:
            raise SQLFileNotFoundError(path_str) from e
        except StorageOperationFailedError as e:
            if "not found" in str(e).lower() or "no such file" in str(e).lower():
                raise SQLFileNotFoundError(path_str) from e
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

        Args:
            content: Raw SQL file content to parse
            file_path: File path for error reporting

        Returns:
            Dictionary mapping normalized statement names to NamedStatement objects

        Raises:
            SQLFileParseError: If no named statements found, duplicate names exist,
                              or invalid dialect names are specified
        """
        statements: dict[str, NamedStatement] = {}
        content.splitlines()

        name_matches = list(QUERY_NAME_PATTERN.finditer(content))
        if not name_matches:
            raise SQLFileParseError(
                file_path, file_path, ValueError("No named SQL statements found (-- name: statement_name)")
            )

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

                    normalized_dialect = _normalize_dialect(declared_dialect)

                    if normalized_dialect not in SUPPORTED_DIALECTS:
                        suggestions = _get_dialect_suggestions(normalized_dialect)
                        warning_msg = f"Unknown dialect '{declared_dialect}' at line {statement_start_line + 1}"
                        if suggestions:
                            warning_msg += f". Did you mean: {', '.join(suggestions)}?"
                        warning_msg += (
                            f". Supported dialects: {', '.join(sorted(SUPPORTED_DIALECTS))}. Using dialect as-is."
                        )
                        logger.warning(warning_msg)
                        dialect = declared_dialect.lower()
                    else:
                        dialect = normalized_dialect
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

        if not statements:
            raise SQLFileParseError(file_path, file_path, ValueError("No valid SQL statements found after parsing"))

        return statements

    def load_sql(self, *paths: Union[str, Path]) -> None:
        """Load SQL files and parse named queries.

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
                    elif path_obj.exists():
                        self._load_single_file(path_obj, None)
                        loaded_count += 1
                    elif path_obj.suffix:
                        self._raise_file_not_found(str(path))

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
        """Load a single SQL file with optional namespace and caching.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.
        """
        path_str = str(file_path)

        if path_str in self._files:
            return

        cache_config = get_cache_config()
        if not cache_config.compiled_cache_enabled:
            self._load_file_without_cache(file_path, namespace)
            return

        cache_key_str = self._generate_file_cache_key(file_path)
        cache_key = CacheKey((cache_key_str,))
        unified_cache = get_default_cache()
        cached_file = unified_cache.get(cache_key)

        if (
            cached_file is not None
            and isinstance(cached_file, CachedSQLFile)
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
            return

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

            cached_file_data = CachedSQLFile(sql_file=sql_file, parsed_statements=file_statements)
            unified_cache.put(cache_key, cached_file_data)

    def _load_file_without_cache(self, file_path: Union[str, Path], namespace: Optional[str]) -> None:
        """Load a single SQL file without caching.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.
        """
        path_str = str(file_path)

        content = self._read_file_content(file_path)
        sql_file = SQLFile(content=content, path=path_str)
        self._files[path_str] = sql_file

        statements = self._parse_sql_content(content, path_str)
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

    def add_named_sql(self, name: str, sql: str, dialect: "Optional[str]" = None) -> None:
        """Add a named SQL query directly without loading from a file.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.
            dialect: Optional dialect for the SQL statement.

        Raises:
            ValueError: If query name already exists.
        """
        if name in self._queries:
            existing_source = self._query_to_file.get(name, "<directly added>")
            msg = f"Query name '{name}' already exists (source: {existing_source})"
            raise ValueError(msg)

        if dialect is not None:
            normalized_dialect = _normalize_dialect(dialect)
            if normalized_dialect not in SUPPORTED_DIALECTS:
                suggestions = _get_dialect_suggestions(normalized_dialect)
                warning_msg = f"Unknown dialect '{dialect}'"
                if suggestions:
                    warning_msg += f". Did you mean: {', '.join(suggestions)}?"
                warning_msg += f". Supported dialects: {', '.join(sorted(SUPPORTED_DIALECTS))}. Using dialect as-is."
                logger.warning(warning_msg)
                dialect = dialect.lower()
            else:
                dialect = normalized_dialect

        statement = NamedStatement(name=name, sql=sql.strip(), dialect=dialect, start_line=0)
        self._queries[name] = statement
        self._query_to_file[name] = "<directly added>"

    def get_sql(
        self, name: str, parameters: "Optional[Any]" = None, dialect: "Optional[str]" = None, **kwargs: "Any"
    ) -> "SQL":
        """Get a SQL object by statement name with dialect support.

        Args:
            name: Name of the statement (from -- name: in SQL file).
                  Hyphens in names are converted to underscores.
            parameters: Parameters for the SQL statement.
            dialect: Optional dialect override.
            **kwargs: Additional parameters to pass to the SQL object.

        Returns:
            SQL object ready for execution.

        Raises:
            SQLFileNotFoundError: If statement name not found.
        """
        correlation_id = CorrelationContext.get()

        safe_name = _normalize_query_name(name)

        if safe_name not in self._queries:
            available = ", ".join(sorted(self._queries.keys())) if self._queries else "none"
            logger.error(
                "Statement not found: %s",
                name,
                extra={
                    "statement_name": name,
                    "safe_name": safe_name,
                    "available_statements": len(self._queries),
                    "correlation_id": correlation_id,
                },
            )
            raise SQLFileNotFoundError(name, path=f"Statement '{name}' not found. Available statements: {available}")

        parsed_statement = self._queries[safe_name]

        effective_dialect = dialect or parsed_statement.dialect

        if dialect is not None:
            normalized_dialect = _normalize_dialect(dialect)
            if normalized_dialect not in SUPPORTED_DIALECTS:
                suggestions = _get_dialect_suggestions(normalized_dialect)
                warning_msg = f"Unknown dialect '{dialect}'"
                if suggestions:
                    warning_msg += f". Did you mean: {', '.join(suggestions)}?"
                warning_msg += f". Supported dialects: {', '.join(sorted(SUPPORTED_DIALECTS))}. Using dialect as-is."
                logger.warning(warning_msg)
                effective_dialect = dialect.lower()
            else:
                effective_dialect = normalized_dialect

        sql_kwargs = dict(kwargs)
        if parameters is not None:
            sql_kwargs["parameters"] = parameters

        sqlglot_dialect = None
        if effective_dialect:
            sqlglot_dialect = _normalize_dialect_for_sqlglot(effective_dialect)

        if not effective_dialect and "statement_config" not in sql_kwargs:
            validator = ParameterValidator()
            param_info = validator.extract_parameters(parsed_statement.sql)
            if param_info:
                styles = {p.style for p in param_info}
                if styles:
                    detected_style = next(iter(styles))
                    sql_kwargs["statement_config"] = StatementConfig(
                        parameter_config=ParameterStyleConfig(
                            default_parameter_style=detected_style,
                            supported_parameter_styles=styles,
                            preserve_parameter_format=True,
                        )
                    )

        return SQL(parsed_statement.sql, dialect=sqlglot_dialect, **sql_kwargs)

    def get_file(self, path: Union[str, Path]) -> "Optional[SQLFile]":
        """Get a loaded SQLFile object by path.

        Args:
            path: Path of the file.

        Returns:
            SQLFile object if loaded, None otherwise.
        """
        return self._files.get(str(path))

    def get_file_for_query(self, name: str) -> "Optional[SQLFile]":
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
            unified_cache = get_default_cache()
            unified_cache.clear()

    def clear_file_cache(self) -> None:
        """Clear the file cache only, keeping loaded queries."""
        cache_config = get_cache_config()
        if cache_config.compiled_cache_enabled:
            unified_cache = get_default_cache()
            unified_cache.clear()

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
