"""Simple SQL file loader using aiosql-style parsing.

This module provides a lightweight SQL file loader that:
- Parses aiosql-style SQL files (-- name: comments)
- Returns SQLSpec SQL objects ready for execution
- Uses singleton pattern for efficient file caching
- Integrates seamlessly with existing SQLSpec drivers
- Supports filters and convenience methods for different operation types
"""

import logging
import re
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Union,
)

from sqlspec.exceptions import MissingDependencyError, SQLSpecError
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import AIOSQL_INSTALLED, AiosqlSQLOperationType
from sqlspec.utils.singleton import SingletonMeta

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter

logger = logging.getLogger("sqlspec.extensions.aiosql.loader")

# Compiled regex patterns for performance (using aiosql's patterns)
AIOSQL_QUERY_PATTERN = re.compile(
    r"--\s*name:\s*(\w+)(?:\([^)]*\))?([\$\!\*\#\^\<]*)\s*$", re.MULTILINE | re.IGNORECASE
)

# Pattern for validating query names (security)
VALID_QUERY_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Minimum parts needed for a valid aiosql query (name, suffix, sql)
MIN_QUERY_PARTS = 3

__all__ = (
    "AiosqlLoader",
    "SqlFileParseError",
)


class SqlFileParseError(SQLSpecError):
    """Raised when SQL file parsing fails."""


class AiosqlLoader(metaclass=SingletonMeta):
    """Simple SQL file loader using aiosql-style parsing.

    This loader parses aiosql-style SQL files and returns SQLSpec SQL objects
    that can be executed with any SQLSpec driver. It's much simpler than the
    full aiosql integration - just file parsing + SQLSpec execution.

    Example:
        >>> # Load SQL file
        >>> loader = AiosqlLoader("queries.sql")
        >>> # Get SQL object ready for execution
        >>> get_users_sql = loader.get_sql("get_users", "postgresql")
        >>> # Execute with any SQLSpec driver (schema_type at execution time)
        >>> result = driver.execute(
        ...     get_users_sql, {"active": True}, schema_type=User
        ... )
    """

    _file_cache: ClassVar[dict[str, dict[str, tuple[str, "AiosqlSQLOperationType"]]]] = {}

    def __init__(
        self,
        sql_path: Union[str, Path],
        config: "Optional[SQLConfig]" = None,
    ) -> None:
        """Initialize the SQL loader.

        Args:
            sql_path: Path to SQL file
            config: SQLSpec configuration

        Raises:
            MissingDependencyError: If aiosql is not installed (for compatibility)
            SqlFileParseError: If file path is invalid or insecure
        """
        if not AIOSQL_INSTALLED:
            msg = "aiosql"
            raise MissingDependencyError(msg, "aiosql")

        self.sql_path = self._validate_and_resolve_path(sql_path)
        self.config = config or SQLConfig()

        # Use the absolute path as cache key
        cache_key = str(self.sql_path.absolute())

        if cache_key not in self._file_cache:
            logger.info("Loading and parsing SQL file: %s", self.sql_path)
            self._file_cache[cache_key] = self._parse_sql_file()
        else:
            logger.debug("Using cached SQL file: %s", self.sql_path)

        self._queries = self._file_cache[cache_key]

    @staticmethod
    def _validate_and_resolve_path(sql_path: Union[str, Path]) -> Path:
        """Validate and resolve the SQL file path for security."""
        try:
            path = Path(sql_path).resolve()
        except (OSError, ValueError) as e:
            msg = f"Invalid SQL file path: {sql_path}"
            raise SqlFileParseError(msg) from e

        # Basic security check
        path_str = str(path)
        suspicious_patterns = ["../", "..\\", "~", "$"]
        if any(pattern in path_str for pattern in suspicious_patterns):
            msg = f"Potentially unsafe SQL file path: {sql_path}"
            raise SqlFileParseError(msg)

        return path

    def _parse_sql_file(self) -> dict[str, tuple[str, "AiosqlSQLOperationType"]]:
        if not self.sql_path.exists():
            msg = f"SQL file not found: {self.sql_path}"
            raise SqlFileParseError(msg)

        if not self.sql_path.is_file():
            msg = f"Path is not a file: {self.sql_path}"
            raise SqlFileParseError(msg)

        try:
            content = self.sql_path.read_text(encoding="utf-8")
        except Exception as e:
            msg = f"Failed to read SQL file {self.sql_path}: {e}"
            raise SqlFileParseError(msg) from e

        return self._parse_sql_content(content)

    def _parse_sql_content(self, content: str) -> dict[str, tuple[str, "AiosqlSQLOperationType"]]:
        queries = {}

        # Split content by query definitions
        parts = AIOSQL_QUERY_PATTERN.split(content)

        if len(parts) < MIN_QUERY_PARTS:
            msg = "No valid aiosql queries found in file"
            raise SqlFileParseError(msg)

        # Skip first part (content before first query)
        for i in range(1, len(parts), 3):
            if i + 2 >= len(parts):
                break

            query_name = parts[i].strip()
            operation_suffix = parts[i + 1].strip() or ""  # Default to empty (SELECT)
            sql_text = parts[i + 2].strip()

            if not query_name or not sql_text:
                continue

            # Validate query name for security
            if not VALID_QUERY_NAME_PATTERN.match(query_name):
                logger.warning("Skipping query with invalid name: %s", query_name)
                continue

            # Map aiosql operation suffixes to operation types
            operation_type = self._map_operation_type(operation_suffix)

            queries[query_name] = (sql_text, operation_type)
            logger.debug("Parsed query: %s (%s)", query_name, operation_type)

        if not queries:
            msg = "No valid queries parsed from SQL file"
            raise SqlFileParseError(msg)

        logger.info("Successfully parsed %d queries from %s", len(queries), self.sql_path)
        return queries

    @staticmethod
    def _map_operation_type(suffix: str) -> "AiosqlSQLOperationType":
        """Map aiosql operation suffix to operation type."""
        mapping = {
            "^": AiosqlSQLOperationType.SELECT_ONE,
            "$": AiosqlSQLOperationType.SELECT_VALUE,
            "!": AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            "<!": AiosqlSQLOperationType.INSERT_RETURNING,
            "*!": AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
            "#": AiosqlSQLOperationType.SCRIPT,
            "": AiosqlSQLOperationType.SELECT,  # Default
        }
        return mapping.get(suffix, AiosqlSQLOperationType.SELECT)

    @property
    def query_names(self) -> list[str]:
        """Get list of all available query names."""
        return list(self._queries.keys())

    def has_query(self, name: str) -> bool:
        """Check if a query with the given name exists."""
        return name in self._queries

    def get_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get a SQL object ready for execution with optional filters.

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object ready for execution with SQLSpec drivers

        Raises:
            SqlFileParseError: If query not found
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        sql_text, _ = self._queries[name]
        effective_config = config or self.config

        # Create basic SQL object - schema_type handled at execution time
        sql_obj = SQL(sql_text, config=effective_config)

        # Apply filters if provided
        for filter_obj in filters:
            if filter_obj is not None:
                sql_obj = filter_obj.append_to_statement(sql_obj)

        return sql_obj

    # Convenience methods for different operation types

    def get_select_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get a SELECT SQL object (for queries with no suffix or ^ suffix).

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for SELECT operations

        Raises:
            SqlFileParseError: If query not found or not a SELECT operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        if operation_type not in (
            AiosqlSQLOperationType.SELECT,
            AiosqlSQLOperationType.SELECT_ONE,
            AiosqlSQLOperationType.SELECT_VALUE,
        ):
            msg = f"Query '{name}' is not a SELECT operation (operation type: {operation_type})"
            raise SqlFileParseError(msg)

        return self.get_sql(name, *filters, config=config)

    def get_insert_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get an INSERT SQL object (for queries with ! or <! suffix).

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for INSERT operations

        Raises:
            SqlFileParseError: If query not found or not an INSERT operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        if operation_type not in (
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            AiosqlSQLOperationType.INSERT_RETURNING,
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
        ):
            msg = f"Query '{name}' is not an INSERT operation (operation type: {operation_type})"
            raise SqlFileParseError(msg)

        return self.get_sql(name, *filters, config=config)

    def get_update_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get an UPDATE SQL object (for queries with ! suffix).

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for UPDATE operations

        Raises:
            SqlFileParseError: If query not found or not an UPDATE operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        if operation_type not in (
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
        ):
            msg = f"Query '{name}' is not an UPDATE operation (operation type: {operation_type})"
            raise SqlFileParseError(msg)

        return self.get_sql(name, *filters, config=config)

    def get_delete_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get a DELETE SQL object (for queries with ! suffix).

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for DELETE operations

        Raises:
            SqlFileParseError: If query not found or not a DELETE operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        if operation_type not in (
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
        ):
            msg = f"Query '{name}' is not a DELETE operation (operation type: {operation_type})"
            raise SqlFileParseError(msg)

        return self.get_sql(name, *filters, config=config)

    def get_script_sql(
        self,
        name: str,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get a SCRIPT SQL object (for queries with # suffix).

        Note: Scripts don't support filters as they typically contain multiple statements.

        Args:
            name: Query name
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for SCRIPT operations

        Raises:
            SqlFileParseError: If query not found or not a SCRIPT operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        if operation_type != AiosqlSQLOperationType.SCRIPT:
            msg = f"Query '{name}' is not a SCRIPT operation (operation type: {operation_type})"
            raise SqlFileParseError(msg)

        return self.get_sql(name, config=config)

    def get_operation_type(self, name: str) -> "AiosqlSQLOperationType":
        """Get the operation type for a query.

        Args:
            name: Query name

        Returns:
            The aiosql operation type

        Raises:
            SqlFileParseError: If query not found
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        _, operation_type = self._queries[name]
        return operation_type

    def get_raw_sql(self, name: str) -> str:
        """Get the raw SQL text for a query.

        Args:
            name: Query name

        Returns:
            Raw SQL text

        Raises:
            SqlFileParseError: If query not found
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        sql_text, _ = self._queries[name]
        return sql_text

    def __contains__(self, query_name: str) -> bool:
        """Check if query exists using 'in' operator."""
        return self.has_query(query_name)

    def __getitem__(self, query_name: str) -> str:
        """Get raw SQL using dictionary-like access."""
        return self.get_raw_sql(query_name)

    def __len__(self) -> int:
        """Get number of loaded queries."""
        return len(self._queries)

    def __repr__(self) -> str:
        """String representation of the loader."""
        return f"AiosqlLoader(path='{self.sql_path}', queries={len(self._queries)})"

    def get_merge_sql(
        self,
        name: str,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Get a MERGE SQL object (for queries with ! suffix that contain MERGE).

        Args:
            name: Query name
            *filters: Optional statement filters to apply
            config: Optional SQLSpec configuration override

        Returns:
            SQL object for MERGE operations

        Raises:
            SqlFileParseError: If query not found or not a MERGE operation
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        sql_text, operation_type = self._queries[name]

        # MERGE operations should use ! suffix and contain MERGE keyword
        if operation_type != AiosqlSQLOperationType.INSERT_UPDATE_DELETE:
            msg = f"Query '{name}' is not a MERGE operation (operation type: {operation_type}). MERGE queries should use '!' suffix."
            raise SqlFileParseError(msg)

        # Check if SQL actually contains MERGE keyword
        if "MERGE" not in sql_text.upper():
            msg = f"Query '{name}' does not contain MERGE statement. Expected MERGE operation but found: {sql_text[:50]}..."
            raise SqlFileParseError(msg)

        return self.get_sql(name, *filters, config=config)
