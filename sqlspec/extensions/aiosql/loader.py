"""Advanced SQL file loader with singleton caching and full SQLSpec integration.

This module provides a sophisticated SQL loading system that:
- Uses singleton pattern for efficient file caching
- Parses aiosql-style SQL files
- Creates typed SQL objects that work with SQLSpec's builder API
- Supports full SQLSpec feature integration (filters, transformations, etc.)
- Provides seamless developer experience with type annotations
"""

import logging
import re
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from sqlglot import exp

from sqlspec.exceptions import MissingDependencyError, SQLSpecError
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import AIOSQL_INSTALLED, AiosqlSQLOperationType, ModelDTOT, SQLParameterType
from sqlspec.utils.singleton import SingletonMeta

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import ExecuteResult, SelectResult

logger = logging.getLogger("sqlspec.extensions.aiosql.loader")

__all__ = (
    "AiosqlLoader",
    "AiosqlQuery",
    "SqlFileParseError",
)

T = TypeVar("T")
DriverT = TypeVar("DriverT", bound=Union["SyncDriverAdapterProtocol[Any, Any]", "AsyncDriverAdapterProtocol[Any, Any]"])


class SqlFileParseError(SQLSpecError):
    """Raised when SQL file parsing fails."""


class AiosqlQuery:
    """Represents a single query loaded from an aiosql file.

    This class wraps a SQLSpec SQL object and provides seamless integration
    with the entire SQLSpec ecosystem, including builders, filters, and transformations.

    Example:
        >>> loader = AiosqlLoader("queries.sql", dialect="postgresql")
        >>> get_users = loader.get_query("get_users", return_type=User)
        >>> # Use with SQLSpec builder API
        >>> filtered_query = get_users.where(col("age") > 18).limit(10)
        >>> # Execute with driver
        >>> result = driver.execute(filtered_query, schema_type=User)
        >>> # Apply filters dynamically
        >>> from sqlspec.statement.filters import SearchFilter
        >>> result = driver.execute(
        ...     get_users,
        ...     filters=[SearchFilter("name", "John")],
        ...     schema_type=User,
        ... )
    """

    def __init__(
        self,
        name: str,
        sql_text: str,
        operation_type: "AiosqlSQLOperationType",
        dialect: str,
        config: "Optional[SQLConfig]" = None,
        return_type: "Optional[type[ModelDTOT]]" = None,
    ) -> None:
        """Initialize an aiosql query.

        Args:
            name: Query name from the SQL file
            sql_text: Raw SQL text
            operation_type: Type of SQL operation (from aiosql)
            dialect: SQL dialect
            config: SQLSpec configuration
            return_type: Optional return type annotation
        """
        self.name = name
        self.operation_type = operation_type
        self.return_type = return_type
        self._sql_obj = SQL(sql_text, dialect=dialect, config=config)

    @property
    def sql(self) -> "SQL":
        """Get the underlying SQLSpec SQL object."""
        return self._sql_obj

    @property
    def sql_text(self) -> str:
        """Get the raw SQL text."""
        return self._sql_obj.sql

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the parsed SQLGlot expression."""
        return self._sql_obj.expression

    @property
    def dialect(self) -> "DialectType":
        """Get the SQL dialect."""
        return self._sql_obj.dialect

    def with_parameters(self, parameters: "SQLParameterType") -> "SQL":
        """Create a new SQL object with parameters.

        Args:
            parameters: Query parameters

        Returns:
            New SQL object with parameters applied
        """
        return SQL(
            self._sql_obj.sql,
            parameters=parameters,
            dialect=self._sql_obj.dialect,
            config=self._sql_obj.config,
        )

    def append_filter(self, filter_obj: "StatementFilter") -> "SQL":
        """Apply a filter to this query.

        Args:
            filter_obj: SQLSpec filter to apply

        Returns:
            New SQL object with filter applied
        """
        return self._sql_obj.append_filter(filter_obj)

    def where(self, condition: Union[str, exp.Condition]) -> "SQL":
        """Add WHERE condition using builder pattern.

        Args:
            condition: WHERE condition to add

        Raises:
            SqlFileParseError: If WHERE is added to non-SELECT query

        Returns:
            New SQL object with WHERE condition
        """
        if self._sql_obj.expression and isinstance(self._sql_obj.expression, exp.Select):
            new_expression = self._sql_obj.expression.where(condition)
            return SQL(
                str(new_expression),
                dialect=self._sql_obj.dialect,
                config=self._sql_obj.config,
            )
        msg = f"Cannot add WHERE to non-SELECT query: {self.name}"
        raise SqlFileParseError(msg)

    def limit(self, count: int) -> "SQL":
        """Add LIMIT using builder pattern.

        Args:
            count: Limit count

        Raises:
            SqlFileParseError: If LIMIT is added to non-SELECT query

        Returns:
            New SQL object with LIMIT applied
        """
        if self._sql_obj.expression and isinstance(self._sql_obj.expression, exp.Select):
            new_expression = self._sql_obj.expression.limit(count)
            return SQL(
                str(new_expression),
                dialect=self._sql_obj.dialect,
                config=self._sql_obj.config,
            )
        msg = f"Cannot add LIMIT to non-SELECT query: {self.name}"
        raise SqlFileParseError(msg)

    def order_by(self, *expressions: Union[str, exp.Expression]) -> "SQL":
        """Add ORDER BY using builder pattern.

        Args:
            *expressions: ORDER BY expressions

        Raises:
            SqlFileParseError: If ORDER BY is added to non-SELECT query

        Returns:
            New SQL object with ORDER BY applied
        """
        if self._sql_obj.expression and isinstance(self._sql_obj.expression, exp.Select):
            new_expression = self._sql_obj.expression.order_by(*expressions)
            return SQL(
                str(new_expression),
                dialect=self._sql_obj.dialect,
                config=self._sql_obj.config,
            )
        msg = f"Cannot add ORDER BY to non-SELECT query: {self.name}"
        raise SqlFileParseError(msg)

    def __str__(self) -> str:
        """String representation of the query."""
        return self._sql_obj.sql

    def __repr__(self) -> str:
        """Repr of the query."""
        return f"AiosqlQuery(name='{self.name}', operation_type='{self.operation_type}')"


class AiosqlLoader(metaclass=SingletonMeta):
    """Singleton SQL file loader with comprehensive SQLSpec integration.

    This loader parses aiosql-style SQL files once and caches the results,
    providing typed SQL objects that work seamlessly with SQLSpec's entire
    feature set including builders, filters, transformations, and more.

    Example:
        >>> # Load once, cache forever
        >>> loader = AiosqlLoader("queries.sql", dialect="postgresql")
        >>> # Get typed queries with return type annotations
        >>> get_users = loader.get_query("get_users", return_type=User)
        >>> create_user = loader.get_query(
        ...     "create_user", return_type=User
        ... )
        >>> # Use with full SQLSpec features
        >>> result = driver.execute(
        ...     get_users.where(col("active") == True).limit(10),
        ...     schema_type=User,
        ...     filters=[SearchFilter("name", "John")],
        ... )
        >>> # Seamless builder API integration
        >>> filtered_query = (
        ...     get_users.where(col("department") == "Engineering")
        ...     .order_by("created_at DESC")
        ...     .limit(20)
        ... )
    """

    _file_cache: ClassVar[dict[str, dict[str, "AiosqlQuery"]]] = {}

    def __init__(
        self,
        sql_path: Union[str, Path],
        dialect: str = "postgresql",
        config: "Optional[SQLConfig]" = None,
    ) -> None:
        """Initialize the SQL loader.

        Args:
            sql_path: Path to SQL file or directory
            dialect: SQL dialect to use
            config: SQLSpec configuration

        Raises:
            MissingDependencyError: If aiosql is not installed
        """
        if not AIOSQL_INSTALLED:
            msg = "aiosql"
            raise MissingDependencyError(msg, "aiosql")

        self.sql_path = Path(sql_path)
        self.dialect = dialect
        self.config = config or SQLConfig()

        # Use the absolute path as cache key to handle different working directories
        cache_key = str(self.sql_path.absolute())

        if cache_key not in self._file_cache:
            logger.info("Loading and parsing SQL file: %s", self.sql_path)
            self._file_cache[cache_key] = self._parse_sql_file()
        else:
            logger.debug("Using cached SQL file: %s", self.sql_path)

        self._queries = self._file_cache[cache_key]

    def _parse_sql_file(self) -> dict[str, "AiosqlQuery"]:
        """Parse aiosql-style SQL file and create AiosqlQuery objects.

        Returns:
            Dictionary mapping query names to AiosqlQuery objects

        Raises:
            SqlFileParseError: If file parsing fails
        """
        if not self.sql_path.exists():
            msg = f"SQL file not found: {self.sql_path}"
            raise SqlFileParseError(msg)

        try:
            content = self.sql_path.read_text(encoding="utf-8")
        except Exception as e:
            msg = f"Failed to read SQL file {self.sql_path}: {e}"
            raise SqlFileParseError(msg) from e

        return self._parse_sql_content(content)

    def _parse_sql_content(self, content: str) -> dict[str, "AiosqlQuery"]:
        """Parse SQL content and extract queries.

        Args:
            content: Raw SQL file content

        Returns:
            Dictionary of parsed queries

        Raises:
            SqlFileParseError: If parsing fails
        """
        queries = {}

        # Pattern to match aiosql query definitions
        # Matches: -- name: query_name^
        # Where ^ can be $, !, *, #, etc. (aiosql operation types)
        query_pattern = re.compile(r"--\s*name:\s*(\w+)([\$\!\*\#\^]?)\s*$", re.MULTILINE | re.IGNORECASE)

        # Split content by query definitions
        parts = query_pattern.split(content)

        if len(parts) < 3:
            msg = "No valid aiosql queries found in file"
            raise SqlFileParseError(msg)

        # Skip first part (content before first query)
        for i in range(1, len(parts), 3):
            if i + 2 >= len(parts):
                break

            query_name = parts[i].strip()
            operation_suffix = parts[i + 1].strip() or "^"  # Default to select
            sql_text = parts[i + 2].strip()

            if not query_name or not sql_text:
                continue

            # Map aiosql operation suffixes to operation types
            operation_type = self._map_operation_type(operation_suffix)

            try:
                query = AiosqlQuery(
                    name=query_name,
                    sql_text=sql_text,
                    operation_type=operation_type,
                    dialect=self.dialect,
                    config=self.config,
                )
                queries[query_name] = query
                logger.debug("Parsed query: %s (%s)", query_name, operation_type)

            except Exception as e:
                logger.warning("Failed to parse query '%s': %s", query_name, e)
                msg = f"Failed to parse query '{query_name}': {e}"
                raise SqlFileParseError(msg) from e

        if not queries:
            msg = "No valid queries parsed from SQL file"
            raise SqlFileParseError(msg)

        logger.info("Successfully parsed %d queries from %s", len(queries), self.sql_path)
        return queries

    @staticmethod
    def _map_operation_type(suffix: str) -> "AiosqlSQLOperationType":
        """Map aiosql operation suffix to operation type.

        Args:
            suffix: aiosql operation suffix (^, $, !, etc.)

        Returns:
            SQL operation type
        """
        # Basic mapping - can be extended based on aiosql documentation
        mapping = {
            "^": AiosqlSQLOperationType.SELECT_ONE,
            "$": AiosqlSQLOperationType.SELECT_VALUE,
            "!": AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            "<!": AiosqlSQLOperationType.INSERT_RETURNING,
            "*!": AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
            "#": AiosqlSQLOperationType.SCRIPT,
        }
        return mapping.get(suffix, AiosqlSQLOperationType.SELECT)

    @property
    def query_names(self) -> list[str]:
        """Get list of all available query names."""
        return list(self._queries.keys())

    def has_query(self, name: str) -> bool:
        """Check if a query with the given name exists.

        Args:
            name: Query name to check

        Returns:
            True if query exists
        """
        return name in self._queries

    @overload
    def get_query(
        self,
        name: str,
        *,
        return_type: "type[ModelDTOT]",
    ) -> "AiosqlQuery": ...

    @overload
    def get_query(
        self,
        name: str,
        *,
        return_type: None = None,
    ) -> "AiosqlQuery": ...

    def get_query(
        self,
        name: str,
        *,
        return_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "AiosqlQuery":
        """Get a query by name with optional return type annotation.

        Args:
            name: Query name
            return_type: Optional return type for result mapping

        Returns:
            AiosqlQuery object with type information

        Raises:
            SqlFileParseError: If query not found
        """
        if name not in self._queries:
            msg = f"Query '{name}' not found. Available queries: {', '.join(self.query_names)}"
            raise SqlFileParseError(msg)

        query = self._queries[name]
        if return_type:
            query.return_type = return_type  # pyright: ignore

        return query

    def get_all_queries(self) -> dict[str, "AiosqlQuery"]:
        """Get all loaded queries.

        Returns:
            Dictionary mapping query names to AiosqlQuery objects
        """
        return self._queries.copy()

    def execute_query(
        self,
        driver: "DriverT",  # pyright: ignore
        query_name: str,
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: Optional[Any] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[Any], ExecuteResult[Any]]":
        """Execute a query using a SQLSpec driver.

        Args:
            driver: SQLSpec driver (sync or async)
            query_name: Name of query to execute
            parameters: Query parameters
            *filters: SQLSpec filters to apply
            connection: Optional database connection
            **kwargs: Additional arguments passed to driver.execute

        Returns:
            Query execution result
        """
        query = self.get_query(query_name)

        # Apply parameters if provided
        sql_obj = query.with_parameters(parameters) if parameters else query.sql

        # Apply filters
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        # Execute with driver
        return cast(
            "Union[SelectResult[Any], ExecuteResult[Any]]",
            driver.execute(sql_obj, connection=connection, **kwargs),
        )

    async def aexecute_query(
        self,
        driver: "AsyncDriverAdapterProtocol[Any, Any]",
        query_name: str,
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: Optional[Any] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[Any], ExecuteResult[Any]]":
        """Execute a query using an async SQLSpec driver.

        Args:
            driver: Async SQLSpec driver
            query_name: Name of query to execute
            parameters: Query parameters
            *filters: SQLSpec filters to apply
            connection: Optional database connection
            **kwargs: Additional arguments passed to driver.execute

        Returns:
            Query execution result
        """
        query = self.get_query(query_name)

        # Apply parameters if provided
        sql_obj = query.with_parameters(parameters) if parameters else query.sql

        # Apply filters
        for filter_obj in filters:
            sql_obj = sql_obj.append_filter(filter_obj)

        # Execute with async driver
        return await driver.execute(sql_obj, connection=connection, **kwargs)

    def __contains__(self, query_name: str) -> bool:
        """Check if query exists using 'in' operator."""
        return query_name in self._queries

    def __getitem__(self, query_name: str) -> "AiosqlQuery":
        """Get query using dictionary-like access."""
        return self.get_query(query_name)

    def __len__(self) -> int:
        """Get number of loaded queries."""
        return len(self._queries)

    def __repr__(self) -> str:
        """Repr of the loader."""
        return f"AiosqlLoader(path='{self.sql_path}', queries={len(self._queries)})"
