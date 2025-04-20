from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
)

from sqlglot import parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError, SQLParsingError
from sqlspec.typing import ConnectionT, StatementParameterType

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable

__all__ = (
    "AsyncArrowBulkOperationsMixin",
    "AsyncParquetExportMixin",
    "SQLTranslatorMixin",
    "SyncArrowBulkOperationsMixin",
    "SyncParquetExportMixin",
)


class SyncArrowBulkOperationsMixin(Generic[ConnectionT]):
    """Mixin for sync drivers supporting bulk Apache Arrow operations."""

    __supports_arrow__: "ClassVar[bool]" = True

    @abstractmethod
    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        raise NotImplementedError


class AsyncArrowBulkOperationsMixin(Generic[ConnectionT]):
    """Mixin for async drivers supporting bulk Apache Arrow operations."""

    __supports_arrow__: "ClassVar[bool]" = True

    @abstractmethod
    async def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        raise NotImplementedError


class SyncParquetExportMixin(Generic[ConnectionT]):
    """Mixin for sync drivers supporting Parquet export."""

    @abstractmethod
    def select_to_parquet(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> None:
        """Export a SQL query to a Parquet file."""
        raise NotImplementedError


class AsyncParquetExportMixin(Generic[ConnectionT]):
    """Mixin for async drivers supporting Parquet export."""

    @abstractmethod
    async def select_to_parquet(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> None:
        """Export a SQL query to a Parquet file."""
        raise NotImplementedError


class SQLTranslatorMixin(Generic[ConnectionT]):
    """Mixin for drivers supporting SQL translation."""

    dialect: str

    def convert_to_dialect(
        self,
        sql: str,
        to_dialect: DialectType = None,
        pretty: bool = True,
    ) -> str:
        """Convert a SQL query to a different dialect.

        Args:
            sql: The SQL query string to convert.
            to_dialect: The target dialect to convert to.
            pretty: Whether to pretty-print the SQL query.

        Returns:
            The converted SQL query string.

        Raises:
            SQLParsingError: If the SQL query cannot be parsed.
            SQLConversionError: If the SQL query cannot be converted to the target dialect.
        """
        try:
            parsed = parse_one(sql, dialect=self.dialect)
        except Exception as e:
            error_msg = f"Failed to parse SQL: {e!s}"
            raise SQLParsingError(error_msg) from e
        if to_dialect is None:
            to_dialect = self.dialect
        try:
            return parsed.sql(dialect=to_dialect, pretty=pretty)
        except Exception as e:
            error_msg = f"Failed to convert SQL to {to_dialect}: {e!s}"
            raise SQLConversionError(error_msg) from e
