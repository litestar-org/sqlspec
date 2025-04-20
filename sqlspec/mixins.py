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

from sqlspec.exceptions import SQLConversionError
from sqlspec.typing import ConnectionT, StatementParameterType

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable


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

    def convert_to_dialect(self, sql: str, to_dialect: DialectType) -> str:
        """Convert a SQL query to a different dialect.

        Args:
            sql: The SQL query string to convert.
            to_dialect: The target dialect to convert to.

        Returns:
            The converted SQL query string.

        Raises:
            SQLConversionError: If the conversion fails.
        """

        try:
            # Parse the SQL query
            parsed = parse_one(sql, dialect=self.dialect)

            # Convert to the target dialect
            return parsed.sql(dialect=to_dialect, pretty=True)
        except Exception as e:
            error_msg = f"Failed to convert SQL: {e!s}"
            raise SQLConversionError(error_msg) from e
