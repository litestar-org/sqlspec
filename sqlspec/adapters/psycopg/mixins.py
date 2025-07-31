"""Psycopg-specific mixins for shared functionality."""

from typing import TYPE_CHECKING, Any

from mypy_extensions import trait

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL

__all__ = ("PsycopgCopyMixin",)


@trait
class PsycopgCopyMixin:
    """Mixin for PostgreSQL COPY operation handling.

    Provides shared COPY logic for both sync and async Psycopg drivers,
    eliminating code duplication between PsycopgSyncDriver and PsycopgAsyncDriver.
    """

    __slots__ = ()

    def _handle_copy_operation_from_pipeline(self, cursor: Any, statement: "SQL") -> Any:
        """Handle PostgreSQL COPY operations using pipeline metadata.

        This method provides the core COPY handling logic that is shared between
        sync and async drivers. The actual cursor.copy() calls must be handled
        by the specific driver implementations.

        Args:
            cursor: Database cursor (sync or async)
            statement: SQL statement with COPY metadata from pipeline
        """
        # Get the original SQL from pipeline metadata
        metadata = statement._processing_context.metadata if statement._processing_context else {}
        sql_text = metadata.get("postgres_copy_original_sql")
        if not sql_text:
            # Fallback to expression
            sql_text = str(statement.expression)

        # Get the raw COPY data from pipeline metadata
        copy_data = metadata.get("postgres_copy_data")

        if copy_data:
            # Handle different parameter formats (positional or keyword)
            if isinstance(copy_data, dict):
                # For named parameters, assume single data value or concatenate all values
                if len(copy_data) == 1:
                    data_str = str(next(iter(copy_data.values())))
                else:
                    data_str = "\n".join(str(value) for value in copy_data.values())
            elif isinstance(copy_data, (list, tuple)):
                # For positional parameters, if single item, use as is, otherwise join
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            return self._execute_copy_with_data(cursor, sql_text, data_str)
        return self._execute_copy_without_data(cursor, sql_text)

    def _execute_copy_with_data(self, _cursor: Any, _sql_text: str, _data_str: str) -> Any:
        """Execute COPY operation with data.

        Must be implemented by concrete drivers for sync/async handling.

        Args:
            _cursor: Database cursor
            _sql_text: COPY SQL statement
            _data_str: Data to copy

        Returns:
            Driver-specific result
        """
        msg = "Concrete drivers must implement _execute_copy_with_data"
        raise NotImplementedError(msg)

    def _execute_copy_without_data(self, _cursor: Any, _sql_text: str) -> Any:
        """Execute COPY operation without data.

        Must be implemented by concrete drivers for sync/async handling.

        Args:
            _cursor: Database cursor
            _sql_text: COPY SQL statement

        Returns:
            Driver-specific result
        """
        msg = "Concrete drivers must implement _execute_copy_without_data"
        raise NotImplementedError(msg)
