from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._exception_handler import BaseSyncExceptionHandler

if TYPE_CHECKING:
    from sqlspec.core import SQL
    from sqlspec.driver import ExecutionResult, SyncDataDictionaryBase

__all__ = ("ExampleDriver", "ExampleExceptionHandler", "test_new_adapter")


# start-example
class ExampleExceptionHandler(BaseSyncExceptionHandler):
    """Adapter-specific exception handler wrapping database errors."""


class ExampleDriver(SyncDriverAdapterBase):
    """Example synchronous database driver adapter."""

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:
        """Return the data dictionary instance for schema inspection.

        Returns:
            Data dictionary instance.
        """
        raise NotImplementedError

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single statement and return execution metadata.

        Args:
            cursor: Database cursor.
            statement: SQL statement to execute.

        Returns:
            Execution result metadata.
        """
        raise NotImplementedError

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a statement with multiple parameter sets.

        Args:
            cursor: Database cursor.
            statement: SQL statement with batched parameters.

        Returns:
            Execution result metadata.
        """
        raise NotImplementedError

    def begin(self) -> None:
        """Begin a transaction on the current connection."""
        raise NotImplementedError

    def commit(self) -> None:
        """Commit the current transaction."""
        raise NotImplementedError

    def rollback(self) -> None:
        """Roll back the current transaction."""
        raise NotImplementedError

    @contextmanager
    def with_cursor(self, connection: Any) -> Generator[Any, None, None]:
        """Acquire and yield a cursor from the connection.

        Args:
            connection: Active database connection.

        Yields:
            Database cursor.
        """
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def handle_database_exceptions(self) -> ExampleExceptionHandler:
        """Provide the exception handler context manager.

        Returns:
            Exception handler instance.
        """
        return ExampleExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        """Check whether the active connection is in an open transaction.

        Returns:
            True if in a transaction, False otherwise.
        """
        return False


# end-example


def test_new_adapter() -> None:
    """Validate that the example driver skeleton defines required methods."""
    assert ExampleDriver._connection_in_transaction is not None
