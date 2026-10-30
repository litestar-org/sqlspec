"""Minimal synchronous driver adapter skeleton implementing SyncDriverAdapterBase."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._exception_handler import BaseSyncExceptionHandler

if TYPE_CHECKING:
    from sqlspec.core import SQL
    from sqlspec.driver import ExecutionResult, SyncDataDictionaryBase

__all__ = ("ExampleDriver", "ExampleExceptionHandler", "main")


class ExampleExceptionHandler(BaseSyncExceptionHandler):
    """Adapter-specific exception handler wrapping database errors."""


class ExampleDriver(SyncDriverAdapterBase):
    """Minimal synchronous database driver adapter skeleton."""

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Return the data dictionary instance for schema inspection."""
        raise NotImplementedError

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a single statement and return execution metadata."""
        raise NotImplementedError

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute a statement with multiple parameter sets."""
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
        """Acquire and yield a cursor from the active connection.

        Args:
            connection: Active database connection.

        Yields:
            Database cursor bound to the connection.
        """
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def handle_database_exceptions(self) -> ExampleExceptionHandler:
        """Provide the exception handler context manager."""
        return ExampleExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        """Check whether the active connection is in an open transaction."""
        return False


def main() -> type[ExampleDriver]:
    """Verify the driver adapter skeleton defines required methods."""
    assert callable(ExampleDriver.dispatch_execute)
    assert callable(ExampleDriver.handle_database_exceptions)
    return ExampleDriver


if __name__ == "__main__":
    print(main().__name__)
