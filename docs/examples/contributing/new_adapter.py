"""Minimal synchronous driver adapter skeleton implementing SyncDriverAdapterBase."""

# --8<-- [start:example]
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar

from sqlspec.adapters.sqlite.core import driver_profile
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.parameters import get_driver_profile, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._exception_handler import BaseSyncExceptionHandler
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.core import SQL, DriverParameterProfile
    from sqlspec.driver import ExecutionResult, SyncDataDictionaryBase

register_driver_profile("sqlite", driver_profile, allow_override=True)
EXAMPLE_DRIVER_PROFILE: "DriverParameterProfile" = get_driver_profile("sqlite")


class ExampleExceptionHandler(BaseSyncExceptionHandler):
    """Adapter-specific exception handler wrapping database errors."""

    __slots__ = ()

    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        """Map driver exceptions onto pending_exception."""
        _ = exc_type
        self.pending_exception = SQLSpecError(str(exc_val))
        return True


class ExampleDriver(SyncDriverAdapterBase):
    """Minimal synchronous database driver adapter skeleton."""

    __slots__ = ()
    dialect = "sqlite"
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )

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


# --8<-- [end:example]

__all__ = ("EXAMPLE_DRIVER_PROFILE", "ExampleDriver", "ExampleExceptionHandler", "main")


def main() -> type[ExampleDriver]:
    """Verify the driver adapter skeleton defines required methods and profile."""
    assert callable(ExampleDriver.dispatch_execute)
    assert callable(ExampleDriver.handle_database_exceptions)
    assert ExampleDriver.type_coercion_capabilities.datetime_binding == "iso_text"
    assert EXAMPLE_DRIVER_PROFILE.default_dialect == "sqlite"

    handler = ExampleExceptionHandler()
    with handler:
        msg = "boom"
        raise RuntimeError(msg)
    assert isinstance(handler.pending_exception, SQLSpecError)
    assert str(handler.pending_exception) == "boom"
    return ExampleDriver


if __name__ == "__main__":
    print(main().__name__)
