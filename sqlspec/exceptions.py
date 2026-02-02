from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Final

STACK_SQL_PREVIEW_LIMIT: Final[int] = 120

__all__ = (
    "SQLSTATE_EXCEPTION_MAP",
    "CheckViolationError",
    "ConfigResolverError",
    "ConnectionTimeoutError",
    "DataError",
    "DatabaseConnectionError",
    "DeadlockError",
    "DialectNotSupportedError",
    "EventChannelError",
    "FileNotFoundInStorageError",
    "ForeignKeyViolationError",
    "ImproperConfigurationError",
    "IntegrityError",
    "InvalidVersionFormatError",
    "MigrationError",
    "MissingDependencyError",
    "MultipleResultsFoundError",
    "NotFoundError",
    "NotNullViolationError",
    "OperationalError",
    "OutOfOrderMigrationError",
    "PermissionDeniedError",
    "QueryTimeoutError",
    "RepositoryError",
    "SQLBuilderError",
    "SQLConversionError",
    "SQLFileNotFoundError",
    "SQLFileParseError",
    "SQLParsingError",
    "SQLSpecError",
    "SerializationConflictError",
    "SerializationError",
    "SquashValidationError",
    "StackExecutionError",
    "StorageCapabilityError",
    "StorageOperationFailedError",
    "TransactionError",
    "TransactionRetryError",
    "UniqueViolationError",
    "map_sqlstate_to_exception",
)


class SQLSpecError(Exception):
    """Base exception class for SQLSpec exceptions."""

    detail: str = ""

    def __init__(self, *args: Any, detail: str = "") -> None:
        """Initialize SQLSpecError.

        Args:
            *args: args are converted to :class:`str` before passing to :class:`Exception`
            detail: detail of the exception.
        """
        str_args = [str(arg) for arg in args if arg]
        if not detail:
            detail = str_args[0] if str_args else ""
        self.detail = detail
        if detail and detail not in str_args:
            str_args = [detail, *str_args]
        super().__init__(*str_args)

    def __repr__(self) -> str:
        if self.detail:
            return f"{self.__class__.__name__} - {self.detail}"
        return self.__class__.__name__

    def __str__(self) -> str:
        parts = list(self.args)
        if self.detail and self.detail not in self.args:
            parts.append(self.detail)
        return " ".join(parts).strip()


class MissingDependencyError(SQLSpecError):
    """Raised when a required dependency is not installed."""

    def __init__(self, package: str, install_package: str | None = None) -> None:
        super().__init__(
            f"Package {package!r} is not installed but required. You can install it by running "
            f"'pip install sqlspec[{install_package or package}]' to install sqlspec with the required extra "
            f"or 'pip install {install_package or package}' to install the package separately"
        )


class BackendNotRegisteredError(SQLSpecError):
    """Raised when a requested storage backend key is not registered."""

    def __init__(self, backend_key: str) -> None:
        super().__init__(f"Storage backend '{backend_key}' is not registered. Please register it before use.")


class EventChannelError(SQLSpecError):
    """Raised when event channel operations fail."""


class ConfigResolverError(SQLSpecError):
    """Exception raised when config resolution fails."""


class SQLParsingError(SQLSpecError):
    """Issues parsing SQL statements."""

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = "Issues parsing SQL statement."
        super().__init__(message)


class SQLBuilderError(SQLSpecError):
    """Issues Building or Generating SQL statements."""

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = "Issues building SQL statement."
        super().__init__(message)


class SQLConversionError(SQLSpecError):
    """Issues converting SQL statements."""

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = "Issues converting SQL statement."
        super().__init__(message)


class ImproperConfigurationError(SQLSpecError):
    """Raised when configuration is invalid or incomplete."""


class DialectNotSupportedError(SQLBuilderError):
    """Raised when a SQL dialect does not support a specific feature."""


class SerializationError(SQLSpecError):
    """Encoding or decoding of an object failed."""


class RepositoryError(SQLSpecError):
    """Base repository exception type."""


class IntegrityError(RepositoryError):
    """Data integrity error."""


class NotFoundError(RepositoryError):
    """An identity does not exist."""


class MultipleResultsFoundError(RepositoryError):
    """A single database result was required but more than one were found."""


class UniqueViolationError(IntegrityError):
    """A unique constraint was violated."""


class ForeignKeyViolationError(IntegrityError):
    """A foreign key constraint was violated."""


class CheckViolationError(IntegrityError):
    """A check constraint was violated."""


class NotNullViolationError(IntegrityError):
    """A not-null constraint was violated."""


class DatabaseConnectionError(SQLSpecError):
    """Database connection error (invalid credentials, network failure, etc.)."""


class PermissionDeniedError(DatabaseConnectionError):
    """Database access denied due to insufficient privileges.

    Raised when:
    - User lacks privileges for the operation (SQLSTATE 42501)
    - Invalid credentials provided (SQLSTATE 28000/28P01)
    - Database access denied (MySQL 1044/1045/1142)
    - Oracle insufficient privileges (ORA-01031)
    """


class ConnectionTimeoutError(DatabaseConnectionError):
    """Database connection attempt timed out.

    Raised when:
    - TCP connection timeout to database server
    - DNS resolution timeout
    - SSL/TLS handshake timeout
    - Oracle connect timeout (ORA-12170)
    """


class TransactionError(SQLSpecError):
    """Transaction error (rollback, deadlock, serialization failure)."""


class SerializationConflictError(TransactionError):
    """Serialization conflict (SQLSTATE 40001) requiring retry."""


class TransactionRetryError(TransactionError):
    """Transaction failed after retries were exhausted."""


class DeadlockError(TransactionError):
    """Deadlock detected during transaction execution.

    Raised when:
    - PostgreSQL deadlock detected (SQLSTATE 40P01)
    - MySQL deadlock detected (Error 1213)
    - Oracle deadlock detected (ORA-00060)
    - SQLite database locked (SQLITE_LOCKED)

    Applications should typically retry the transaction when this error occurs.
    """


class DataError(SQLSpecError):
    """Invalid data type or format for database operation."""


class StackExecutionError(SQLSpecError):
    """Raised when a statement stack operation fails."""

    def __init__(
        self,
        operation_index: int,
        sql: str,
        original_error: Exception,
        *,
        adapter: str | None = None,
        mode: str = "fail-fast",
        native_pipeline: bool | None = None,
        downgrade_reason: str | None = None,
    ) -> None:
        pipeline_state = "enabled" if native_pipeline else "disabled"
        adapter_label = adapter or "unknown-adapter"
        preview = " ".join(sql.strip().split())
        if len(preview) > STACK_SQL_PREVIEW_LIMIT:
            preview = f"{preview[: STACK_SQL_PREVIEW_LIMIT - 3]}..."
        detail = (
            f"Stack operation {operation_index} failed on {adapter_label} "
            f"(mode={mode}, pipeline={pipeline_state}) sql={preview}"
        )
        super().__init__(detail)
        self.operation_index = operation_index
        self.sql = sql
        self.original_error = original_error
        self.adapter = adapter
        self.mode = mode
        self.native_pipeline = native_pipeline
        self.downgrade_reason = downgrade_reason

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base}: {self.original_error}" if self.original_error else base


class OperationalError(SQLSpecError):
    """Operational database error (timeout, disk full, resource limit)."""


class QueryTimeoutError(OperationalError):
    """Query execution timed out or was canceled.

    Raised when:
    - Statement timeout exceeded (SQLSTATE 57014)
    - Query canceled by user/operator
    - Lock wait timeout exceeded (MySQL 1205)
    - Oracle user requested cancel (ORA-01013)
    """


class StorageOperationFailedError(SQLSpecError):
    """Raised when a storage backend operation fails (e.g., network, permission, API error)."""


class StorageCapabilityError(SQLSpecError):
    """Raised when a requested storage bridge capability is unavailable."""

    def __init__(self, message: str, *, capability: str | None = None, remediation: str | None = None) -> None:
        parts = [message]
        if capability:
            parts.append(f"(capability: {capability})")
        if remediation:
            parts.append(remediation)
        detail = " ".join(parts)
        super().__init__(detail)
        self.capability = capability
        self.remediation = remediation


class FileNotFoundInStorageError(StorageOperationFailedError):
    """Raised when a file or object is not found in the storage backend."""


class SQLFileNotFoundError(SQLSpecError):
    """Raised when a SQL file cannot be found."""

    def __init__(self, name: str, path: "str | None" = None) -> None:
        """Initialize the error.

        Args:
            name: Name of the SQL file.
            path: Optional path where the file was expected.
        """
        message = f"SQL file '{name}' not found at path: {path}" if path else f"SQL file '{name}' not found"
        super().__init__(message)
        self.name = name
        self.path = path


class SQLFileParseError(SQLSpecError):
    """Raised when a SQL file cannot be parsed."""

    def __init__(self, name: str, path: str, original_error: "Exception") -> None:
        """Initialize the error.

        Args:
            name: Name of the SQL file.
            path: Path to the SQL file.
            original_error: The underlying parsing error.
        """
        message = f"Failed to parse SQL file '{name}' at {path}: {original_error}"
        super().__init__(message)
        self.name = name
        self.path = path
        self.original_error = original_error


class MigrationError(SQLSpecError):
    """Base exception for migration-related errors."""


class InvalidVersionFormatError(MigrationError):
    """Raised when a migration version format is invalid.

    Invalid formats include versions that don't match sequential (0001)
    or timestamp (YYYYMMDDHHmmss) patterns, or timestamps with invalid dates.
    """


class OutOfOrderMigrationError(MigrationError):
    """Raised when an out-of-order migration is detected in strict mode.

    Out-of-order migrations occur when a pending migration has a timestamp
    earlier than already-applied migrations, typically from late-merging branches.
    """


class SquashValidationError(MigrationError):
    """Raised when migration squash validation fails.

    Squash validation errors occur when:
    - Version range is invalid (start > end)
    - Gap detected in version sequence
    - Mixed migration types that cannot be squashed
    - Target file already exists
    """


# SQLSTATE class code length (first 2 characters of 5-character SQLSTATE)
SQLSTATE_CLASS_CODE_LEN: Final[int] = 2

# SQLSTATE to exception mapping for database-agnostic error translation
SQLSTATE_EXCEPTION_MAP: Final[dict[str, type[SQLSpecError]]] = {
    # Exact SQLSTATE matches (5 characters) - most specific
    "23505": UniqueViolationError,
    "23503": ForeignKeyViolationError,
    "23502": NotNullViolationError,
    "23514": CheckViolationError,
    "40001": SerializationConflictError,
    "40P01": DeadlockError,
    "57014": QueryTimeoutError,
    # Class-level matches (2 characters) - broader categories
    "02": NotFoundError,
    "08": DatabaseConnectionError,
    "22": DataError,
    "23": IntegrityError,
    "28": PermissionDeniedError,
    "40": TransactionError,
    "42": SQLParsingError,
    "53": OperationalError,
    "54": OperationalError,
    "55": OperationalError,
    "57": OperationalError,
    "58": OperationalError,
}


def map_sqlstate_to_exception(sqlstate: str | None) -> type[SQLSpecError] | None:
    """Map a SQLSTATE code to a SQLSpec exception class.

    Checks in order of specificity:
    1. Exact 5-character match (e.g., "23505" → UniqueViolationError)
    2. 2-character class match (e.g., "23" → IntegrityError)

    Args:
        sqlstate: 5-character SQLSTATE code (e.g., "23505")

    Returns:
        Matching exception class or None if not mapped
    """
    if not sqlstate:
        return None

    # Cache global in local for faster access in mypyc
    exc_map = SQLSTATE_EXCEPTION_MAP

    # Single lookup instead of in + []
    if exc_class := exc_map.get(sqlstate):
        return exc_class

    # Class prefix lookup
    if len(sqlstate) >= SQLSTATE_CLASS_CODE_LEN and (exc_class := exc_map.get(sqlstate[:SQLSTATE_CLASS_CODE_LEN])):
        return exc_class

    return None


@contextmanager
def wrap_exceptions(
    wrap_exceptions: bool = True, suppress: "type[Exception] | tuple[type[Exception], ...] | None" = None
) -> Generator[None, None, None]:
    """Context manager for exception handling with optional suppression.

    Args:
        wrap_exceptions: If True, wrap exceptions in RepositoryError. If False, let them pass through.
        suppress: Exception type(s) to suppress completely (like contextlib.suppress).
                 If provided, these exceptions are caught and ignored.
    """
    try:
        yield

    except Exception as exc:
        if suppress is not None and (
            (isinstance(suppress, type) and isinstance(exc, suppress))
            or (isinstance(suppress, tuple) and isinstance(exc, suppress))
        ):
            return

        if isinstance(exc, SQLSpecError):
            raise

        if wrap_exceptions is False:
            raise
        msg = "An error occurred during the operation."
        raise RepositoryError(detail=msg) from exc
