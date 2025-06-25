from collections.abc import Generator
from contextlib import contextmanager
from enum import Enum
from typing import Any, Optional, Union, cast

__all__ = (
    "ExtraParameterError",
    "FileNotFoundInStorageError",
    "ImproperConfigurationError",
    "IntegrityError",
    "MissingDependencyError",
    "MissingParameterError",
    "MultipleResultsFoundError",
    "NotFoundError",
    "ParameterError",
    "ParameterStyleMismatchError",
    "PipelineExecutionError",
    "QueryError",
    "RepositoryError",
    "RiskLevel",
    "SQLBuilderError",
    "SQLConversionError",
    "SQLFileNotFoundError",
    "SQLFileParseError",
    "SQLFileParsingError",
    "SQLInjectionError",
    "SQLParsingError",
    "SQLSpecError",
    "SQLTransformationError",
    "SQLValidationError",
    "SerializationError",
    "StorageOperationFailedError",
    "UnknownParameterError",
    "UnsafeSQLError",
)


class SQLSpecError(Exception):
    """Base exception class from which all Advanced Alchemy exceptions inherit."""

    detail: str

    def __init__(self, *args: Any, detail: str = "") -> None:
        """Initialize ``AdvancedAlchemyException``.

        Args:
            *args: args are converted to :class:`str` before passing to :class:`Exception`
            detail: detail of the exception.
        """
        str_args = [str(arg) for arg in args if arg]
        if not detail:
            if str_args:
                detail, *str_args = str_args
            elif hasattr(self, "detail"):
                detail = self.detail
        self.detail = detail
        super().__init__(*str_args)

    def __repr__(self) -> str:
        if self.detail:
            return f"{self.__class__.__name__} - {self.detail}"
        return self.__class__.__name__

    def __str__(self) -> str:
        return " ".join((*self.args, self.detail)).strip()


class MissingDependencyError(SQLSpecError, ImportError):
    """Missing optional dependency.

    This exception is raised only when a module depends on a dependency that has not been installed.
    """

    def __init__(self, package: str, install_package: Optional[str] = None) -> None:
        super().__init__(
            f"Package {package!r} is not installed but required. You can install it by running "
            f"'pip install sqlspec[{install_package or package}]' to install sqlspec with the required extra "
            f"or 'pip install {install_package or package}' to install the package separately"
        )


class BackendNotRegisteredError(SQLSpecError):
    """Raised when a requested storage backend key is not registered."""

    def __init__(self, backend_key: str) -> None:
        super().__init__(f"Storage backend '{backend_key}' is not registered. Please register it before use.")


class SQLLoadingError(SQLSpecError):
    """Issues loading referenced SQL file."""

    def __init__(self, message: Optional[str] = None) -> None:
        if message is None:
            message = "Issues loading referenced SQL file."
        super().__init__(message)


class SQLParsingError(SQLSpecError):
    """Issues parsing SQL statements."""

    def __init__(self, message: Optional[str] = None) -> None:
        if message is None:
            message = "Issues parsing SQL statement."
        super().__init__(message)


class SQLFileParsingError(SQLSpecError):
    """Issues parsing SQL files."""

    def __init__(self, message: Optional[str] = None) -> None:
        if message is None:
            message = "Issues parsing SQL files."
        super().__init__(message)


class SQLBuilderError(SQLSpecError):
    """Issues Building or Generating SQL statements."""

    def __init__(self, message: Optional[str] = None) -> None:
        if message is None:
            message = "Issues building SQL statement."
        super().__init__(message)


class SQLConversionError(SQLSpecError):
    """Issues converting SQL statements."""

    def __init__(self, message: Optional[str] = None) -> None:
        if message is None:
            message = "Issues converting SQL statement."
        super().__init__(message)


# -- SQL Validation Errors --
class RiskLevel(Enum):
    """SQL risk assessment levels."""

    SKIP = 1
    SAFE = 2
    LOW = 3
    MEDIUM = 4
    HIGH = 5
    CRITICAL = 6

    def __str__(self) -> str:
        """String representation.

        Returns:
            Lowercase name of the style.
        """
        return self.name.lower()

    def __lt__(self, other: "RiskLevel") -> bool:  # pragma: no cover
        """Less than comparison for ordering."""
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return self.value < other.value

    def __le__(self, other: "RiskLevel") -> bool:  # pragma: no cover
        """Less than or equal comparison for ordering."""
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return self.value <= other.value

    def __gt__(self, other: "RiskLevel") -> bool:  # pragma: no cover
        """Greater than comparison for ordering."""
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return self.value > other.value

    def __ge__(self, other: "RiskLevel") -> bool:  # pragma: no cover
        """Greater than or equal comparison for ordering."""
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return self.value >= other.value


class SQLValidationError(SQLSpecError):
    """Base class for SQL validation errors."""

    sql: Optional[str]
    risk_level: RiskLevel

    def __init__(self, message: str, sql: Optional[str] = None, risk_level: RiskLevel = RiskLevel.MEDIUM) -> None:
        """Initialize with SQL context and risk level."""
        detail_message = message
        if sql is not None:
            detail_message = f"{message}\nSQL: {sql}"
        super().__init__(detail=detail_message)
        self.sql = sql
        self.risk_level = risk_level


class SQLTransformationError(SQLSpecError):
    """Base class for SQL transformation errors."""

    sql: Optional[str]

    def __init__(self, message: str, sql: Optional[str] = None) -> None:
        """Initialize with SQL context and risk level."""
        detail_message = message
        if sql is not None:
            detail_message = f"{message}\nSQL: {sql}"
        super().__init__(detail=detail_message)
        self.sql = sql


class SQLInjectionError(SQLValidationError):
    """Raised when potential SQL injection is detected."""

    pattern: Optional[str]

    def __init__(self, message: str, sql: Optional[str] = None, pattern: Optional[str] = None) -> None:
        """Initialize with injection pattern context."""
        detail_message = message
        if pattern:
            detail_message = f"{message} (Pattern: {pattern})"
        super().__init__(detail_message, sql, RiskLevel.CRITICAL)
        self.pattern = pattern


class UnsafeSQLError(SQLValidationError):
    """Raised when unsafe SQL constructs are detected."""

    construct: Optional[str]

    def __init__(self, message: str, sql: Optional[str] = None, construct: Optional[str] = None) -> None:
        """Initialize with unsafe construct context."""
        detail_message = message
        if construct:
            detail_message = f"{message} (Construct: {construct})"
        super().__init__(detail_message, sql, RiskLevel.HIGH)
        self.construct = construct


# -- SQL Query Errors --
class QueryError(SQLSpecError):
    """Base class for Query errors."""


# -- SQL Parameter Errors --
class ParameterError(SQLSpecError):
    """Base class for parameter-related errors."""

    sql: Optional[str]

    def __init__(self, message: str, sql: Optional[str] = None) -> None:
        """Initialize with optional SQL context."""
        detail_message = message
        if sql is not None:
            detail_message = f"{message}\nSQL: {sql}"
        super().__init__(detail=detail_message)
        self.sql = sql


class UnknownParameterError(ParameterError):
    """Raised when encountering unknown parameter syntax."""


class MissingParameterError(ParameterError):
    """Raised when required parameters are missing."""


class ExtraParameterError(ParameterError):
    """Raised when extra parameters are provided."""


class ParameterStyleMismatchError(SQLSpecError):
    """Error when parameter style doesn't match SQL placeholder style.

    This exception is raised when there's a mismatch between the parameter type
    (dictionary, tuple, etc.) and the placeholder style in the SQL query
    (named, positional, etc.).
    """

    sql: Optional[str]

    def __init__(self, message: Optional[str] = None, sql: Optional[str] = None) -> None:
        final_message = message
        if final_message is None:
            final_message = (
                "Parameter style mismatch: dictionary parameters provided but no named placeholders found in SQL."
            )

        detail_message = final_message
        if sql:
            detail_message = f"{final_message}\nSQL: {sql}"

        super().__init__(detail=detail_message)
        self.sql = sql


class ImproperConfigurationError(SQLSpecError):
    """Improper Configuration error.

    This exception is raised only when a module depends on a dependency that has not been installed.
    """


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


class StorageOperationFailedError(SQLSpecError):
    """Raised when a storage backend operation fails (e.g., network, permission, API error)."""


class FileNotFoundInStorageError(StorageOperationFailedError):
    """Raised when a file or object is not found in the storage backend."""


class SQLFileNotFoundError(SQLSpecError):
    """Raised when a SQL file cannot be found."""

    def __init__(self, name: str, path: "Optional[str]" = None) -> None:
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


@contextmanager
def wrap_exceptions(
    wrap_exceptions: bool = True, suppress: "Optional[Union[type[Exception], tuple[type[Exception], ...]]]" = None
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
        # Handle suppression first
        if suppress is not None and (
            (isinstance(suppress, type) and isinstance(exc, suppress))
            or (isinstance(suppress, tuple) and isinstance(exc, suppress))
        ):
            return  # Suppress this exception

        # If it's already a SQLSpec exception, don't wrap it
        if isinstance(exc, SQLSpecError):
            raise

        # Handle wrapping
        if wrap_exceptions is False:
            raise
        msg = "An error occurred during the operation."
        raise RepositoryError(detail=msg) from exc


class PipelineExecutionError(SQLSpecError):
    """Rich error information for pipeline execution failures."""

    def __init__(
        self,
        message: str,
        *,
        operation_index: "Optional[int]" = None,
        failed_operation: "Optional[Any]" = None,
        partial_results: "Optional[list[Any]]" = None,
        driver_error: "Optional[Exception]" = None,
    ) -> None:
        """Initialize the pipeline execution error.

        Args:
            message: Error message describing the failure
            operation_index: Index of the operation that failed
            failed_operation: The PipelineOperation that failed
            partial_results: Results from operations that succeeded before the failure
            driver_error: Original exception from the database driver
        """
        super().__init__(message)
        self.operation_index = operation_index
        self.failed_operation = failed_operation
        self.partial_results = partial_results or []
        self.driver_error = driver_error

    def get_failed_sql(self) -> "Optional[str]":
        """Get the SQL that failed for debugging."""
        if self.failed_operation and hasattr(self.failed_operation, "sql"):
            return cast("str", self.failed_operation.sql.to_sql())
        return None

    def get_failed_parameters(self) -> "Optional[Any]":
        """Get the parameters that failed."""
        if self.failed_operation and hasattr(self.failed_operation, "original_params"):
            return self.failed_operation.original_params
        return None
