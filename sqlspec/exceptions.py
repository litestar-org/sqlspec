from collections.abc import Generator
from contextlib import contextmanager
from enum import Enum, auto
from typing import Any, Optional

__all__ = (
    "ExtraParameterError",
    "ImproperConfigurationError",
    "IntegrityError",
    "MissingDependencyError",
    "MissingParameterError",
    "MultipleResultsFoundError",
    "NotFoundError",
    "ParameterError",
    "ParameterStyleMismatchError",
    "QueryError",
    "RepositoryError",
    "RiskLevel",
    "SQLBuilderError",
    "SQLConversionError",
    "SQLInjectionError",
    "SQLParsingError",
    "SQLSpecError",
    "SQLTransformationError",
    "SQLValidationError",
    "SerializationError",
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
            f"or 'pip install {install_package or package}' to install the package separately",
        )


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

    SKIP = auto()
    SAFE = auto()
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()

    def __str__(self) -> str:
        """String representation.

        Returns:
            Lowercase name of the style.
        """
        return self.name.lower()


class SQLValidationError(SQLSpecError):
    """Base class for SQL validation errors."""

    sql: Optional[str]
    risk_level: RiskLevel

    def __init__(self, message: str, sql: Optional[str] = None, risk_level: RiskLevel = RiskLevel.MEDIUM) -> None:
        """Initialize with SQL context and risk level."""
        detail_message = message
        if sql:
            detail_message = f"{message}\\nSQL: {sql}"
        super().__init__(detail=detail_message)
        self.sql = sql
        self.risk_level = risk_level


class SQLTransformationError(SQLSpecError):
    """Base class for SQL transformation errors."""

    sql: Optional[str]

    def __init__(self, message: str, sql: Optional[str] = None) -> None:
        """Initialize with SQL context and risk level."""
        detail_message = message
        if sql:
            detail_message = f"{message}\\nSQL: {sql}"
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
        if sql:
            detail_message = f"{message}\\nSQL: {sql}"
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


@contextmanager
def wrap_exceptions(wrap_exceptions: bool = True) -> Generator[None, None, None]:
    try:
        yield

    except Exception as exc:
        if wrap_exceptions is False:
            raise
        msg = "An error occurred during the operation."
        raise RepositoryError(detail=msg) from exc
