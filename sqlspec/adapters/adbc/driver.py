# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import logging
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias


from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig

# Type handlers removed - ADBC has good native type support

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AdbcConnection: TypeAlias = Connection
else:
    AdbcConnection = Connection


class _AdbcCursorManager:
    """Context manager for ADBC cursor management."""

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


DIALECT_PATTERNS = {
    "postgres": ["postgres", "postgresql"],
    "bigquery": ["bigquery"],
    "sqlite": ["sqlite", "flight", "flightsql"],
    "duckdb": ["duckdb"],
    "mysql": ["mysql"],
    "snowflake": ["snowflake"],
}

# Parameter style configurations
DIALECT_PARAMETER_STYLES = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC Sync Driver Adapter with modern architecture."""

    # Default parameter config - will be overridden in __init__ based on dialect
    parameter_config: DriverParameterConfig

    def __init__(self, connection: "AdbcConnection", config: "Optional[SQLConfig]" = None) -> None:
        dialect = self._get_dialect(connection)
        if config and not config.dialect:
            config = config.replace(dialect=dialect)
        elif not config:
            config = SQLConfig(dialect=dialect)

        super().__init__(connection=connection, config=config)
        self.dialect: DialectType = dialect
        default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
            self.dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
        )

        # Override the class parameter_config for this instance
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=supported_styles,
            default_parameter_style=default_style,
            type_coercion_map={
                # ADBC has good native type support across dialects
                # Add any specific type mappings as needed
            },
            has_native_list_expansion=True,  # ADBC handles lists natively
        )

        # Type handlers can be registered if the base class supports it
        # ADBC has good native type support, so we don't need explicit type handlers

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name."""
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            # Check against known patterns
            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    return dialect
        except Exception:
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Handle PostgreSQL rollback requirement after failed transaction."""
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")

    def _handle_postgres_empty_params(self, params: "Any") -> "Any":
        """Handle empty parameters for PostgreSQL to avoid struct type errors."""
        if self.dialect == "postgres" and isinstance(params, dict) and not params:
            return None
        return params

    def with_cursor(self, connection: "AdbcConnection") -> "_AdbcCursorManager":
        return _AdbcCursorManager(connection)

    def _perform_execute(self, cursor: "Cursor", statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""
        try:
            sql, parameters = statement.compile()
            if statement.is_script:
                statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
                for stmt in statements:
                    if stmt.strip():
                        cursor.execute(
                            stmt,
                            parameters=self._prepare_cursor_parameters(self._handle_postgres_empty_params(parameters)),
                        )
            elif statement.is_many:
                cursor.executemany(sql, parameters or [])
            else:
                cursor.execute(
                    sql, parameters=self._prepare_cursor_parameters(self._handle_postgres_empty_params(parameters))
                )
        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

    def _extract_select_data(self, cursor: "Cursor") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data  # type: ignore[assignment]

        return cast("list[dict[str, Any]]", dict_data), column_names, len(dict_data)

    def _extract_execute_rowcount(self, cursor: "Cursor") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else -1

    def begin(self) -> None:
        """Begin database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("ROLLBACK")

    def commit(self) -> None:
        """Commit database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("COMMIT")

    def _prepare_cursor_parameters(self, parameters: "Any") -> "list[Any]":
        """Convert parameters to the format expected by ADBC cursor."""
        if parameters is None:
            return []
        if isinstance(parameters, (list, tuple)):
            return list(parameters)
        if isinstance(parameters, dict) and not parameters:
            return []
        return [parameters]
