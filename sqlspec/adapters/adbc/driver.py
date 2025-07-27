# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias


from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

# Type handlers removed - ADBC has good native type support

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AdbcConnection: TypeAlias = Connection
else:
    AdbcConnection = Connection


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
    parameter_config = DriverParameterConfig(
        paramstyle=ParameterStyle.QMARK, type_coercion_map={}, has_native_list_expansion=True
    )

    def __init__(self, connection: "AdbcConnection", config: "Optional[SQLConfig]" = None) -> None:
        dialect = self._get_dialect(connection)
        if config and not config.dialect:
            config = config.replace(dialect=dialect)
        elif not config:
            config = SQLConfig(dialect=dialect)

        super().__init__(connection=connection, config=config)
        self.dialect: DialectType = dialect

        # Set up parameter configuration based on dialect
        default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
            self.dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
        )

        # Override the class parameter_config for this instance
        object.__setattr__(
            self,
            "parameter_config",
            DriverParameterConfig(
                supported_parameter_styles=supported_styles,
                default_parameter_style=default_style,
                type_coercion_map={
                    # ADBC has good native type support across dialects
                    # Add any specific type mappings as needed
                },
                has_native_list_expansion=True,  # ADBC handles lists natively
            ),
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

    @contextmanager
    def with_cursor(self, connection: "AdbcConnection") -> "Iterator[Cursor]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _perform_execute(self, cursor: "Cursor", statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        try:
            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self._prepare_driver_parameters(params)
                # Handle PostgreSQL empty params
                prepared_params = self._handle_postgres_empty_params(prepared_params)
                cursor_params = self._prepare_cursor_parameters(prepared_params)
                self._execute_with_params(cursor, sql, cursor_params)
        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

    def _execute_with_params(self, cursor: "Cursor", sql: str, params: "list[Any]") -> None:
        """Execute SQL with proper parameter handling."""
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

    def _build_select_result(self, cursor: "Cursor", statement: "SQL") -> "SQLResult":
        """Build result for SELECT operations."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data  # type: ignore[assignment]

        return SQLResult(
            statement=statement,
            data=cast("list[dict[str, Any]]", dict_data),
            column_names=column_names,
            rows_affected=len(dict_data),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: "Cursor", statement: "SQL") -> "SQLResult":
        """Build result for modification operations."""
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount if cursor.rowcount is not None else -1,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    def _build_result(self, cursor: "Cursor", statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution."""
        if self.returns_rows(statement.expression):
            return self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin database transaction."""
        conn = connection or self.connection
        # ADBC transaction handling varies by backend
        if hasattr(conn, "begin"):
            conn.begin()
        elif hasattr(conn, "autocommit"):
            conn.autocommit = False

    def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback database transaction."""
        conn = connection or self.connection
        if hasattr(conn, "rollback"):
            conn.rollback()

    def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit database transaction."""
        conn = connection or self.connection
        if hasattr(conn, "commit"):
            conn.commit()

    def _prepare_driver_parameters(self, parameters: "Any") -> "Any":
        """Prepare parameters for the ADBC driver."""
        return parameters

    def _prepare_cursor_parameters(self, parameters: "Any") -> "list[Any]":
        """Convert parameters to the format expected by ADBC cursor."""
        if parameters is None:
            return []
        if isinstance(parameters, (list, tuple)):
            return list(parameters)
        if isinstance(parameters, dict) and not parameters:
            return []
        return [parameters]
