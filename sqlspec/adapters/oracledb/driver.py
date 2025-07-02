from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT, SQLParameterType
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = get_logger("adapters.oracledb")


def _process_oracle_parameters(params: Any) -> Any:
    """Process parameters to handle Oracle-specific requirements.

    - Extract values from TypedParameter objects
    - Convert tuples to lists (Oracle doesn't support tuples)
    """
    from sqlspec.statement.parameters import TypedParameter

    if params is None:
        return None

    if isinstance(params, TypedParameter):
        return _process_oracle_parameters(params.value)

    if isinstance(params, tuple):
        return [_process_oracle_parameters(item) for item in params]
    if isinstance(params, list):
        processed = []
        for param_set in params:
            if isinstance(param_set, (tuple, list)):
                processed.append([_process_oracle_parameters(item) for item in param_set])
            else:
                processed.append(_process_oracle_parameters(param_set))
        return processed
    if isinstance(params, dict):
        return {key: _process_oracle_parameters(value) for key, value in params.items()}
    return params


class OracleSyncDriver(
    SyncDriverAdapterProtocol[OracleSyncConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "oracle"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.NAMED_COLON,
        ParameterStyle.POSITIONAL_COLON,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.NAMED_COLON
    support_native_arrow_export = True
    __slots__ = ()

    def __init__(
        self,
        connection: OracleSyncConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _process_parameters(self, parameters: "SQLParameterType") -> "SQLParameterType":
        """Process parameters to handle Oracle-specific requirements.

        - Extract values from TypedParameter objects
        - Convert tuples to lists (Oracle doesn't support tuples)
        """
        return _process_oracle_parameters(parameters)

    @contextmanager
    def _get_cursor(self, connection: Optional[OracleSyncConnection] = None) -> Generator[Cursor, None, None]:
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_statement(
        self, statement: SQL, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        target_style = self.default_parameter_style

        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            target_style = self.default_parameter_style
        elif detected_styles:
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, params = statement.compile(placeholder_style=target_style)
            params = self._process_parameters(params)
            # Oracle doesn't like underscores in bind parameter names
            if isinstance(params, list) and params and isinstance(params[0], dict):
                # Fix the SQL and parameters
                for key in list(params[0].keys()):
                    if key.startswith("_arg_"):
                        new_key = key[1:].replace("_", "")
                        sql = sql.replace(f":{key}", f":{new_key}")
                        for param_set in params:
                            if isinstance(param_set, dict) and key in param_set:
                                param_set[new_key] = param_set.pop(key)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        # Oracle doesn't like underscores in bind parameter names
        if isinstance(params, dict):
            # Fix the SQL and parameters
            for key in list(params.keys()):
                if key.startswith("_arg_"):
                    new_key = key[1:].replace("_", "")
                    sql = sql.replace(f":{key}", f":{new_key}")
                    params[new_key] = params.pop(key)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            processed_params = self._process_parameters(parameters) if parameters else []
            cursor.execute(sql, processed_params)

            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                return SQLResult(
                    statement=statement,
                    data=fetched_data,
                    column_names=column_names,
                    rows_affected=cursor.rowcount,
                    operation_type="SELECT",
                )

            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=cursor.rowcount,
                operation_type=self._determine_operation_type(statement),
                metadata={"status_message": "OK"},
            )

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            if param_list is None:
                param_list = []
            elif param_list and not isinstance(param_list, list):
                # Single parameter set, wrap it
                param_list = [param_list]
            elif param_list and not isinstance(param_list[0], (list, tuple, dict)):
                # Already a flat list, likely from incorrect usage
                param_list = [param_list]
            # Parameters have already been processed in _execute_statement
            cursor.executemany(sql, param_list)
            return SQLResult(
                statement=SQL(sql),
                data=[],
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _execute_script(
        self, script: str, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        statements = self._split_script_statements(script, strip_trailing_semicolon=True)
        with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement and statement.strip():
                    cursor.execute(statement.strip())

        return SQLResult(
            statement=SQL(script),
            data=[],
            rows_affected=0,
            operation_type="SCRIPT",
            metadata={"status_message": "SCRIPT EXECUTED"},
            total_statements=len(statements),
            successful_statements=len(statements),
        )

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        # For fetch_arrow_table, we need to use POSITIONAL_COLON style since the SQL has :1 placeholders
        sql_str, params = sql.compile(placeholder_style=ParameterStyle.POSITIONAL_COLON)
        if params is None:
            params = []

        processed_params = self._process_parameters(params) if params else []

        oracle_df = conn.fetch_df_all(sql_str, processed_params)
        from pyarrow.interchange.from_dataframe import from_dataframe

        arrow_table = from_dataframe(oracle_df)

        return ArrowResult(statement=sql, data=arrow_table)

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)

        with self._get_cursor(conn) as cursor:
            if mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for oracledb ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: :1, :2, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join(f":{i + 1}" for i in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount


class OracleAsyncDriver(
    AsyncDriverAdapterProtocol[OracleAsyncConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: DialectType = "oracle"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.NAMED_COLON,
        ParameterStyle.POSITIONAL_COLON,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.NAMED_COLON
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False
    __slots__ = ()

    def __init__(
        self,
        connection: OracleAsyncConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _process_parameters(self, parameters: "SQLParameterType") -> "SQLParameterType":
        """Process parameters to handle Oracle-specific requirements.

        - Extract values from TypedParameter objects
        - Convert tuples to lists (Oracle doesn't support tuples)
        """
        return _process_oracle_parameters(parameters)

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[OracleAsyncConnection] = None
    ) -> AsyncGenerator[AsyncCursor, None]:
        conn_to_use = connection or self.connection
        cursor: AsyncCursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await ensure_async_(cursor.close)()

    async def _execute_statement(
        self, statement: SQL, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        target_style = self.default_parameter_style

        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            target_style = self.default_parameter_style
        elif detected_styles:
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, params = statement.compile(placeholder_style=target_style)
            params = self._process_parameters(params)
            # Oracle doesn't like underscores in bind parameter names
            if isinstance(params, list) and params and isinstance(params[0], dict):
                # Fix the SQL and parameters
                for key in list(params[0].keys()):
                    if key.startswith("_arg_"):
                        new_key = key[1:].replace("_", "")
                        sql = sql.replace(f":{key}", f":{new_key}")
                        for param_set in params:
                            if isinstance(param_set, dict) and key in param_set:
                                param_set[new_key] = param_set.pop(key)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        # Oracle doesn't like underscores in bind parameter names
        if isinstance(params, dict):
            # Fix the SQL and parameters
            for key in list(params.keys()):
                if key.startswith("_arg_"):
                    new_key = key[1:].replace("_", "")
                    sql = sql.replace(f":{key}", f":{new_key}")
                    params[new_key] = params.pop(key)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            if parameters is None:
                await cursor.execute(sql)
            else:
                processed_params = self._process_parameters(parameters)
                await cursor.execute(sql, processed_params)

            # For SELECT statements, extract data while cursor is open
            if self.returns_rows(statement.expression):
                fetched_data = await cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                return SQLResult(
                    statement=statement,
                    data=fetched_data,
                    column_names=column_names,
                    rows_affected=cursor.rowcount,
                    operation_type="SELECT",
                )

            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=cursor.rowcount,
                operation_type=self._determine_operation_type(statement),
                metadata={"status_message": "OK"},
            )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            if param_list is None:
                param_list = []
            elif param_list and not isinstance(param_list, list):
                # Single parameter set, wrap it
                param_list = [param_list]
            elif param_list and not isinstance(param_list[0], (list, tuple, dict)):
                # Already a flat list, likely from incorrect usage
                param_list = [param_list]
            # Parameters have already been processed in _execute_statement
            await cursor.executemany(sql, param_list)
            return SQLResult(
                statement=SQL(sql),
                data=[],
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    async def _execute_script(
        self, script: str, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)
        # Oracle doesn't support multi-statement scripts in a single execute
        # The splitter now handles PL/SQL blocks correctly when strip_trailing_semicolon=True
        statements = self._split_script_statements(script, strip_trailing_semicolon=True)

        async with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement and statement.strip():
                    await cursor.execute(statement.strip())

        return SQLResult(
            statement=SQL(script),
            data=[],
            rows_affected=0,
            operation_type="SCRIPT",
            metadata={"status_message": "SCRIPT EXECUTED"},
            total_statements=len(statements),
            successful_statements=len(statements),
        )

    async def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        # For fetch_arrow_table, we need to use POSITIONAL_COLON style since the SQL has :1 placeholders
        sql_str, params = sql.compile(placeholder_style=ParameterStyle.POSITIONAL_COLON)
        if params is None:
            params = []

        processed_params = self._process_parameters(params) if params else []

        oracle_df = await conn.fetch_df_all(sql_str, processed_params)
        from pyarrow.interchange.from_dataframe import from_dataframe

        arrow_table = from_dataframe(oracle_df)

        return ArrowResult(statement=sql, data=arrow_table)

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)

        async with self._get_cursor(conn) as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for oracledb ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: :1, :2, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join(f":{i + 1}" for i in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            await cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount
