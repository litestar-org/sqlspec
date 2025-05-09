# ruff: noqa: PLR0915, C901, PLR0912, PLR0911
import contextlib
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast, overload

import sqlglot
from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp as sqlglot_exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.statement import AT_NAME_REGEX, DOLLAR_NAME_REGEX, PARAM_REGEX, QMARK_REGEX
from sqlspec.typing import ArrowTable, StatementParameterType

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SyncArrowBulkOperationsMixin["AdbcConnection"],
    SQLTranslatorMixin["AdbcConnection"],
    SyncDriverAdapterProtocol["AdbcConnection"],
):
    """ADBC Sync Driver Adapter."""

    connection: AdbcConnection
    __supports_arrow__: ClassVar[bool] = True
    dialect: str = "adbc"

    def __init__(self, connection: "AdbcConnection") -> None:
        """Initialize the ADBC driver adapter."""
        self.connection = connection

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        driver_name = connection.adbc_get_info()["vendor_name"].lower()
        if "postgres" in driver_name:
            return "postgres"
        if "bigquery" in driver_name:
            return "bigquery"
        if "sqlite" in driver_name:
            return "sqlite"
        if "duckdb" in driver_name:
            return "duckdb"
        if "mysql" in driver_name:
            return "mysql"
        if "snowflake" in driver_name:
            return "snowflake"
        return "postgres"  # default to postgresql dialect

    @staticmethod
    def _cursor(connection: "AdbcConnection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "AdbcConnection") -> Generator["Cursor", None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _extract_colon_param_names(self, sql: str) -> list[str]:
        try:
            param_names = self._extract_param_names_sqlglot(sql)

        except Exception:  # Catches sqlglot parsing errors or the deliberate RuntimeError  # noqa: BLE001
            msg = f"ADBC: sqlglot parsing for :name params failed. Falling back to PARAM_REGEX. SQL: {sql}"
            logger.debug(msg)
            param_names = self._extract_param_names_regex(sql)
        return param_names

    @staticmethod
    def _extract_param_names_regex(sql: str) -> list[str]:
        param_names: list[str] = []
        param_names.extend(
            var_name
            for match in PARAM_REGEX.finditer(sql)
            if not (match.group("dquote") or match.group("squote") or match.group("comment"))
            and (var_name := match.group("var_name"))
        )
        return param_names

    @staticmethod
    def _extract_param_names_sqlglot(sql: str) -> list[str]:
        parsed_sql = sqlglot.parse_one(sql, read="mysql")
        param_names: list[str] = [node.name for node in parsed_sql.find_all(sqlglot_exp.Parameter) if node.name]
        if not param_names:
            msg = "Sqlglot found no :name parameters via parsing."
            raise SQLParsingError(msg)
        return param_names

    @staticmethod
    def _extract_duckdb_dollar_param_names(sql: str) -> list[str]:
        # DOLLAR_NAME_REGEX is specifically compiled to find $ followed by a letter
        # (e.g., (?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*)), so it won't match $1, $2 etc.
        param_names: list[str] = [
            var_name
            for match in DOLLAR_NAME_REGEX.finditer(sql)
            if not (match.group("dquote") or match.group("squote") or match.group("comment"))
            and (var_name := match.group("var_name"))
        ]
        return param_names

    @staticmethod
    def _extract_bigquery_at_param_names(sql: str) -> list[str]:
        param_names: list[str] = []
        try:
            parsed_sql = sqlglot.parse_one(sql, read="bigquery")
            # @foo is often Parameter in sqlglot for BQ
            param_names.extend(node.name for node in parsed_sql.find_all(sqlglot_exp.Parameter) if node.name)
        except Exception as e:  # noqa: BLE001
            msg = f"ADBC (bigquery): sqlglot failed to parse for @name params: {e}. Falling back to AT_NAME_REGEX. SQL: {sql}"
            logger.debug(msg)
            param_names.extend(
                var_name
                for match in AT_NAME_REGEX.finditer(sql)
                if not (match.group("dquote") or match.group("squote") or match.group("comment"))
                and (var_name := match.group("var_name"))
            )
        return param_names

    @staticmethod
    def _build_ordered_param_tuple(
        param_names: list[str],
        params_dict: dict[str, Any],
        dialect_for_error_msg: str,
        sql_for_error_msg: str,
        placeholder_prefix_for_error_msg: str,
    ) -> tuple[Any, ...]:
        # If dict is empty, return empty tuple even if param_names is not (implies SQL has placeholders but no params were actually needed/provided)
        if not params_dict:
            return ()

        # If dict is provided, but no placeholders were found in SQL to order them by
        if not param_names and params_dict:
            msg = (
                f"ADBC {dialect_for_error_msg}: Dictionary parameters provided ({list(params_dict.keys())}), but no recognizable "
                f"'{placeholder_prefix_for_error_msg}name' placeholders found in SQL to determine order. SQL: {sql_for_error_msg}"
            )
            raise SQLParsingError(msg)

        params_values_list: list[Any] = []
        missing_keys: list[str] = []
        for name in param_names:
            if name in params_dict:
                params_values_list.append(params_dict[name])
            else:
                missing_keys.append(name)

        if missing_keys:
            # Format missing keys string properly
            missing_keys_fmt = ", ".join([f"{placeholder_prefix_for_error_msg}{k}" for k in missing_keys])
            msg = (
                f"ADBC {dialect_for_error_msg}: Missing data for parameter(s) {missing_keys_fmt} "
                f"found in SQL but not provided in dictionary. SQL: {sql_for_error_msg}"
            )
            raise SQLParsingError(msg)

        # Check for unused keys provided in the dictionary
        unused_keys = set(params_dict.keys()) - set(param_names)
        if unused_keys:
            msg = f"ADBC {dialect_for_error_msg}: Parameters provided in dictionary but not found in SQL: {sorted(unused_keys)}. SQL: {sql_for_error_msg}"
            logger.warning(msg)
            raise SQLParsingError(msg)

        return tuple(params_values_list)

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[tuple[Any, ...]]]":  # Always returns tuple or None for params
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...]]] = None
        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for adbc driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters

        # --- Dictionary Parameters Handling ---
        if isinstance(merged_params, dict):
            # Handle empty dictionary case cleanly
            if not merged_params:
                # Check if SQL actually requires parameters
                if (
                    self._extract_colon_param_names(sql)
                    or self._extract_duckdb_dollar_param_names(sql)
                    or self._extract_bigquery_at_param_names(sql)
                ):
                    msg = f"ADBC: SQL contains named placeholders but parameter dictionary is empty. SQL: {sql}"
                    raise SQLParsingError(msg)
                return sql, ()  # Return SQL as is, with empty tuple for params

            actual_adbc_dialect = self._get_dialect(self.connection)
            sql_to_execute: str = sql
            ordered_param_names: list[str] = []
            placeholder_prefix = ":"  # Default prefix for error messages

            # Determine parameter order based on original SQL style (:name > $name > @name)
            ordered_param_names = self._extract_colon_param_names(sql)
            if not ordered_param_names:
                if actual_adbc_dialect == "duckdb":
                    ordered_param_names = self._extract_duckdb_dollar_param_names(sql)
                    placeholder_prefix = "$" if ordered_param_names else ":"
                elif actual_adbc_dialect == "bigquery":
                    ordered_param_names = self._extract_bigquery_at_param_names(sql)
                    placeholder_prefix = "@" if ordered_param_names else ":"
                # Add elif for other native styles here if necessary

            # Build the ordered tuple (raises error if inconsistent)
            params_tuple = self._build_ordered_param_tuple(
                ordered_param_names, merged_params, actual_adbc_dialect, sql, placeholder_prefix
            )

            # Transpile SQL syntax if necessary for target dialect/param style
            if actual_adbc_dialect in {"duckdb", "sqlite"}:
                if actual_adbc_dialect == "duckdb":
                    msg = (
                        f"ADBC (duckdb) PRE-TRANSPILE DictCase: SQL='{sql}', "
                        f"placeholder_prefix='{placeholder_prefix}', "
                        f"ordered_param_names='{ordered_param_names}', "
                        f"params_tuple='{params_tuple}'"
                    )
                    logger.debug(msg)
                try:
                    read_dialect_for_transpile = "duckdb" if placeholder_prefix == "$" else "mysql"
                    has_qmarks = any(
                        True
                        for m in QMARK_REGEX.finditer(sql)
                        if not (m.group("dquote") or m.group("squote") or m.group("comment")) and m.group("qmark")
                    )

                    if not has_qmarks and ordered_param_names:
                        target_write_dialect = actual_adbc_dialect
                        msg = f"ADBC ({target_write_dialect}) DEBUG: Transpiling Dict Params. Read='{read_dialect_for_transpile}', Write='{target_write_dialect}'. SQL='{sql}'"
                        logger.debug(msg)
                        sql_to_execute = sqlglot.transpile(
                            sql, read=read_dialect_for_transpile, write=target_write_dialect, pretty=False
                        )[0]
                        msg = f"ADBC ({target_write_dialect}) DEBUG: Transpile Dict Result='{sql_to_execute}'"
                        logger.debug(msg)
                    elif has_qmarks:
                        sql_to_execute = sql
                    else:  # No named params and no qmarks
                        sql_to_execute = sql
                except Exception as e:
                    msg = (
                        f"ADBC ({actual_adbc_dialect}): Failed to transpile SQL to 'qmark' style "
                        f"when using dictionary parameters. Error: {e}. Original SQL: {sql}"
                    )
                    logger.warning(msg)
                    raise SQLParsingError(msg) from e
            else:
                # Transpile to the native style (e.g., @name for BQ). Use `args` to allow sqlglot type handling.
                try:
                    sql_to_execute = sqlglot.transpile(sql, args=merged_params, write=actual_adbc_dialect)[0]
                except Exception as e:  # noqa: BLE001
                    msg = f"ADBC ({actual_adbc_dialect}): Failed to transpile SQL to native style: {e}. Using original SQL. SQL: {sql}"
                    logger.warning(msg)
                    sql_to_execute = sql

            return sql_to_execute, params_tuple

        # --- Sequence/Scalar Parameters Handling ---
        if isinstance(merged_params, (list, tuple)):
            actual_adbc_dialect = self._get_dialect(self.connection)
            colon_names = self._extract_colon_param_names(sql)
            # Other named param styles are not typically mixed with sequence params by users for SQLite.
            # We focus on :name for this special SQLite handling.

            if colon_names and actual_adbc_dialect == "sqlite":
                # SQLite: SQL has :name, params are a tuple. Map them.
                if len(colon_names) != len(merged_params):
                    msg = (
                        f"ADBC (sqlite): Tuple parameter count ({len(merged_params)}) does not match "
                        f"named placeholder count ({len(colon_names)}) in SQL: {sql}"
                    )
                    raise SQLParsingError(msg)
                # Parameters are already a tuple (merged_params) and in the correct order by convention.
                # We just need to transpile the SQL.
                try:
                    msg = f"ADBC (sqlite) DEBUG: Transpiling Tuple/Scalar. Read='mysql', Write='sqlite'. SQL='{sql}'"
                    logger.debug(msg)
                    sql_to_execute = sqlglot.transpile(sql, read="mysql", write="sqlite", pretty=False)[0]
                    msg = f"ADBC (sqlite) DEBUG: Transpile Tuple/Scalar Result='{sql_to_execute}'"
                    logger.debug(msg)

                    # Return the transpiled SQL with the original parameters (as tuple)
                    # If execution reached here, merged_params must have been a list/tuple based on the outer check
                    return sql_to_execute, tuple(merged_params)

                except Exception as e:
                    # Determine original param type for error message more reliably
                    param_type_str = "tuple/list" if isinstance(parameters, (list, tuple)) else "unknown"
                    msg = (
                        f"ADBC (sqlite): Failed to transpile SQL with :name to 'qmark' style "
                        f"when using {param_type_str} parameters. Error: {e}. Original SQL: {sql}"
                    )
                    logger.exception(msg)
                    raise SQLParsingError(msg) from e
            elif (
                colon_names
                or self._extract_duckdb_dollar_param_names(sql)
                or self._extract_bigquery_at_param_names(sql)
            ):
                # For other dialects, or if not SQLite with :name, this is a mismatch.
                msg = f"ADBC: Sequence/tuple parameters provided, but SQL contains named placeholders (:name, $name, @name). SQL: {sql}"
                raise ParameterStyleMismatchError(msg)

            # If no named placeholders were found, or if it's SQLite and we handled :name above,
            # we assume the SQL is qmark or some other positional style compatible with the tuple.
            return sql, tuple(merged_params)

        # --- Scalar Parameters Handling (Separate Block) ---
        if merged_params is not None and not isinstance(merged_params, (list, tuple, dict)):  # type: ignore[unreachable]
            actual_adbc_dialect = self._get_dialect(self.connection)  # type: ignore[unreachable]
            colon_names = self._extract_colon_param_names(sql)

            if colon_names and actual_adbc_dialect == "sqlite":
                # SQLite: SQL has :name, param is scalar.
                if len(colon_names) != 1:
                    msg = f"ADBC (sqlite): Scalar parameter provided, but SQL has {len(colon_names)} named placeholders (expected 1). SQL: {sql}"
                    raise SQLParsingError(msg)
                # Parameter is scalar (merged_params). Wrap in a tuple.
                # Transpile SQL.
                try:
                    msg = f"ADBC (sqlite) DEBUG: Transpiling Scalar. Read='mysql', Write='sqlite'. SQL='{sql}'"
                    logger.debug(msg)
                    sql_to_execute = sqlglot.transpile(sql, read="mysql", write="sqlite", pretty=False)[0]
                    msg = f"ADBC (sqlite) DEBUG: Transpile Scalar Result='{sql_to_execute}'"
                    logger.debug(msg)

                except Exception as e:
                    msg = (
                        f"ADBC (sqlite): Failed to transpile SQL with :name to 'qmark' style "
                        f"when using a scalar parameter. Error: {e}. Original SQL: {sql}"
                    )
                    logger.exception(msg)
                    raise SQLParsingError(msg) from e
                else:
                    return sql_to_execute, (merged_params,)  # Return scalar wrapped in tuple
            elif (
                colon_names
                or self._extract_duckdb_dollar_param_names(sql)
                or self._extract_bigquery_at_param_names(sql)
            ):
                # For other dialects, or if not SQLite with :name, this is a mismatch.
                msg = f"ADBC: Scalar parameter provided, but SQL contains named placeholders. SQL: {sql}"
                raise ParameterStyleMismatchError(msg)

            # If no named placeholders, or if it's SQLite and we handled :name,
            # check for qmark count for scalar.
            qmark_count = sum(
                1
                for m in QMARK_REGEX.finditer(sql)
                if not (m.group("dquote") or m.group("squote") or m.group("comment")) and m.group("qmark")
            )
            if qmark_count != 1:
                msg = f"ADBC: Scalar parameter provided, but SQL contains {qmark_count} qmark ('?') placeholders (expected 1). SQL: {sql}"
                raise SQLParsingError(msg)
            return sql, (merged_params,)

        # --- No Parameters Provided Validation ---
        if merged_params is None:
            has_placeholders = bool(
                self._extract_colon_param_names(sql)
                or self._extract_duckdb_dollar_param_names(sql)
                or self._extract_bigquery_at_param_names(sql)
                or any(
                    True
                    for m in QMARK_REGEX.finditer(sql)
                    if not (m.group("dquote") or m.group("squote") or m.group("comment")) and m.group("qmark")
                )
            )
            if has_placeholders:
                msg = f"ADBC: SQL statement appears to contain parameter placeholders, but no parameters were provided. SQL: {sql}"
                raise SQLParsingError(msg)

        return sql, None  # No parameters provided, and none found/needed

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AdbcConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore
            if not results:
                return []

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AdbcConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if not result:
                return None

            first_row = result[0]

            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            result_dict = dict(zip(column_names, first_row))

            if schema_type is None:
                return result_dict
            return cast("ModelDTOT", schema_type(**result_dict))

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            The results of the query as an Apache Arrow Table.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("ArrowTable", cursor.fetch_arrow_table())  # pyright: ignore[reportUnknownMemberType]
