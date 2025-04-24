import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
    cast,
    overload,
)

# Spanner imports
# Use specific imports for clarity and potential type stub resolution
from google.cloud.spanner_v1 import Transaction, exceptions, param_types  # pyright: ignore

# sqlspec imports
from sqlspec.base import (
    SyncDriverAdapterProtocol,
)
from sqlspec.exceptions import NotFoundError, SQLConversionError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import PARAM_REGEX, SQLStatement
from sqlspec.typing import ModelDTOT, StatementParameterType, T

if TYPE_CHECKING:
    from collections.abc import Sequence

    from google.cloud.spanner_v1.streamed import StreamedResultSet

    # Define Connection types matching base protocol
SpannerConnection = Transaction


logger = logging.getLogger("sqlspec")

__all__ = ("SpannerConnection", "SpannerDriver")


# --- Helper Functions ---\


def _spanner_row_to_dict(row: "Sequence[Any]", fields: "list[str]") -> "dict[str, Any]":
    """Converts a Spanner result row (sequence) to a dictionary."""
    return dict(zip(fields, row))


# --- Base Parameter Processing (Shared Logic) ---\


def _base_process_sql_params(
    sql: str, parameters: "Optional[StatementParameterType]", dialect: str, kwargs: "Optional[dict[str, Any]]"
) -> "tuple[str, Optional[dict[str, Any]]]":
    """Process SQL and parameters for Spanner, converting :param -> @param.

    Returns the processed SQL and the parameter dictionary.
    """
    stmt = SQLStatement(sql=sql, parameters=parameters, dialect=dialect, kwargs=kwargs or None)
    processed_sql, processed_params = stmt.process()

    param_dict: Optional[dict[str, Any]] = None

    if isinstance(processed_params, (list, tuple)):
        msg = "Spanner requires named parameters (dict), not positional parameters."
        raise SQLParsingError(msg)
    if isinstance(processed_params, dict):
        param_dict = processed_params
        # Convert :param style to @param style for Spanner
        processed_sql_parts: list[str] = []
        last_end = 0
        found_params_regex: list[str] = []

        # Use PARAM_REGEX from statement module
        for match in PARAM_REGEX.finditer(processed_sql):
            # Skip matches inside quotes or comments if PARAM_REGEX handles them
            # (Assuming PARAM_REGEX correctly ignores quoted/commented sections)
            # if match.group("dquote") or match.group("squote") or match.group("comment"):
            #     continue

            var_match = match.group("var_name_colon")
            perc_match = match.group("var_name_perc")  # Check for %(param)s style too

            if var_match:
                var_name = var_match
                start_char = ":"
                start_idx = match.start("var_name_colon") - 1  # Position of ':'
                end_idx = match.end("var_name_colon")
            elif perc_match:
                # Need to adjust indices for %(...)s structure
                var_name = perc_match
                start_char = "%("  # This won't be used directly below, just for error msg
                start_idx = match.start("var_name_perc") - 2  # Position of '%'
                end_idx = match.end("var_name_perc") + 3  # Position after ')s'
            else:
                continue  # Skip non-parameter matches

            found_params_regex.append(var_name)

            if var_name not in param_dict:
                msg = (
                    f"Named parameter '{start_char}{var_name}' found in SQL but missing from parameters. "
                    f"SQL: {processed_sql}, Params: {param_dict.keys()}"
                )
                raise SQLParsingError(msg)

            processed_sql_parts.extend((processed_sql[last_end:start_idx], f"@{var_name}"))
            last_end = end_idx

        processed_sql_parts.append(processed_sql[last_end:])
        final_sql = "".join(processed_sql_parts)

        # If no :param or %(param)s found, but we have a dict, assume user wrote @param directly
        if not found_params_regex and param_dict:
            logger.debug(
                "Dict params provided (%s), but no standard ':%s' or '%%(%s)s' placeholders found. "
                "Assuming SQL uses @param directly. SQL: %s",
                list(param_dict.keys()),
                "param",
                "param",
                processed_sql,
            )
            return processed_sql, param_dict  # Return original SQL

        return final_sql, param_dict

    # If parameters is None or not a dict/list/tuple after processing
    return processed_sql, None


def _get_spanner_param_types(params: "Optional[dict[str, Any]]") -> "dict[str, Any]":
    """Generate basic Spanner parameter types (defaults to STRING).

    Placeholder: A more robust implementation would inspect param values.
    """
    # TODO: Enhance with actual type inference or allow user override via `param_types`
    return dict.fromkeys(params, param_types.STRING) if params else {}


# --- Synchronous Driver ---\n


class SpannerDriver(
    SyncDriverAdapterProtocol["SpannerConnection"],
    SQLTranslatorMixin["SpannerConnection"],
):
    """Spanner Sync Driver Adapter.

    Operates within a specific Spanner Snapshot or Transaction context.
    """

    dialect: str = "spanner"

    def __init__(self, connection: "SpannerConnection", **kwargs: Any) -> None:
        """Initialize with a Spanner Snapshot or Transaction."""
        self.connection = connection
        # kwargs are ignored for now, consistent with protocol

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[dict[str, Any]]]":
        """Process SQL and parameters for Spanner sync driver."""
        return _base_process_sql_params(sql, parameters, self.dialect, kwargs)

    def _execute_sql(
        self, sql: str, params: "Optional[dict[str, Any]]", context: "SpannerConnection"
    ) -> "StreamedResultSet":
        """Executes SQL using the provided Snapshot or Transaction."""
        types = _get_spanner_param_types(params)
        try:
            return context.execute_sql(sql, params or {}, types)
        except exceptions.NotFound as e:
            # Intercept NotFound early if possible, though typically raised on iteration
            msg = f"Spanner query execution failed: {e}"
            raise NotFoundError(msg) from e
        except exceptions.InvalidArgument as e:
            msg = f"Invalid argument during Spanner query execution: {e}. SQL: {sql}, Params: {params}"
            raise SQLParsingError(
                msg
            ) from e
        except Exception as e:
            # Catch other potential Spanner or network errors
            msg = f"Spanner query execution error: {e}"
            raise SQLConversionError(msg) from e

    def _execute_update(self, sql: str, params: "Optional[dict[str, Any]]", transaction: "Transaction") -> int:
        """Executes DML using the provided Transaction.

        Returns:
            -1 as Spanner's execute_update doesn't directly return row count easily.
        """
        types = _get_spanner_param_types(params)
        try:
            # execute_update returns the commit timestamp on success, not row count.
            _ = transaction.execute_update(sql, params or {}, types)
            # We return -1 as a placeholder, indicating success without a specific count.
            return -1
        except exceptions.NotFound as e:
            msg = f"Spanner update execution failed: {e}"
            raise NotFoundError(msg) from e
        except exceptions.InvalidArgument as e:
            msg = f"Invalid argument during Spanner update execution: {e}. SQL: {sql}, Params: {params}"
            raise SQLParsingError(
                msg
            ) from e
        except Exception as e:
            msg = f"Spanner update execution error: {e}"
            raise SQLConversionError(msg) from e

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
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
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...

    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Execute a SELECT query and return all results.

        Args:
            sql: The SQL query to execute.
            parameters: Optional parameters for the query.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert results to.
            **kwargs: Additional keyword arguments.

        Returns:
            A sequence of results, either as dictionaries or instances of schema_type.
        """
        context = connection or self.connection
        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)
        result_set = self._execute_sql(processed_sql, params, context)

        # Convert rows to dictionaries
        results = [
            _spanner_row_to_dict(row, [field.name for field in result_set.metadata.row_type.fields])  # pyright: ignore
            for row in result_set
        ]

        # Convert to schema type if specified
        if schema_type:
            return [schema_type(**result) for result in results]

        return results

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
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
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Execute a SELECT query and return the first result.

        Args:
            sql: The SQL query to execute.
            parameters: Optional parameters for the query.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert results to.
            **kwargs: Additional keyword arguments.

        Returns:
            The first result, either as a dictionary or an instance of schema_type.

        Raises:
            NotFoundError: If no results are found.
        """
        context = connection or self.connection
        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)
        result_set = self._execute_sql(processed_sql, params, context)

        try:
            # Get first row
            row = next(result_set)  # pyright: ignore
        except StopIteration:
            msg = "No results found for query"
            raise NotFoundError(msg)

        # Convert row to dictionary
        result = _spanner_row_to_dict(row, [field.name for field in result_set.metadata.row_type.fields])  # pyright: ignore

        # Convert to schema type if specified
        if schema_type:
            return schema_type(**result)

        return result

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
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
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Execute a SELECT query and return the first result or None.

        Args:
            sql: The SQL query to execute.
            parameters: Optional parameters for the query.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert results to.
            **kwargs: Additional keyword arguments.

        Returns:
            The first result, either as a dictionary or an instance of schema_type,
            or None if no results are found.
        """
        context = connection or self.connection
        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)
        result_set = self._execute_sql(processed_sql, params, context)

        try:
            # Get first row
            row = next(result_set)  # pyright: ignore
        except StopIteration:
            return None

        # Convert row to dictionary
        result = _spanner_row_to_dict(row, [field.name for field in result_set.metadata.row_type.fields])  # pyright: ignore

        # Convert to schema type if specified
        if schema_type:
            return schema_type(**result)

        return result

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> Any: ...

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> T: ...

    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Execute a SELECT query and return the first value of the first result.

        Args:
            sql: The SQL query to execute.
            parameters: Optional parameters for the query.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert the value to.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value of the first result, optionally converted to schema_type.

        Raises:
            NotFoundError: If no results are found.
        """
        context = connection or self.connection
        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)

        try:
            # Get first row
            row = next(self._execute_sql(processed_sql, params, context))  # pyright: ignore
        except StopIteration:
            msg = "No results found for query"
            raise NotFoundError(msg)

        if schema_type:
            return cast("T", schema_type(row[0]))  # pyright: ignore

        return row[0]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
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
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...

    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Execute a SELECT query and return the first value of the first result or None.

        Args:
            sql: The SQL query to execute.
            parameters: Optional parameters for the query.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert the value to.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value of the first result, optionally converted to schema_type,
            or None if no results are found.
        """
        context = connection or self.connection
        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)

        try:
            # Get first row
            row = next(self._execute_sql(processed_sql, params, context))  # pyright: ignore
        except StopIteration:
            return None

        # Convert to schema type if specified
        if schema_type:
            return cast("T", schema_type(row[0]))  # pyright: ignore

        return row[0]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Execute an INSERT, UPDATE, or DELETE statement.

        Args:
            sql: The SQL statement to execute.
            parameters: Optional parameters for the statement.
            connection: Optional connection to use instead of the default.
            **kwargs: Additional keyword arguments.

        Returns:
            The number of rows affected, or -1 if the count is not available.

        Raises:
            SQLConversionError: If the statement execution fails.
        """
        context = connection or self.connection
        if not isinstance(context, Transaction):  # pyright: ignore
            msg = "INSERT/UPDATE/DELETE operations require a Transaction"
            raise SQLConversionError(msg)

        processed_sql, params = self._process_sql_params(sql, parameters, **kwargs)
        return self._execute_update(processed_sql, params, context)

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
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
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Execute an INSERT, UPDATE, or DELETE statement with RETURNING clause.

        Note: Spanner doesn't support general RETURNING DML, so this method raises an error.

        Args:
            sql: The SQL statement to execute.
            parameters: Optional parameters for the statement.
            connection: Optional connection to use instead of the default.
            schema_type: Optional schema type to convert results to.
            **kwargs: Additional keyword arguments.

        Raises:
            SQLConversionError: Always raised as Spanner doesn't support RETURNING DML.
        """
        msg = "Spanner doesn't support RETURNING DML"
        raise SQLConversionError(msg)

    def execute_script(
        self,
        sql: str,  # Should contain multiple statements typically
        parameters: "Optional[StatementParameterType]" = None,  # Params might not be applicable to scripts
        /,
        *,
        connection: "Optional[SpannerConnection]" = None,
        **kwargs: Any,
    ) -> str:  # Protocol expects string status
        """Execute a SQL script containing multiple statements.

        Args:
            sql: The SQL script to execute.
            parameters: Optional parameters for the script.
            connection: Optional connection to use instead of the default.
            **kwargs: Additional keyword arguments.

        Returns:
            A status string indicating success.

        Raises:
            SQLConversionError: If the script execution fails.
        """
        context = connection or self.connection
        if not isinstance(context, Transaction):  # pyright: ignore
            msg = "Script execution requires a Transaction"
            raise SQLConversionError(msg)

        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        try:
            # Execute each statement in the script
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    if statement.upper().startswith(("SELECT", "WITH")):
                        self._execute_sql(statement, parameters, context)
                    else:
                        self._execute_update(statement, parameters, context)
            return "Script executed successfully"
        except Exception as e:
            msg = f"Script execution failed: {e}"
            raise SQLConversionError(msg) from e
