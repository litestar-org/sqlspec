import contextlib
import datetime
from collections.abc import Iterator, Sequence
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    Union,
    cast,
    overload,
)

import sqlglot
from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.exceptions import NotFound

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import NotFoundError, SQLSpecError
from sqlspec.mixins import (
    SQLTranslatorMixin,
    SyncArrowBulkOperationsMixin,
    SyncParquetExportMixin,
)
from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

if TYPE_CHECKING:
    from google.cloud.bigquery import Client, SchemaField
    from google.cloud.bigquery.table import Row

__all__ = ("BigQueryDriver", )


class BigQueryDriver(
    SyncDriverAdapterProtocol["Client"],
    SyncArrowBulkOperationsMixin["Client"],
    SyncParquetExportMixin["Client"],
    SQLTranslatorMixin["Client"],
):
    """Synchronous BigQuery Driver Adapter."""

    dialect: str = "bigquery"
    connection: "Client"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(self, connection: "Client", **kwargs: Any) -> None:
        super().__init__(connection=connection)
        self._default_query_job_config = kwargs.get("default_query_job_config") or getattr(
            connection, "default_query_job_config", None
        )

    @staticmethod
    def _get_bq_param_type(value: Any) -> "tuple[Optional[str], Optional[str]]":  # noqa: PLR0911, PLR0912
        if isinstance(value, bool):
            return "BOOL", None
        if isinstance(value, int):
            return "INT64", None
        if isinstance(value, float):
            return "FLOAT64", None
        if isinstance(value, Decimal):
            # Precision/scale might matter, but BQ client handles conversion.
            # Defaulting to BIGNUMERIC, NUMERIC might be desired in some cases though (User change)
            return "BIGNUMERIC", None
        if isinstance(value, str):
            return "STRING", None
        if isinstance(value, bytes):
            return "BYTES", None
        if isinstance(value, datetime.date):
            return "DATE", None
        # DATETIME is for timezone-naive values
        if isinstance(value, datetime.datetime) and value.tzinfo is None:
            return "DATETIME", None
        # TIMESTAMP is for timezone-aware values
        if isinstance(value, datetime.datetime) and value.tzinfo is not None:
            return "TIMESTAMP", None
        if isinstance(value, datetime.time):
            return "TIME", None

        # Handle Arrays - Determine element type
        if isinstance(value, (list, tuple)):
            if not value:
                # Cannot determine type of empty array, BQ requires type.
                # Raise or default? Defaulting is risky. Let's raise.
                msg = "Cannot determine BigQuery ARRAY type for empty sequence."
                raise SQLSpecError(msg)
            # Infer type from first element
            first_element = value[0]
            element_type, _ = BigQueryDriver._get_bq_param_type(first_element)
            if element_type is None:
                msg = f"Unsupported element type in ARRAY: {type(first_element)}"
                raise SQLSpecError(msg)
            return "ARRAY", element_type

        # Handle Structs (basic dict mapping) - Requires careful handling
        # if isinstance(value, dict):
        #    # This requires recursive type mapping for sub-fields.
        #    # For simplicity, users might need to construct StructQueryParameter manually.
        #    # return "STRUCT", None # Placeholder if implementing  # noqa: ERA001
        #    raise SQLSpecError("Automatic STRUCT mapping not implemented. Please use bigquery.StructQueryParameter.")  # noqa: ERA001

        return None, None  # Unsupported type

    def _run_query_job(  # noqa: C901, PLR0912, PLR0915 (User change)
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[Client]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        is_script: bool = False,
        **kwargs: Any,
    ) -> "QueryJob":
        conn = self._connection(connection)

        # Determine the final job config, creating a new one if necessary
        # to avoid modifying a shared default config.
        if job_config:
            final_job_config = job_config  # Use the provided config directly
        elif self._default_query_job_config:
            final_job_config = QueryJobConfig()
        else:
            final_job_config = QueryJobConfig()  # Create a fresh config

        # --- Parameter Handling Logic --- Start
        params: Union[dict[str, Any], list[Any], None] = None
        param_style: Optional[str] = None  # 'named' (@), 'qmark' (?)
        use_preformatted_params = False
        final_sql = sql  # Default to original SQL

        # Check for pre-formatted BQ parameters first
        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in parameters)
        ):
            if kwargs:
                msg = "Cannot mix pre-formatted BigQuery parameters with keyword arguments."
                raise SQLSpecError(msg)
            use_preformatted_params = True
            final_job_config.query_parameters = list(parameters)
            # Keep final_sql = sql, as it should match the pre-formatted named params

        # Determine parameter style and merge standard parameters ONLY if not preformatted
        if not use_preformatted_params:
            if isinstance(parameters, dict):
                params = {**parameters, **kwargs}
                param_style = "named"
            elif isinstance(parameters, (list, tuple)):
                if kwargs:
                    msg = "Cannot mix positional parameters with keyword arguments."
                    raise SQLSpecError(msg)
                # Check if it's primitives for qmark style
                if all(
                    not isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in parameters
                ):
                    params = list(parameters)
                    param_style = "qmark"
                else:
                    # Mixed list or non-BQ parameter objects
                    msg = "Invalid mix of parameter types in list. Use only primitive values or only BigQuery QueryParameter objects."
                    raise SQLSpecError(msg)

            elif kwargs:
                params = kwargs
                param_style = "named"
            elif parameters is not None and not isinstance(
                parameters, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)
            ):
                # Could be a single primitive value for positional
                params = [parameters]
                param_style = "qmark"
            elif parameters is not None:  # Single BQ parameter object
                msg = "Single BigQuery QueryParameter objects should be passed within a list."
                raise SQLSpecError(msg)

        # Use sqlglot to transpile ONLY if not a script and not preformatted
        if not is_script and not use_preformatted_params:
            try:
                # Transpile for syntax normalization/dialect conversion if needed
                # Use BigQuery dialect for both reading and writing
                final_sql = sqlglot.transpile(sql, read=self.dialect, write=self.dialect)[0]
            except Exception as e:
                # Catch potential sqlglot errors
                msg = f"SQL transpilation failed using sqlglot: {e!s}"  # Adjusted message
                raise SQLSpecError(msg) from e
            # else: If preformatted_params, final_sql remains the original sql

        # --- Parameter Handling Logic --- (Moved outside the transpilation try/except)
        # Prepare BQ parameters based on style, ONLY if not preformatted
        if not use_preformatted_params:
            if param_style == "named" and params:
                # Convert dict params to BQ ScalarQueryParameter
                if isinstance(params, dict):
                    final_job_config.query_parameters = [
                        bigquery.ScalarQueryParameter(name, self._get_bq_param_type(value)[0], value)
                        for name, value in params.items()
                    ]
                else:
                    # This path should ideally not be reached if param_style logic is correct
                    msg = f"Internal error: Parameter style is 'named' but parameters are not a dict: {type(params)}"
                    raise SQLSpecError(msg)
            elif param_style == "qmark" and params:
                # Convert list params to BQ ScalarQueryParameter
                final_job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(None, self._get_bq_param_type(value)[0], value) for value in params
                ]

        # --- Parameter Handling Logic --- End

        # Determine which kwargs to pass to the actual query method.
        # We only want to pass kwargs that were *not* treated as SQL parameters.
        final_query_kwargs = {}
        if parameters is not None and kwargs:  # Params came via arg, kwargs are separate
            final_query_kwargs = kwargs
        # Else: If params came via kwargs, they are already handled, so don't pass them again.

        # Execute query
        return conn.query(
            final_sql,
            job_config=final_job_config,
            **final_query_kwargs,  # Pass only relevant kwargs
        )

    @staticmethod
    def _rows_to_results(
        rows: "Iterator[Row]",
        schema: "Sequence[SchemaField]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> Sequence[Union[ModelDTOT, dict[str, Any]]]:
        processed_results = []
        # Create a quick lookup map for schema fields from the passed schema
        schema_map = {field.name: field for field in schema}

        for row in rows:
            # row here is now a Row object from the iterator
            row_dict = {}
            for key, value in row.items():  # Use row.items() on the Row object
                field = schema_map.get(key)
                # Workaround remains the same
                if field and field.field_type == "TIMESTAMP" and isinstance(value, str) and "." in value:
                    try:
                        parsed_value = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)
                        row_dict[key] = parsed_value
                    except ValueError:
                        row_dict[key] = value  # type: ignore[assignment]
                else:
                    row_dict[key] = value
            # Use the processed dictionary for the final result
            if schema_type:
                processed_results.append(schema_type(**row_dict))
            else:
                processed_results.append(row_dict)  # type: ignore[arg-type]
        if schema_type:
            return cast("Sequence[ModelDTOT]", processed_results)
        return cast("Sequence[dict[str, Any]]", processed_results)

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
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
        connection: "Optional[Client]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        query_job = self._run_query_job(sql, parameters, connection, job_config, **kwargs)
        return self._rows_to_results(query_job.result(), query_job.result().schema, schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
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
        connection: "Optional[Client]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        query_job = self._run_query_job(sql, parameters, connection, job_config, **kwargs)
        rows_iterator = query_job.result()
        try:
            # Pass the iterator containing only the first row to _rows_to_results
            # This ensures the timestamp workaround is applied consistently.
            # We need to pass the original iterator for schema access, but only consume one row.
            first_row = next(rows_iterator)
            # Create a simple iterator yielding only the first row for processing
            single_row_iter = iter([first_row])
            # We need RowIterator type for schema, create mock/proxy if needed, or pass schema
            # Let's try passing schema directly to _rows_to_results (requires modifying it)
            results = self._rows_to_results(single_row_iter, rows_iterator.schema, schema_type)
            return results[0]
        except StopIteration:
            msg = "No result found when one was expected"
            raise NotFoundError(msg) from None

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
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
        connection: "Optional[Client]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        query_job = self._run_query_job(sql, parameters, connection, job_config, **kwargs)
        rows_iterator = query_job.result()
        try:
            first_row = next(rows_iterator)
            # Create a simple iterator yielding only the first row for processing
            single_row_iter = iter([first_row])
            # Pass schema directly
            results = self._rows_to_results(single_row_iter, rows_iterator.schema, schema_type)
            return results[0]
        except StopIteration:
            return None

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[T, Any]: ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[T, Any]:
        query_job = self._run_query_job(
            sql=sql, parameters=parameters, connection=connection, job_config=job_config, **kwargs
        )
        rows = query_job.result()
        try:
            first_row = next(iter(rows))
            value = first_row[0]
            # Apply timestamp workaround if necessary
            field = rows.schema[0]  # Get schema for the first column
            if field and field.field_type == "TIMESTAMP" and isinstance(value, str) and "." in value:
                with contextlib.suppress(ValueError):
                    value = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)

            return cast("T", value) if schema_type else value
        except (StopIteration, IndexError):
            msg = "No value found when one was expected"
            raise NotFoundError(msg) from None

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
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
        connection: "Optional[Client]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        query_job = self._run_query_job(
            sql=sql, parameters=parameters, connection=connection, job_config=job_config, **kwargs
        )
        rows = query_job.result()
        try:
            first_row = next(iter(rows))
            value = first_row[0]
            # Apply timestamp workaround if necessary
            field = rows.schema[0]  # Get schema for the first column
            if field and field.field_type == "TIMESTAMP" and isinstance(value, str) and "." in value:
                with contextlib.suppress(ValueError):
                    value = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)

            return cast("T", value) if schema_type else value
        except (StopIteration, IndexError):
            return None

    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional["Client"] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> int:
        """Executes INSERT, UPDATE, DELETE and returns affected row count.

        Returns:
            int: The number of rows affected by the DML statement.
        """
        query_job = self._run_query_job(
            sql=sql, parameters=parameters, connection=connection, job_config=job_config, **kwargs
        )
        # DML statements might not return rows, check job properties
        # num_dml_affected_rows might be None initially, wait might be needed
        query_job.result()  # Ensure completion
        return query_job.num_dml_affected_rows or 0  # Return 0 if None

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
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
        connection: "Optional[Client]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[ModelDTOT, dict[str, Any]]:
        """BigQuery DML RETURNING equivalent is complex, often requires temp tables or scripting."""
        msg = "BigQuery does not support `RETURNING` clauses directly in the same way as some other SQL databases. Consider multi-statement queries or alternative approaches."
        raise NotImplementedError(msg)

    def execute_script(
        self,
        sql: str,  # Expecting a script here
        parameters: "Optional[StatementParameterType]" = None,  # Parameters might be complex in scripts
        /,
        *,
        connection: "Optional[Client]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> str:
        """Executes a BigQuery script and returns the job ID.

        Returns:
            str: The job ID of the executed script.
        """
        query_job = self._run_query_job(
            sql=sql,
            parameters=parameters,
            connection=connection,
            job_config=job_config,
            is_script=True,
            **kwargs,
        )
        return str(query_job.job_id)

    # --- Mixin Implementations ---

    def select_arrow(  # pyright: ignore  # noqa: PLR0912
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Client]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        conn = self._connection(connection)
        final_job_config = job_config or self._default_query_job_config or QueryJobConfig()

        # Determine parameter style and merge parameters (Similar to _run_query_job)
        params: Union[dict[str, Any], list[Any], None] = None
        param_style: Optional[str] = None  # 'named' (@), 'qmark' (?)

        if isinstance(parameters, dict):
            params = {**parameters, **kwargs}
            param_style = "named"
        elif isinstance(parameters, (list, tuple)):
            if kwargs:
                msg = "Cannot mix positional parameters with keyword arguments."
                raise SQLSpecError(msg)
            params = list(parameters)
            param_style = "qmark"
        elif kwargs:
            params = kwargs
            param_style = "named"
        elif parameters is not None:
            params = [parameters]
            param_style = "qmark"

        # Use sqlglot to transpile and bind parameters
        try:
            transpiled_sql = sqlglot.transpile(sql, args=params or {}, read=None, write=self.dialect)[0]
        except Exception as e:
            msg = f"SQL transpilation/binding failed using sqlglot: {e!s}"
            raise SQLSpecError(msg) from e

        # Prepare BigQuery specific parameters if named style was used
        if param_style == "named" and params:
            if not isinstance(params, dict):
                # This should be logically impossible due to how param_style is set
                msg = "Internal error: named parameter style detected but params is not a dict."
                raise SQLSpecError(msg)
            query_parameters = []
            for key, value in params.items():
                param_type, array_element_type = self._get_bq_param_type(value)

                if param_type == "ARRAY" and array_element_type:
                    query_parameters.append(bigquery.ArrayQueryParameter(key, array_element_type, value))
                elif param_type:
                    query_parameters.append(bigquery.ScalarQueryParameter(key, param_type, value))  # type: ignore[arg-type]
                else:
                    msg = f"Unsupported parameter type for BigQuery Arrow named parameter '{key}': {type(value)}"
                    raise SQLSpecError(msg)
            final_job_config.query_parameters = query_parameters
        elif param_style == "qmark" and params:
            # Positional params handled by client library
            pass

        # Execute the query and get Arrow table
        try:
            query_job = conn.query(transpiled_sql, job_config=final_job_config)
            arrow_table = query_job.to_arrow()  # Waits for job completion

        except Exception as e:
            msg = f"BigQuery Arrow query execution failed: {e!s}"
            raise SQLSpecError(msg) from e
        return arrow_table

    def select_to_parquet(
        self,
        sql: str,  # Expects table ID: project.dataset.table
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        destination_uri: "Optional[str]" = None,
        connection: "Optional[Client]" = None,
        job_config: "Optional[bigquery.ExtractJobConfig]" = None,
        **kwargs: Any,
    ) -> None:
        """Exports a BigQuery table to Parquet files in Google Cloud Storage.

        Raises:
            NotImplementedError: If the SQL is not a fully qualified table ID or if parameters are provided.
            NotFoundError: If the source table is not found.
            SQLSpecError: If the Parquet export fails.
        """
        if destination_uri is None:
            msg = "destination_uri is required"
            raise SQLSpecError(msg)
        conn = self._connection(connection)
        if "." not in sql or parameters is not None:
            msg = "select_to_parquet currently expects a fully qualified table ID (project.dataset.table) as the `sql` argument and no `parameters`."
            raise NotImplementedError(msg)

        source_table_ref = bigquery.TableReference.from_string(sql, default_project=conn.project)

        final_extract_config = job_config or bigquery.ExtractJobConfig()  # type: ignore[no-untyped-call]
        final_extract_config.destination_format = bigquery.DestinationFormat.PARQUET

        try:
            extract_job = conn.extract_table(
                source_table_ref,
                destination_uri,
                job_config=final_extract_config,
                # Location is correctly inferred by the client library
            )
            extract_job.result()  # Wait for completion

        except NotFound:
            msg = f"Source table not found for Parquet export: {source_table_ref}"
            raise NotFoundError(msg) from None
        except Exception as e:
            msg = f"BigQuery Parquet export failed: {e!s}"
            raise SQLSpecError(msg) from e
        if extract_job.errors:
            msg = f"BigQuery Parquet export failed: {extract_job.errors}"
            raise SQLSpecError(msg)
