import contextlib
import datetime
import logging
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

from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.exceptions import NotFound

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import NotFoundError, ParameterStyleMismatchError, SQLSpecError
from sqlspec.filters import StatementFilter
from sqlspec.mixins import (
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowBulkOperationsMixin,
    SyncParquetExportMixin,
)
from sqlspec.statement import SQLStatement
from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

if TYPE_CHECKING:
    from google.cloud.bigquery import SchemaField
    from google.cloud.bigquery.table import Row

__all__ = ("BigQueryConnection", "BigQueryDriver")

BigQueryConnection = Client

logger = logging.getLogger("sqlspec")


class BigQueryDriver(
    SyncDriverAdapterProtocol["BigQueryConnection"],
    SyncArrowBulkOperationsMixin["BigQueryConnection"],
    SyncParquetExportMixin["BigQueryConnection"],
    SQLTranslatorMixin["BigQueryConnection"],
    ResultConverter,
):
    """Synchronous BigQuery Driver Adapter."""

    dialect: str = "bigquery"
    connection: "BigQueryConnection"
    __supports_arrow__: ClassVar[bool] = True

    def __init__(self, connection: "BigQueryConnection", **kwargs: Any) -> None:
        super().__init__(connection=connection)
        self._default_query_job_config = kwargs.get("default_query_job_config") or getattr(
            connection, "default_query_job_config", None
        )

    @staticmethod
    def _get_bq_param_type(value: Any) -> "tuple[Optional[str], Optional[str]]":
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

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters using SQLStatement with dialect support.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to bind to the statement.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Raises:
            ParameterStyleMismatchError: If pre-formatted BigQuery parameters are mixed with keyword arguments.

        Returns:
            A tuple of (sql, parameters) ready for execution.
        """
        # Special case: check for pre-formatted BQ parameters
        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in parameters)
        ):
            if kwargs:
                msg = "Cannot mix pre-formatted BigQuery parameters with keyword arguments."
                raise ParameterStyleMismatchError(msg)
            return sql, parameters

        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        # Process the statement for execution
        processed_sql, processed_params, _ = statement.process()

        return processed_sql, processed_params

    def _run_query_job(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
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

        # Process SQL and parameters
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Handle pre-formatted parameters
        if (
            isinstance(processed_params, (list, tuple))
            and processed_params
            and all(
                isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in processed_params
            )
        ):
            final_job_config.query_parameters = list(processed_params)
        # Convert regular parameters to BigQuery parameters
        elif isinstance(processed_params, dict):
            # Convert dict params to BQ ScalarQueryParameter
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, self._get_bq_param_type(value)[0], value)
                for name, value in processed_params.items()
            ]
        elif isinstance(processed_params, (list, tuple)):
            # Convert list params to BQ ScalarQueryParameter
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(None, self._get_bq_param_type(value)[0], value)
                for value in processed_params
            ]

        # Determine which kwargs to pass to the actual query method
        # We only want to pass kwargs that were *not* treated as SQL parameters
        final_query_kwargs = {}
        if parameters is not None and kwargs:  # Params came via arg, kwargs are separate
            final_query_kwargs = kwargs
        # Else: If params came via kwargs, they are already handled, so don't pass them again

        # Execute query
        return conn.query(
            final_sql,
            job_config=final_job_config,
            **final_query_kwargs,
        )

    @overload
    def _rows_to_results(
        self,
        rows: "Iterator[Row]",
        schema: "Sequence[SchemaField]",
        schema_type: "type[ModelDTOT]",
    ) -> Sequence[ModelDTOT]: ...
    @overload
    def _rows_to_results(
        self,
        rows: "Iterator[Row]",
        schema: "Sequence[SchemaField]",
        schema_type: None = None,
    ) -> Sequence[dict[str, Any]]: ...
    def _rows_to_results(
        self,
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
            processed_results.append(row_dict)
        return self.to_schema(processed_results, schema_type=schema_type)

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            job_config: Optional job configuration.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
        )
        return self._rows_to_results(query_job.result(), query_job.result().schema, schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
        )
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
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
        )
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
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
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
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[T, Any]:
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
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
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        query_job = self._run_query_job(
            sql,
            parameters,
            *filters,
            connection=connection,
            job_config=job_config,
            **kwargs,
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
        *filters: StatementFilter,
        connection: Optional["BigQueryConnection"] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> int:
        """Executes INSERT, UPDATE, DELETE and returns affected row count.

        Returns:
            int: The number of rows affected by the DML statement.
        """
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
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
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
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
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> str:
        """Executes a BigQuery script and returns the job ID.

        Returns:
            str: The job ID of the executed script.
        """
        query_job = self._run_query_job(
            sql,
            parameters,
            connection=connection,
            job_config=job_config,
            is_script=True,
            **kwargs,
        )
        return str(query_job.job_id)

    # --- Mixin Implementations ---

    def select_arrow(  # pyright: ignore
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        conn = self._connection(connection)
        final_job_config = job_config or self._default_query_job_config or QueryJobConfig()

        # Process SQL and parameters using SQLStatement
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Convert parameters to BigQuery format
        if isinstance(processed_params, dict):
            query_parameters = []
            for key, value in processed_params.items():
                param_type, array_element_type = self._get_bq_param_type(value)

                if param_type == "ARRAY" and array_element_type:
                    query_parameters.append(bigquery.ArrayQueryParameter(key, array_element_type, value))
                elif param_type:
                    query_parameters.append(bigquery.ScalarQueryParameter(key, param_type, value))  # type: ignore[arg-type]
                else:
                    msg = f"Unsupported parameter type for BigQuery Arrow named parameter '{key}': {type(value)}"
                    raise SQLSpecError(msg)
            final_job_config.query_parameters = query_parameters
        elif isinstance(processed_params, (list, tuple)):
            # Convert sequence parameters
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(None, self._get_bq_param_type(value)[0], value)
                for value in processed_params
            ]

        # Execute the query and get Arrow table
        try:
            query_job = conn.query(processed_sql, job_config=final_job_config)
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
        *filters: StatementFilter,
        destination_uri: "Optional[str]" = None,
        connection: "Optional[BigQueryConnection]" = None,
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

    def _connection(self, connection: "Optional[BigQueryConnection]" = None) -> "BigQueryConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
