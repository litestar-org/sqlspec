import contextlib
import datetime
import logging
from collections.abc import Iterator, Mapping, Sequence
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
from google.cloud.bigquery import Client
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.exceptions import NotFound
from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import NotFoundError, ParameterStyleMismatchError, SQLSpecError
from sqlspec.sql.filters import StatementFilter
from sqlspec.sql.mixins import (
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowBulkOperationsMixin,
    SyncParquetExportMixin,
)
from sqlspec.sql.statement import SQLStatement, Statement
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
            return "BIGNUMERIC", None
        if isinstance(value, str):
            return "STRING", None
        if isinstance(value, bytes):
            return "BYTES", None
        if isinstance(value, datetime.date):
            return "DATE", None
        if isinstance(value, datetime.datetime) and value.tzinfo is None:
            return "DATETIME", None
        if isinstance(value, datetime.datetime) and value.tzinfo is not None:
            return "TIMESTAMP", None
        if isinstance(value, datetime.time):
            return "TIME", None

        if isinstance(value, (list, tuple)):
            if not value:
                msg = "Cannot determine BigQuery ARRAY type for empty sequence."
                raise SQLSpecError(msg)
            first_element = value[0]
            element_type, _ = BigQueryDriver._get_bq_param_type(first_element)
            if element_type is None:
                msg = f"Unsupported element type in ARRAY: {type(first_element)}"
                raise SQLSpecError(msg)
            return "ARRAY", element_type

        return None, None

    def _process_sql_params(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: "Any",
    ) -> "tuple[str, Optional[list[Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]]]]":
        """Process SQL and parameters for BigQuery.

        Leverages SQLStatement to parse SQL, validate parameters, and obtain a
        sqlglot AST. This method then transforms the AST to use BigQuery-style
        @named placeholders and prepares a list of bigquery.QueryParameter objects.

        Args:
            sql: The SQL statement to process (string or sqlglot Expression).
            parameters: The parameters to bind. Can be data or a StatementFilter.
            *filters: Additional statement filters to apply.
            **kwargs: Additional keyword arguments (treated as named parameters for the SQL statement).

        Raises:
            ParameterStyleMismatchError: If pre-formatted BigQuery parameters are mixed with keyword arguments.
            SQLSpecError: For unsupported parameter types or empty arrays.

        Returns:
            A tuple of (processed_sql_string, list_of_bigquery_query_parameters).
        """
        passed_parameters_for_statement: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None
        combined_filters_list: list[StatementFilter] = list(filters)

        if parameters is not None:
            if isinstance(parameters, StatementFilter):
                combined_filters_list.insert(0, parameters)
            # Check for pre-formatted BigQuery parameters first
            elif (
                isinstance(parameters, (list, tuple))
                and parameters
                and all(
                    isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in parameters
                )
            ):
                if kwargs:
                    msg = "Cannot mix pre-formatted BigQuery parameters with keyword arguments."
                    raise ParameterStyleMismatchError(msg)
                # If pre-formatted, ensure SQL is a string and return directly
                final_sql_str = str(sql)
                return final_sql_str, list(parameters)
            else:
                passed_parameters_for_statement = parameters

        # If not pre-formatted, proceed with SQLStatement processing
        statement = SQLStatement(sql, passed_parameters_for_statement, kwargs=kwargs, dialect=self.dialect)
        for filter_obj in combined_filters_list:
            statement = statement.apply_filter(filter_obj)

        parsed_expr, final_ordered_params, placeholder_nodes_in_order = statement.process()

        if not placeholder_nodes_in_order:
            return parsed_expr.sql(dialect=self.dialect), None

        # BigQuery uses @named parameters. Transform all placeholders to @p0, @p1, etc.
        # or use existing names if they were named (e.g. :foo -> @foo)
        bq_params: list[Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]] = []
        param_names_in_sql: list[str] = []

        # Create a mapping from original placeholder ID to a new named placeholder for BigQuery (e.g., @p0)
        # sqlglot for BigQuery dialect typically converts :name to @name and ? to @var_N
        # We will rely on sqlglot to generate the appropriate @named placeholders in the SQL string.
        # Then we extract these names to build the ScalarQueryParameter list in the correct order.

        current_ast = parsed_expr
        # If the original parameters were a dict, sqlglot should have used these names (prefixed with @)
        # If they were positional, sqlglot would generate names like @var_1, @var_2

        # Generate SQL first, as sqlglot might rename placeholders (e.g. ? to @var_1)
        final_sql_str = current_ast.sql(dialect=self.dialect)

        # Extract parameter names as they appear in the generated SQL for BigQuery
        # This relies on sqlglot producing @name style for parameters for the bigquery dialect
        # Placeholder nodes from statement.process() reflect the *original* structure.
        # We need the names as they will appear in the *final* SQL for BigQuery.

        # Re-parse the generated SQL to safely find new parameter names in order
        # This is safer if sqlglot transforms ? or other styles to @named style for BQ.
        final_expr_for_param_extraction = sqlglot.parse_one(final_sql_str, dialect=self.dialect)
        if not final_expr_for_param_extraction:
            # This should not happen if parsed_expr.sql() was successful
            return final_sql_str, None

        # Get the actual placeholder names sqlglot used in the final SQL for BigQuery
        # final_placeholder_names: "list[str]" = [] # Removed unused variable
        # Iterate through the placeholder_nodes_in_order which are from the *original* parsing,
        # but their transformation by sqlglot for the 'bigquery' dialect will give us the names.
        # The key is that `final_ordered_params` is already in the correct order corresponding to these original nodes.

        # idx_counter = 0 # Removed unused variable
        processed_param_names = set()

        for node in final_expr_for_param_extraction.find_all(exp.Parameter):
            # exp.Parameter in sqlglot for BQ dialect has `this` as the name (e.g. "var_1" for @var_1)
            param_name = node.name  # `name` attribute should give the identifier string e.g. "foo" for @foo
            if param_name and param_name not in processed_param_names:
                param_names_in_sql.append(param_name)
                processed_param_names.add(param_name)

        if len(param_names_in_sql) != len(final_ordered_params or []):
            # Fallback or error if names don't match params length
            # This might happen if sqlglot's naming strategy isn't perfectly predictable
            # or if there are complex nested structures not easily mapped.
            # For now, we will generate generic @p0, @p1 names and map them if this mismatch occurs.
            # This part needs careful handling.
            # A simpler approach for positional is to rely on sqlglot to produce `?` if that's what BQ client expects
            # for unnamed params. However, BQ client lib prefers named params for QueryJobConfig.

            # Let's regenerate SQL with explicit @p0, @p1 style if we couldn't map extracted names.
            # This ensures names match the order of final_ordered_params.
            param_names_in_sql = [f"p{i}" for i in range(len(final_ordered_params or []))]
            placeholder_map_for_explicit_naming: dict[int, exp.Expression] = {}
            for i, p_node in enumerate(placeholder_nodes_in_order):
                placeholder_map_for_explicit_naming[id(p_node)] = exp.Parameter(
                    this=exp.Identifier(this=param_names_in_sql[i])
                )

            def replace_with_explicit_bq_names(node: "exp.Expression") -> "exp.Expression":
                return placeholder_map_for_explicit_naming.get(id(node), node)

            transformed_expr_for_explicit_naming = parsed_expr.transform(replace_with_explicit_bq_names, copy=True)
            final_sql_str = transformed_expr_for_explicit_naming.sql(dialect=self.dialect)

        # Now, build the bq_params list using param_names_in_sql and final_ordered_params
        if final_ordered_params:
            if len(param_names_in_sql) == len(final_ordered_params):
                for i, value in enumerate(final_ordered_params):
                    param_name = param_names_in_sql[i]
                    param_type, array_element_type = self._get_bq_param_type(value)
                    if param_type == "ARRAY":
                        bq_params.append(bigquery.ArrayQueryParameter(param_name, array_element_type, value))
                    elif param_type:
                        bq_params.append(bigquery.ScalarQueryParameter(param_name, param_type, value))
                    else:
                        msg = f"Unsupported BigQuery parameter type for value: {type(value)}"
                        raise SQLSpecError(msg)
            else:
                # This case should ideally be handled by the explicit naming fallback or raise an error
                logger.warning(
                    "BigQuery: Mismatch between extracted/generated param names and number of ordered params. Parameter passing might be incorrect."
                )
                # As a last resort, if names couldn't be matched, pass None for params to avoid crash, but log severely.
                return final_sql_str, None

        return final_sql_str, bq_params or None

    def _run_query_job(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        is_script: bool = False,
        **kwargs: "Any",
    ) -> "QueryJob":
        conn = self._connection(connection)

        final_job_config = job_config or QueryJobConfig()
        # Apply default job config if one exists and wasn't overridden by specific job_config
        if not job_config and self._default_query_job_config:
            # Create a new QueryJobConfig from the default one to avoid modifying the original
            final_job_config = QueryJobConfig.from_api_repr(self._default_query_job_config.to_api_repr())  # type: ignore

        # The _process_sql_params now returns BigQuery QueryParameter objects directly
        # or pre-formatted list if that was passed in.
        final_sql_str, bq_query_parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        final_job_config.query_parameters = bq_query_parameters or []

        # kwargs passed to _process_sql_params are for SQLStatement parameter merging.
        # kwargs for conn.query should be separate if any are needed beyond job_config.
        # For now, assuming all relevant config is in job_config.

        return conn.query(
            final_sql_str,
            job_config=final_job_config,  # pyright: ignore
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
        schema_map = {field.name: field for field in schema}

        for row in rows:
            row_dict = {}
            for key, value in row.items():
                field = schema_map.get(key)
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
        )
        return self._rows_to_results(query_job.result(), query_job.result().schema, schema_type)

    @overload
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
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
            first_row = next(rows_iterator)
            single_row_iter = iter([first_row])
            results = self._rows_to_results(single_row_iter, rows_iterator.schema, schema_type)
            return results[0]
        except StopIteration:
            msg = "No result found when one was expected"
            raise NotFoundError(msg) from None

    @overload
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
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
            single_row_iter = iter([first_row])
            results = self._rows_to_results(single_row_iter, rows_iterator.schema, schema_type)
            return results[0]
        except StopIteration:
            return None

    @overload
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[T, Any]: ...
    @overload
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
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
            field = rows.schema[0]
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
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
            field = rows.schema[0]
            if field and field.field_type == "TIMESTAMP" and isinstance(value, str) and "." in value:
                with contextlib.suppress(ValueError):
                    value = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)

            return cast("T", value) if schema_type else value
        except (StopIteration, IndexError):
            return None

    def insert_update_delete(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional["BigQueryConnection"] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> int:
        query_job = self._run_query_job(
            sql, parameters, *filters, connection=connection, job_config=job_config, **kwargs
        )
        query_job.result()
        return query_job.num_dml_affected_rows or 0

    @overload
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> Union[ModelDTOT, dict[str, Any]]:
        msg = "BigQuery does not support `RETURNING` clauses directly in the same way as some other SQL databases. Consider multi-statement queries or alternative approaches."
        raise NotImplementedError(msg)

    def execute_script(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> str:
        query_job = self._run_query_job(
            sql,
            parameters,
            connection=connection,
            job_config=job_config,
            is_script=True,
            **kwargs,
        )
        return str(query_job.job_id)

    def select_arrow(  # pyright: ignore
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        conn = self._connection(connection)
        final_job_config = job_config or self._default_query_job_config or QueryJobConfig()

        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(None, self._get_bq_param_type(value)[0], value)
                for value in processed_params
            ]

        try:
            query_job = conn.query(processed_sql, job_config=final_job_config)
            arrow_table = query_job.to_arrow()
        except Exception as e:
            msg = f"BigQuery Arrow query execution failed: {e!s}"
            raise SQLSpecError(msg) from e
        return arrow_table

    def select_to_parquet(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        destination_uri: "Optional[str]" = None,
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[bigquery.ExtractJobConfig]" = None,
        **kwargs: Any,
    ) -> None:
        if destination_uri is None:
            msg = "destination_uri is required"
            raise SQLSpecError(msg)
        conn = self._connection(connection)

        if parameters is not None:
            msg = (
                "select_to_parquet expects a fully qualified table ID (e.g., 'project.dataset.table') "
                "as the `sql` argument and does not support `parameters`."
            )
            raise NotImplementedError(msg)
        processed_sql, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)

        try:
            source_table_ref = bigquery.TableReference.from_string(processed_sql, default_project=conn.project)
        except ValueError as e:
            msg = (
                "select_to_parquet expects a fully qualified table ID (e.g., 'project.dataset.table') "
                f"as the `sql` argument. Parsing failed for input '{processed_sql}': {e!s}"
            )
            raise NotImplementedError(msg) from e

        final_extract_config = job_config or bigquery.ExtractJobConfig()  # type: ignore[no-untyped-call]
        final_extract_config.destination_format = bigquery.DestinationFormat.PARQUET

        try:
            extract_job = conn.extract_table(
                source_table_ref,
                destination_uri,
                job_config=final_extract_config,
            )
            extract_job.result()

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
        return connection or self.connection
