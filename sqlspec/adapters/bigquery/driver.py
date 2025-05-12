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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters using SQLStatement with dialect support.

        This method also handles the separation of StatementFilter instances that might be
        passed in the 'parameters' argument.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to bind to the statement. This can be a
                        Mapping (dict), Sequence (list/tuple), a single StatementFilter, or None.
            *filters: Additional statement filters to apply.
            **kwargs: Additional keyword arguments (treated as named parameters for the SQL statement).

        Raises:
            ParameterStyleMismatchError: If pre-formatted BigQuery parameters are mixed with keyword arguments.

        Returns:
            A tuple of (processed_sql, processed_parameters) ready for execution.
        """
        passed_parameters: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None
        combined_filters_list: list[StatementFilter] = list(filters)

        if parameters is not None:
            if isinstance(parameters, StatementFilter):
                combined_filters_list.insert(0, parameters)
            else:
                passed_parameters = parameters

        if (
            isinstance(passed_parameters, (list, tuple))
            and passed_parameters
            and all(
                isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in passed_parameters
            )
        ):
            if kwargs:
                msg = "Cannot mix pre-formatted BigQuery parameters with keyword arguments."
                raise ParameterStyleMismatchError(msg)
            return sql, passed_parameters

        statement = SQLStatement(sql, passed_parameters, kwargs=kwargs, dialect=self.dialect)

        for filter_obj in combined_filters_list:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, _ = statement.process()

        return processed_sql, processed_params

    def _run_query_job(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        is_script: bool = False,
        **kwargs: Any,
    ) -> "QueryJob":
        conn = self._connection(connection)

        if job_config:
            final_job_config = job_config
        elif self._default_query_job_config:
            final_job_config = QueryJobConfig.from_api_repr(self._default_query_job_config.to_api_repr())  # type: ignore[no-untyped-call]
        else:
            final_job_config = QueryJobConfig()

        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

        if (
            isinstance(processed_params, (list, tuple))
            and processed_params
            and all(
                isinstance(p, (bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter)) for p in processed_params
            )
        ):
            final_job_config.query_parameters = list(processed_params)
        elif isinstance(processed_params, dict):
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, self._get_bq_param_type(value)[0], value)
                for name, value in processed_params.items()
            ]
        elif isinstance(processed_params, (list, tuple)):
            final_job_config.query_parameters = [
                bigquery.ScalarQueryParameter(None, self._get_bq_param_type(value)[0], value)
                for value in processed_params
            ]

        final_query_kwargs = {}
        if parameters is not None and kwargs:
            final_query_kwargs = kwargs

        return conn.query(
            final_sql,
            job_config=final_job_config,  # pyright: ignore
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
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
        sql: str,
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
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
        sql: str,
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
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
        sql: str,
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
        sql: str,
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
        sql: str,
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

        try:
            source_table_ref = bigquery.TableReference.from_string(sql, default_project=conn.project)
        except ValueError as e:
            msg = (
                "select_to_parquet expects a fully qualified table ID (e.g., 'project.dataset.table') "
                f"as the `sql` argument. Parsing failed for input '{sql}': {e!s}"
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
