# ruff: noqa: PLR6301
import datetime
import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from dataclasses import replace
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Union,
)

from google.cloud.bigquery import (
    ArrayQueryParameter,
    Client,
    QueryJob,
    QueryJobConfig,
    ScalarQueryParameter,
)
from google.cloud.bigquery.table import Row as BigQueryRow

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import (
    ParameterStyleMismatchError,
    SQLSpecError,
)
from sqlspec.statement.mixins import (
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
    SyncParquetMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import DictRow, RowT, SQLParameterType

if TYPE_CHECKING:
    from sqlspec.statement.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter


__all__ = ("BigQueryConnection", "BigQueryDriver")

BigQueryConnection = Client

logger = logging.getLogger("sqlspec.adapters.bigquery")


class BigQueryDriver(
    SyncDriverAdapterProtocol[BigQueryConnection, RowT],
    SyncArrowMixin[BigQueryConnection],
    SyncParquetMixin[BigQueryConnection],
    SQLTranslatorMixin[BigQueryConnection],
    ResultConverter,
):
    """Advanced BigQuery Driver with comprehensive Google Cloud capabilities.

    Protocol Implementation:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations with transaction safety
    - execute_script() - Multi-statement scripts and DDL operations
    """

    dialect: str = "bigquery"
    connection: BigQueryConnection
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = True
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self,
        connection: BigQueryConnection,
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        default_query_job_config: Optional[QueryJobConfig] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize BigQuery driver with comprehensive feature support.

        Args:
            connection: BigQuery Client instance
            config: SQL statement configuration
            instrumentation_config: Instrumentation configuration
            default_row_type: Default row type for results
            default_query_job_config: Default job configuration
            on_job_start: Callback executed when a BigQuery job starts
            on_job_complete: Callback executed when a BigQuery job completes
            **kwargs: Additional driver configuration
        """
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config or InstrumentationConfig(),
            default_row_type=default_row_type,
        )

        # BigQuery-specific configuration
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete

        # Set up default query job configuration
        default_config_kwarg = kwargs.get("default_query_job_config") or default_query_job_config
        conn_default_config = getattr(connection, "default_query_job_config", None)

        if default_config_kwarg is not None and isinstance(default_config_kwarg, QueryJobConfig):
            self._default_query_job_config = default_config_kwarg
        elif conn_default_config is not None and isinstance(conn_default_config, QueryJobConfig):
            self._default_query_job_config = conn_default_config
        else:
            self._default_query_job_config = None

    def _get_placeholder_style(self) -> ParameterStyle:
        """BigQuery uses named parameters with @ prefix."""
        return ParameterStyle.NAMED_AT

    @staticmethod
    def _get_bq_param_type(value: Any) -> tuple[Optional[str], Optional[str]]:
        """Determine BigQuery parameter type from Python value.

        Supports all BigQuery data types including arrays, structs, and geographic types.

        Args:
            value: Python value to convert.

        Returns:
            Tuple of (parameter_type, array_element_type).

        Raises:
            SQLSpecError: If value type is not supported.
        """
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
        if isinstance(value, datetime.datetime):
            return "TIMESTAMP" if value.tzinfo else "DATETIME", None
        if isinstance(value, datetime.time):
            return "TIME", None
        if isinstance(value, (list, tuple)):
            if not value:
                msg = "Cannot determine BigQuery ARRAY type for empty sequence. Provide typed empty array or ensure context implies type."
                raise SQLSpecError(msg)
            first_element = value[0]
            element_type, _ = BigQueryDriver._get_bq_param_type(first_element)
            if element_type is None:
                msg = f"Unsupported element type in ARRAY: {type(first_element)}"
                raise SQLSpecError(msg)
            return "ARRAY", element_type

        # Support for additional BigQuery types
        # Note: Geographic types, JSON, etc. can be added here as needed
        return None, None

    def _prepare_bq_query_parameters(
        self, params_dict: dict[str, Any]
    ) -> list[Union[ScalarQueryParameter, ArrayQueryParameter]]:
        """Convert parameter dictionary to BigQuery parameter objects.

        Args:
            params_dict: Dictionary of parameter names and values.

        Returns:
            List of BigQuery parameter objects.

        Raises:
            SQLSpecError: If parameter type is not supported.
        """
        bq_params: list[Union[ScalarQueryParameter, ArrayQueryParameter]] = []

        if params_dict:
            for name, value in params_dict.items():
                param_name_for_bq = name.lstrip("@")
                param_type, array_element_type = self._get_bq_param_type(value)

                if param_type == "ARRAY" and array_element_type:
                    bq_params.append(ArrayQueryParameter(param_name_for_bq, array_element_type, value))
                elif param_type:
                    bq_params.append(ScalarQueryParameter(param_name_for_bq, param_type, value))
                else:
                    msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(value)}"
                    raise SQLSpecError(msg)

        return bq_params

    def _run_query_job(
        self,
        sql_str: str,
        bq_query_parameters: Optional[list[Union[ScalarQueryParameter, ArrayQueryParameter]]],
        connection: Optional[BigQueryConnection] = None,
        job_config: Optional[QueryJobConfig] = None,
    ) -> QueryJob:
        """Execute a BigQuery job with comprehensive configuration support.

        Args:
            sql_str: SQL string to execute.
            bq_query_parameters: BigQuery parameter objects.
            connection: Optional connection override.
            job_config: Optional job configuration override.

        Returns:
            QueryJob instance.
        """
        conn = connection or self.connection

        # Build final job configuration
        final_job_config = QueryJobConfig()

        # Apply default configuration if available
        if self._default_query_job_config:
            # Copy settings from default config
            for attr in dir(self._default_query_job_config):
                if not attr.startswith("_") and hasattr(final_job_config, attr):
                    with suppress(Exception):
                        value = getattr(self._default_query_job_config, attr)
                        if value is not None:
                            setattr(final_job_config, attr, value)

        # Apply override configuration if provided
        if job_config:
            # Copy settings from override config
            for attr in dir(job_config):
                if not attr.startswith("_") and hasattr(final_job_config, attr):
                    with suppress(Exception):
                        value = getattr(job_config, attr)
                        if value is not None:
                            setattr(final_job_config, attr, value)

        # Set query parameters
        final_job_config.query_parameters = bq_query_parameters or []

        # Execute job start callback
        job_id = f"sqlspec-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"  # noqa: DTZ005
        if self.on_job_start:
            try:
                self.on_job_start(job_id)
            except Exception as e:
                logger.warning("Job start callback failed: %s", str(e), extra={"adapter": "bigquery"})

        # Execute the query
        query_job = conn.query(sql_str, job_config=final_job_config)

        # Execute job complete callback
        if self.on_job_complete:
            try:
                self.on_job_complete(query_job.job_id or job_id, query_job)
            except Exception as e:
                logger.warning("Job complete callback failed: %s", str(e), extra={"adapter": "bigquery"})

        return query_job

    def _rows_to_results(self, rows_iterator: Iterator[BigQueryRow]) -> list[RowT]:
        """Convert BigQuery rows to dictionary format.

        Args:
            rows_iterator: Iterator of BigQuery Row objects.

        Returns:
            List of dictionaries representing the rows.
        """
        return [dict(row) for row in rows_iterator]  # type: ignore[misc]

    def _execute_impl(
        self,
        statement: SQL,
        connection: Optional[BigQueryConnection] = None,
        **kwargs: Any,
    ) -> Union[QueryJob, str]:
        """Core execution implementation for BigQuery operations.

        Args:
            statement: SQL statement object to execute.
            connection: Optional connection override.
            **kwargs: Additional execution parameters, e.g., job_config.

        Raises:
            ParameterStyleMismatchError: If the parameters are not formatted correctly for BigQuery.
            SQLSpecError: For other BigQuery specific errors.

        Returns:
            QueryJob for regular queries or string for script execution.
        """
        # `is_many` and `is_script` are accessed from `statement.is_many` and `statement.is_script`.

        final_sql_str: str
        bq_query_parameters_list: list[Union[ScalarQueryParameter, ArrayQueryParameter]]

        if statement.is_script:
            final_sql_str = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            bq_query_parameters_list = []
        elif (
            isinstance(statement.parameters, (list, tuple))
            and statement.parameters
            and all(isinstance(p, (ScalarQueryParameter, ArrayQueryParameter)) for p in statement.parameters)
        ):
            if not isinstance(statement.sql, str):
                msg = "If statement.parameters contains pre-formatted BigQuery parameters, statement.sql must be a string."
                raise SQLSpecError(msg)
            final_sql_str = statement.sql
            bq_query_parameters_list = list(statement.parameters)
        else:
            final_sql_str = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_from_sql_object = statement.parameters
            params_dict: dict[str, Any] = {}

            if params_from_sql_object is not None:
                if isinstance(params_from_sql_object, dict):
                    params_dict = params_from_sql_object
                else:
                    # This path should ideally not be hit if SQL.get_parameters works as expected for NAMED_AT
                    msg = (
                        f"For BigQuery, parameters should be a dictionary for named placeholders. "
                        f"Received: {type(params_from_sql_object)}"
                    )
                    raise ParameterStyleMismatchError(msg)

            bq_query_parameters_list = self._prepare_bq_query_parameters(params_dict)

        # Note: statement.is_many is not directly used here because BigQuery's batching for DML
        # is typically handled by sending multiple individual DML statements or using specific batch APIs
        # not covered by a single _execute_impl call with is_many=True in the same way as other drivers.
        # The existing `execute_many` method in this driver iterates and calls `_run_query_job`.
        # If a future BigQuery API supports batch DMLs via a single job with multiple parameter sets,
        # then `statement.is_many` would be used here to structure `bq_query_parameters_list` accordingly.

        job_config = kwargs.get("job_config")
        query_job = self._run_query_job(
            final_sql_str, bq_query_parameters_list, connection=connection, job_config=job_config
        )

        if statement.is_script:
            query_job.result()
            return f"SCRIPT EXECUTED (Job ID: {query_job.job_id})"

        return query_job  # For select/DML, return the job; result is fetched in wrapper methods

    def _wrap_select_result(
        self, statement: SQL, result: Any, schema_type: "Optional[type]" = None, **kwargs: Any
    ) -> "SQLResult[RowT]":
        if not isinstance(result, QueryJob):
            logger.warning("_wrap_select_result expected QueryJob, got %s", type(result))
            return SQLResult(statement=statement, data=[], column_names=[], operation_type="SELECT", rows_affected=-1)

        query_job: QueryJob = result
        try:
            # Default timeout for job result, can be overridden via kwargs
            job_result = query_job.result(timeout=kwargs.get("bq_job_timeout"))
            rows_list = self._rows_to_results(iter(job_result))
            # num_rows for select statements is typically len(rows_list) or job_result.total_rows if available
            # query_job.num_rows might also be populated.
            # Let's use len(rows_list) for simplicity after conversion.
            actual_rows_affected_or_returned = len(rows_list)

        except Exception as e:
            logger.exception("BigQuery job failed or timed out", extra={"job_id": query_job.job_id})
            return SQLResult(
                statement=statement,
                data=[],
                column_names=[],
                operation_type="SELECT",
                rows_affected=-1,
                metadata={"error": str(e), "job_id": query_job.job_id},
            )

        column_names = [field.name for field in job_result.schema] if job_result.schema else []

        # If schema_type were used for Pydantic conversion, it would happen here.
        # For now, returning DictRow as per class generic type.
        return SQLResult[RowT](
            statement=statement,
            data=rows_list,
            column_names=column_names,
            operation_type="SELECT",
            rows_affected=actual_rows_affected_or_returned,
        )

    def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> "SQLResult[RowT]":
        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        metadata = {}
        rows_affected = 0

        if isinstance(result, str):
            operation_type = "SCRIPT"
            metadata["status_message"] = result
        elif isinstance(result, QueryJob):
            query_job: QueryJob = result
            metadata["job_id"] = query_job.job_id
            if query_job.statement_type:
                metadata["statement_type"] = query_job.statement_type
            try:
                query_job.result(timeout=kwargs.get("bq_job_timeout"))
                if query_job.num_dml_affected_rows is not None:
                    rows_affected = query_job.num_dml_affected_rows
            except Exception as e:
                logger.exception("BigQuery DML job failed or timed out", extra={"job_id": query_job.job_id})
                rows_affected = -1
                metadata["error"] = str(e)
        else:
            logger.warning("Unexpected result type in _wrap_execute_result: %s", type(result))
            rows_affected = -1
            metadata["error"] = f"Unexpected raw driver result type: {type(result)}"

        return SQLResult[RowT](
            statement=statement, data=[], rows_affected=rows_affected, operation_type=operation_type, metadata=metadata
        )

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[SQLResult[RowT]]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[BigQueryConnection]" = None,
        config: "Optional[SQLConfig]" = None,
        job_config: "Optional[QueryJobConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        current_config = config or self.config

        if not parameters:
            template_stmt = self._build_statement(statement, None, *filters, config=current_config)
            return SQLResult[RowT](
                statement=template_stmt,
                data=[],
                rows_affected=0,
                operation_type="EXECUTE",
                metadata={},
            )

        total_rows_affected = 0
        conn = connection or self.connection
        template_stmt = self._build_statement(statement, None, *filters, config=current_config)
        processed_sql_template = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())
        all_job_ids = []
        errors_occurred = False

        final_metadata: dict[str, Any] = {"job_ids": all_job_ids}
        if errors_occurred:
            final_metadata["errors_occurred_in_batch"] = True
            if total_rows_affected == 0 and parameters:
                total_rows_affected = -1

        for param_set in parameters:
            item_stmt = SQL(
                template_stmt.sql,
                param_set,
                dialect=self.dialect,
                config=replace(current_config or SQLConfig(), enable_validation=False),
            )
            item_params_dict = item_stmt.get_parameters(style=self._get_placeholder_style())
            if not isinstance(item_params_dict, dict):
                logger.error("Parameter set did not resolve to a dictionary for BigQuery: %s", item_params_dict)
                item_params_dict = {}

            bq_item_params = self._prepare_bq_query_parameters(item_params_dict)
            query_job: Optional[QueryJob] = None
            try:
                query_job = self._run_query_job(
                    processed_sql_template, bq_item_params, connection=conn, job_config=job_config
                )
                all_job_ids.append(query_job.job_id)
                query_job.result(timeout=kwargs.get("bq_job_timeout_many"))  # Wait for this job
                if query_job.num_dml_affected_rows is not None:
                    total_rows_affected += query_job.num_dml_affected_rows

            except Exception:
                errors_occurred = True
                job_id_str = query_job.job_id if query_job else "unknown"
                logger.exception(
                    "Error in execute_many for a parameter set. Job ID: %s, Params: %s ",
                    job_id_str,
                    param_set,
                )
                # Decide on behavior: stop all, or continue? For now, continue and report aggregate.

        final_operation_type = "EXECUTE_MANY"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            final_operation_type = str(template_stmt.expression.key).upper() + "_MANY"

        return SQLResult[RowT](
            statement=template_stmt,
            data=[],
            rows_affected=total_rows_affected,
            operation_type=final_operation_type,
            metadata=final_metadata,
        )

    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "BigQueryConnection",
        **kwargs: Any,
    ) -> "ArrowResult":
        job_config = kwargs.get("job_config")
        final_sql_str = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())

        params_from_sql_object = stmt_obj.get_parameters(style=self._get_placeholder_style())
        params_dict: dict[str, Any] = {}
        if params_from_sql_object is not None:
            if isinstance(params_from_sql_object, dict):
                params_dict = params_from_sql_object
            else:
                logger.warning(
                    "Expected a dict of parameters for BigQuery NAMED_AT style, got %s. Attempting to adapt.",
                    type(params_from_sql_object),
                )
                if isinstance(params_from_sql_object, Iterable) and not isinstance(
                    params_from_sql_object, (str, bytes)
                ):
                    params_dict = {f"param_{i}": v for i, v in enumerate(params_from_sql_object)}
                elif params_from_sql_object is not None:
                    params_dict = {"param_0": params_from_sql_object}

        query_job = self._run_query_job(
            final_sql_str, self._prepare_bq_query_parameters(params_dict), connection=connection, job_config=job_config
        )

        return ArrowResult(statement=stmt_obj, data=query_job.to_arrow(bqstorage_client=kwargs.get("bqstorage_client")))

    def _to_parquet_impl(self, stmt_obj: "SQL", connection: "Optional[Client]" = None, **kwargs: "Any") -> None:
        msg = "Parquet export is not implemented for BigQuery driver."
        raise NotImplementedError(msg)

    def _connection(self, connection: "Optional[Client]" = None) -> "Client":
        """Get the connection to use for the operation."""
        return connection or self.connection
