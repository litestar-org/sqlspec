"""Pipeline execution mixin for batch database operations.

This module provides mixins that enable pipelined execution of SQL statements,
allowing multiple operations to be sent to the database in a single network
round-trip for improved performance.

The implementation leverages native driver support where available (psycopg, asyncpg, oracledb)
and provides high-quality simulated behavior for others.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlspec.exceptions import PipelineExecutionError
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from typing import Literal

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.typing import StatementParameters

__all__ = (
    "AsyncPipeline",
    "AsyncPipelinedExecutionMixin",
    "Pipeline",
    "PipelineOperation",
    "SyncPipelinedExecutionMixin",
)

logger = get_logger(__name__)


@dataclass
class PipelineOperation:
    """Container for a queued pipeline operation."""

    sql: SQL
    operation_type: "Literal['execute', 'execute_many', 'execute_script', 'select']"
    filters: "list[StatementFilter]"
    original_params: Any


class SyncPipelinedExecutionMixin:
    """Mixin providing pipeline execution for sync drivers."""

    def pipeline(
        self,
        *,
        isolation_level: "Optional[str]" = None,
        continue_on_error: bool = False,
        max_operations: int = 1000,
        **options: Any,
    ) -> "Pipeline":
        """Create a new pipeline for batch operations.

        Args:
            isolation_level: Transaction isolation level
            continue_on_error: Continue processing after errors
            max_operations: Maximum operations before auto-flush
            **options: Driver-specific pipeline options

        Returns:
            A new Pipeline instance for queuing operations
        """
        return Pipeline(
            driver=self,
            isolation_level=isolation_level,
            continue_on_error=continue_on_error,
            max_operations=max_operations,
            options=options,
        )


class AsyncPipelinedExecutionMixin:
    """Async version of pipeline execution mixin."""

    def pipeline(
        self,
        *,
        isolation_level: "Optional[str]" = None,
        continue_on_error: bool = False,
        max_operations: int = 1000,
        **options: Any,
    ) -> "AsyncPipeline":
        """Create a new async pipeline for batch operations."""
        return AsyncPipeline(
            driver=self,
            isolation_level=isolation_level,
            continue_on_error=continue_on_error,
            max_operations=max_operations,
            options=options,
        )


class Pipeline:
    """Synchronous pipeline with enhanced parameter handling."""

    def __init__(
        self,
        driver: "SyncDriverAdapterProtocol[Any, Any]",
        isolation_level: "Optional[str]" = None,
        continue_on_error: bool = False,
        max_operations: int = 1000,
        options: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.driver = driver
        self.isolation_level = isolation_level
        self.continue_on_error = continue_on_error
        self.max_operations = max_operations
        self.options = options or {}
        self._operations: list[PipelineOperation] = []
        self._results: Optional[list[SQLResult]] = None
        self._simulation_logged = False

    def add_execute(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "Pipeline":
        """Add an execute operation to the pipeline.

        Args:
            statement: SQL statement to execute
            *parameters: Mixed positional args containing parameters and filters
            **kwargs: Named parameters

        Returns:
            Self for fluent API
        """
        filters, params = self._process_parameters(parameters)
        if kwargs:
            if params is None:
                params = kwargs
            elif isinstance(params, dict):
                params = {**params, **kwargs}
            else:
                params = kwargs

        sql_obj = SQL(statement, params, config=self.driver.config)
        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="execute", filters=filters, original_params=params)
        )

        # Check for auto-flush
        if len(self._operations) >= self.max_operations:
            logger.warning("Pipeline auto-flushing at %s operations", len(self._operations))
            self.process()

        return self

    def add_select(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "Pipeline":
        """Add a select operation to the pipeline."""
        filters, params = self._process_parameters(parameters)

        # Merge kwargs into params if provided
        if kwargs:
            if params is None:
                params = kwargs
            elif isinstance(params, dict):
                params = {**params, **kwargs}
            else:
                params = kwargs

        sql_obj = SQL(statement, params, config=self.driver.config)
        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="select", filters=filters, original_params=params)
        )
        return self

    def add_execute_many(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "Pipeline":
        """Add batch execution preserving parameter types.

        Args:
            statement: SQL statement to execute multiple times
            *parameters: First arg should be batch data (list of param sets),
                        followed by optional StatementFilter instances
            **kwargs: Not typically used for execute_many
        """
        filters, params = self._process_parameters(parameters)

        # First non-filter parameter should be the batch data
        if not params or not isinstance(params, (list, tuple)):
            msg = "execute_many requires a sequence of parameter sets as first parameter"
            raise ValueError(msg)

        # params is already the batch data from _process_parameters
        batch_params = params
        sql_obj = SQL(statement).as_many()
        sql_obj._raw_parameters = batch_params

        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="execute_many", filters=filters, original_params=batch_params)
        )
        return self

    def add_execute_script(self, script: "Union[str, SQL]", *filters: StatementFilter, **kwargs: Any) -> "Pipeline":
        """Add a multi-statement script to the pipeline."""
        if isinstance(script, SQL):
            script_str = script.to_sql()
            base_params = script.parameters
        else:
            script_str = script
            base_params = kwargs

        sql_obj = SQL(script_str, base_params, config=self.driver.config).as_script()

        self._operations.append(
            PipelineOperation(
                sql=sql_obj, operation_type="execute_script", filters=list(filters), original_params=base_params
            )
        )
        return self

    def process(self, filters: "Optional[list[StatementFilter]]" = None) -> "list[SQLResult]":
        """Execute all queued operations.

        Args:
            filters: Global filters to apply to all operations

        Returns:
            List of results from all operations
        """
        if not self._operations:
            return []

        # Apply global filters
        if filters:
            self._apply_global_filters(filters)

        # Check for native support
        if hasattr(self.driver, "_execute_pipeline_native"):
            results = self.driver._execute_pipeline_native(self._operations, **self.options)
        else:
            results = self._execute_pipeline_simulated()

        self._results = results
        self._operations.clear()
        return results

    def _process_parameters(self, parameters: "tuple[Any, ...]") -> "tuple[list[StatementFilter], Any]":
        """Separate filters from parameters in positional args."""
        filters = []
        params = []

        for arg in parameters:
            if isinstance(arg, StatementFilter):
                filters.append(arg)
            else:
                # Everything else is treated as parameters
                params.append(arg)

        # Convert to appropriate parameter format
        if len(params) == 0:
            return filters, None
        if len(params) == 1 and isinstance(params[0], (list, tuple, dict)):
            return filters, params[0]
        return filters, params

    def _apply_global_filters(self, filters: "list[StatementFilter]") -> None:
        """Apply filters to all operations in the pipeline."""
        for operation in self._operations:
            operation.filters.extend(filters)

    def _execute_pipeline_simulated(self) -> "list[SQLResult]":
        """Enhanced simulation with transaction support and error handling."""
        results = []
        connection = None

        # Only log once per pipeline, not for each operation
        if not self._simulation_logged:
            logger.info(
                "%s using simulated pipeline. Native support: %s",
                self.driver.__class__.__name__,
                self._has_native_support()
            )
            self._simulation_logged = True

        try:
            # Get a connection for the entire pipeline
            connection = self.driver._connection()

            # Start transaction if not already in one
            if self.isolation_level:
                # Set isolation level if specified
                pass  # Driver-specific implementation

            auto_transaction = False
            if hasattr(connection, "in_transaction") and not connection.in_transaction():
                if hasattr(connection, "begin"):
                    connection.begin()
                auto_transaction = True

            # Process each operation
            for i, op in enumerate(self._operations):
                self._execute_single_operation(i, op, results, connection, auto_transaction)

            # Commit if we started the transaction
            if auto_transaction and hasattr(connection, "commit"):
                connection.commit()

        except Exception as e:
            if connection and auto_transaction and hasattr(connection, "rollback"):
                connection.rollback()
            if not isinstance(e, PipelineExecutionError):
                msg = f"Pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    def _execute_single_operation(
        self,
        i: int,
        op: PipelineOperation,
        results: "list[SQLResult]",
        connection: Any,
        auto_transaction: bool,
    ) -> None:
        """Execute a single pipeline operation with error handling."""
        try:
            # Apply operation-specific filters
            filtered_sql = self._apply_operation_filters(op.sql, op.filters)

            # Execute based on operation type
            if op.operation_type == "execute_script":
                result = self.driver.execute_script(filtered_sql, connection=connection)
            elif op.operation_type == "execute_many":
                result = self.driver.execute_many(
                    filtered_sql, parameters=op.original_params, connection=connection
                )
            elif op.operation_type == "select":
                result = self.driver.execute(filtered_sql, connection=connection)
                # Ensure it's treated as a select
                result.operation_type = "select"
            else:
                result = self.driver.execute(filtered_sql, connection=connection)

            # Add operation context to result
            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if self.continue_on_error:
                # Create error result
                error_result = SQLResult(
                    error=e, statement=op.sql, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
            else:
                # Rollback and raise
                if auto_transaction and hasattr(connection, "rollback"):
                    connection.rollback()
                msg = f"Pipeline failed at operation {i}: {e}"
                raise PipelineExecutionError(
                    msg,
                    operation_index=i,
                    partial_results=results,
                    failed_operation=op,
                ) from e

    def _apply_operation_filters(self, sql: SQL, filters: "list[StatementFilter]") -> SQL:
        """Apply filters to a SQL object."""
        if not filters:
            return sql

        # Apply each filter in sequence
        result_sql = sql
        for filter_obj in filters:
            result_sql = filter_obj.apply(result_sql)

        return result_sql

    def _has_native_support(self) -> bool:
        """Check if driver has native pipeline support."""
        return hasattr(self.driver, "_execute_pipeline_native")

    @property
    def operations(self) -> "list[PipelineOperation]":
        """Get the current list of queued operations."""
        return self._operations.copy()


class AsyncPipeline:
    """Asynchronous pipeline with identical structure to Pipeline."""

    def __init__(
        self,
        driver: "AsyncDriverAdapterProtocol[Any, Any]",
        isolation_level: "Optional[str]" = None,
        continue_on_error: bool = False,
        max_operations: int = 1000,
        options: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.driver = driver
        self.isolation_level = isolation_level
        self.continue_on_error = continue_on_error
        self.max_operations = max_operations
        self.options = options or {}
        self._operations: list[PipelineOperation] = []
        self._results: Optional[list[SQLResult]] = None
        self._simulation_logged = False

    async def add_execute(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "AsyncPipeline":
        """Add an execute operation to the async pipeline."""
        filters, params = self._process_parameters(parameters)

        # Merge kwargs into params if provided
        if kwargs:
            if params is None:
                params = kwargs
            elif isinstance(params, dict):
                params = {**params, **kwargs}
            else:
                params = kwargs

        sql_obj = SQL(statement, params, config=self.driver.config)
        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="execute", filters=filters, original_params=params)
        )

        # Check for auto-flush
        if len(self._operations) >= self.max_operations:
            logger.warning("Async pipeline auto-flushing at %s operations", len(self._operations))
            await self.process()

        return self

    async def add_select(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "AsyncPipeline":
        """Add a select operation to the async pipeline."""
        filters, params = self._process_parameters(parameters)

        if kwargs:
            if params is None:
                params = kwargs
            elif isinstance(params, dict):
                params = {**params, **kwargs}
            else:
                params = kwargs

        sql_obj = SQL(statement, params, config=self.driver.config)
        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="select", filters=filters, original_params=params)
        )
        return self

    async def add_execute_many(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "AsyncPipeline":
        """Add batch execution to the async pipeline."""
        filters, params = self._process_parameters(parameters)

        if not params or not isinstance(params, (list, tuple)):
            msg = "execute_many requires a sequence of parameter sets as first parameter"
            raise ValueError(msg)

        batch_params = params
        sql_obj = SQL(statement).as_many()
        sql_obj._raw_parameters = batch_params

        self._operations.append(
            PipelineOperation(sql=sql_obj, operation_type="execute_many", filters=filters, original_params=batch_params)
        )
        return self

    async def add_execute_script(
        self, script: "Union[str, SQL]", *filters: StatementFilter, **kwargs: Any
    ) -> "AsyncPipeline":
        """Add a script to the async pipeline."""
        if isinstance(script, SQL):
            script_str = script.to_sql()
            base_params = script.parameters
        else:
            script_str = script
            base_params = kwargs

        sql_obj = SQL(script_str, base_params, config=self.driver.config).as_script()

        self._operations.append(
            PipelineOperation(
                sql=sql_obj, operation_type="execute_script", filters=list(filters), original_params=base_params
            )
        )
        return self

    async def process(self, filters: "Optional[list[StatementFilter]]" = None) -> "list[SQLResult]":
        """Execute all queued operations asynchronously."""
        if not self._operations:
            return []

        # Apply global filters
        if filters:
            self._apply_global_filters(filters)

        # Check for native support
        if hasattr(self.driver, "_execute_pipeline_native"):
            results = await self.driver._execute_pipeline_native(self._operations, **self.options)
        else:
            results = await self._execute_pipeline_simulated()

        self._results = results
        self._operations.clear()
        return results

    def _process_parameters(self, parameters: "tuple[Any, ...]") -> "tuple[list[StatementFilter], Any]":
        """Separate filters from parameters (same as sync version)."""
        filters = []
        params = []

        for arg in parameters:
            if isinstance(arg, StatementFilter):
                filters.append(arg)
            else:
                params.append(arg)

        if len(params) == 0:
            return filters, None
        if len(params) == 1 and isinstance(params[0], (list, tuple, dict)):
            return filters, params[0]
        return filters, params

    def _apply_global_filters(self, filters: "list[StatementFilter]") -> None:
        """Apply filters to all operations (same as sync version)."""
        for operation in self._operations:
            operation.filters.extend(filters)

    async def _execute_pipeline_simulated(self) -> "list[SQLResult]":
        """Async version of simulated pipeline execution."""
        results = []
        connection = None

        if not self._simulation_logged:
            logger.info(
                "%s using simulated async pipeline. Native support: %s",
                self.driver.__class__.__name__,
                self._has_native_support()
            )
            self._simulation_logged = True

        try:
            connection = self.driver._connection()

            auto_transaction = False
            if hasattr(connection, "in_transaction") and not connection.in_transaction():
                if hasattr(connection, "begin"):
                    await connection.begin()
                auto_transaction = True

            # Process each operation
            for i, op in enumerate(self._operations):
                await self._execute_single_operation_async(i, op, results, connection, auto_transaction)

            if auto_transaction and hasattr(connection, "commit"):
                await connection.commit()

        except Exception as e:
            if connection and auto_transaction and hasattr(connection, "rollback"):
                await connection.rollback()
            if not isinstance(e, PipelineExecutionError):
                msg = f"Async pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    async def _execute_single_operation_async(
        self,
        i: int,
        op: PipelineOperation,
        results: "list[SQLResult]",
        connection: Any,
        auto_transaction: bool,
    ) -> None:
        """Execute a single async pipeline operation with error handling."""
        try:
            filtered_sql = self._apply_operation_filters(op.sql, op.filters)

            if op.operation_type == "execute_script":
                result = await self.driver.execute_script(filtered_sql, connection=connection)
            elif op.operation_type == "execute_many":
                result = await self.driver.execute_many(
                    filtered_sql, parameters=op.original_params, connection=connection
                )
            elif op.operation_type == "select":
                result = await self.driver.execute(filtered_sql, connection=connection)
                result.operation_type = "select"
            else:
                result = await self.driver.execute(filtered_sql, connection=connection)

            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if self.continue_on_error:
                error_result = SQLResult(
                    error=e, statement=op.sql, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
            else:
                if auto_transaction and hasattr(connection, "rollback"):
                    await connection.rollback()
                msg = f"Async pipeline failed at operation {i}: {e}"
                raise PipelineExecutionError(
                    msg,
                    operation_index=i,
                    partial_results=results,
                    failed_operation=op,
                ) from e

    def _apply_operation_filters(self, sql: SQL, filters: "list[StatementFilter]") -> SQL:
        """Apply filters to a SQL object (same as sync version)."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            result_sql = filter_obj.apply(result_sql)

        return result_sql

    def _has_native_support(self) -> bool:
        """Check if driver has native pipeline support."""
        return hasattr(self.driver, "_execute_pipeline_native")

    @property
    def operations(self) -> "list[PipelineOperation]":
        """Get the current list of queued operations."""
        return self._operations.copy()
