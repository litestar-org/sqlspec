"""Pipeline execution mixin for batch database operations.

This module provides mixins that enable pipelined execution of SQL statements,
allowing multiple operations to be sent to the database in a single network
round-trip for improved performance.

The implementation leverages native driver support where available (psycopg, asyncpg, oracledb)
and provides high-quality simulated behavior for others.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.exceptions import PipelineExecutionError
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from typing import Literal

    from sqlspec.config import DriverT
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
    filters: "Optional[list[StatementFilter]]" = None
    original_params: "Optional[Any]" = None


class SyncPipelinedExecutionMixin:
    """Mixin providing pipeline execution for sync drivers."""

    __slots__ = ()

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
            driver=cast("SyncDriverAdapterProtocol[Any, Any]", self),
            isolation_level=isolation_level,
            continue_on_error=continue_on_error,
            max_operations=max_operations,
            options=options,
        )


class AsyncPipelinedExecutionMixin:
    """Async version of pipeline execution mixin."""

    __slots__ = ()

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
            driver=cast("AsyncDriverAdapterProtocol[Any, Any]", self),
            isolation_level=isolation_level,
            continue_on_error=continue_on_error,
            max_operations=max_operations,
            options=options,
        )


class Pipeline:
    """Synchronous pipeline with enhanced parameter handling."""

    def __init__(
        self,
        driver: "DriverT",  # pyright: ignore
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
        self._results: Optional[list[SQLResult[Any]]] = None
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
        self._operations.append(
            PipelineOperation(
                sql=SQL(statement, *parameters, _config=self.driver.config, **kwargs), operation_type="execute"
            )
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
        self._operations.append(
            PipelineOperation(
                sql=SQL(statement, *parameters, _config=self.driver.config, **kwargs), operation_type="select"
            )
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
        # First parameter should be the batch data
        if not parameters or not isinstance(parameters[0], (list, tuple)):
            msg = "execute_many requires a sequence of parameter sets as first parameter"
            raise ValueError(msg)

        batch_params = parameters[0]
        # Convert tuple to list if needed
        if isinstance(batch_params, tuple):
            batch_params = list(batch_params)
        # Create SQL object and mark as many, passing remaining args as filters
        sql_obj = SQL(statement, *parameters[1:], **kwargs).as_many(batch_params)

        self._operations.append(PipelineOperation(sql=sql_obj, operation_type="execute_many"))
        return self

    def add_execute_script(self, script: "Union[str, SQL]", *filters: StatementFilter, **kwargs: Any) -> "Pipeline":
        """Add a multi-statement script to the pipeline."""
        if isinstance(script, SQL):
            sql_obj = script.as_script()
        else:
            sql_obj = SQL(script, *filters, _config=self.driver.config, **kwargs).as_script()

        self._operations.append(PipelineOperation(sql=sql_obj, operation_type="execute_script"))
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
            results = self.driver._execute_pipeline_native(self._operations, **self.options)  # pyright: ignore
        else:
            results = self._execute_pipeline_simulated()

        self._results = results
        self._operations.clear()
        return cast("list[SQLResult]", results)

    def _execute_pipeline_simulated(self) -> "list[SQLResult]":
        """Enhanced simulation with transaction support and error handling."""
        results: list[SQLResult[Any]] = []
        connection = None
        auto_transaction = False

        # Only log once per pipeline, not for each operation
        if not self._simulation_logged:
            logger.info(
                "%s using simulated pipeline. Native support: %s",
                self.driver.__class__.__name__,
                self._has_native_support(),
            )
            self._simulation_logged = True

        try:
            # Get a connection for the entire pipeline
            connection = self.driver._connection()

            # Start transaction if not already in one
            if self.isolation_level:
                # Set isolation level if specified
                pass  # Driver-specific implementation

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
        self, i: int, op: PipelineOperation, results: "list[SQLResult[Any]]", connection: Any, auto_transaction: bool
    ) -> None:
        """Execute a single pipeline operation with error handling."""
        try:
            # Execute based on operation type
            result: SQLResult[Any]
            if op.operation_type == "execute_script":
                result = cast("SQLResult[Any]", self.driver.execute_script(op.sql, _connection=connection))
            elif op.operation_type == "execute_many":
                result = cast("SQLResult[Any]", self.driver.execute_many(op.sql, _connection=connection))
            else:
                result = cast("SQLResult[Any]", self.driver.execute(op.sql, _connection=connection))

            # Add operation context to result
            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if self.continue_on_error:
                # Create error result
                error_result = SQLResult(
                    statement=op.sql, data=[], error=e, operation_index=i, parameters=op.sql.parameters
                )
                results.append(error_result)
            else:
                if auto_transaction and hasattr(connection, "rollback"):
                    connection.rollback()
                msg = f"Pipeline failed at operation {i}: {e}"
                raise PipelineExecutionError(
                    msg, operation_index=i, partial_results=results, failed_operation=op
                ) from e

    def _apply_global_filters(self, filters: "list[StatementFilter]") -> None:
        """Apply global filters to all operations."""
        for operation in self._operations:
            # Add filters to each operation
            if operation.filters is None:
                operation.filters = []
            operation.filters.extend(filters)

    def _apply_operation_filters(self, sql: SQL, filters: "list[StatementFilter]") -> SQL:
        """Apply filters to a SQL object."""
        result = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result = cast("Any", filter_obj).apply(result)
        return result

    def _has_native_support(self) -> bool:
        """Check if driver has native pipeline support."""
        return hasattr(self.driver, "_execute_pipeline_native")

    def _process_parameters(self, params: tuple[Any, ...]) -> tuple["list[StatementFilter]", "Optional[Any]"]:
        """Extract filters and parameters from mixed args.

        Returns:
            Tuple of (filters, parameters)
        """
        filters: list[StatementFilter] = []
        parameters: list[Any] = []

        for param in params:
            if isinstance(param, StatementFilter):
                filters.append(param)
            else:
                parameters.append(param)

        # Return parameters based on count
        if not parameters:
            return filters, None
        if len(parameters) == 1:
            return filters, parameters[0]
        return filters, parameters

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
        self._results: Optional[list[SQLResult[Any]]] = None
        self._simulation_logged = False

    async def add_execute(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "AsyncPipeline":
        """Add an execute operation to the async pipeline."""
        self._operations.append(
            PipelineOperation(
                sql=SQL(statement, *parameters, _config=self.driver.config, **kwargs), operation_type="execute"
            )
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
        self._operations.append(
            PipelineOperation(
                sql=SQL(statement, *parameters, _config=self.driver.config, **kwargs), operation_type="select"
            )
        )
        return self

    async def add_execute_many(
        self, statement: "Union[str, SQL]", /, *parameters: "Union[StatementParameters, StatementFilter]", **kwargs: Any
    ) -> "AsyncPipeline":
        """Add batch execution to the async pipeline."""
        # First parameter should be the batch data
        if not parameters or not isinstance(parameters[0], (list, tuple)):
            msg = "execute_many requires a sequence of parameter sets as first parameter"
            raise ValueError(msg)

        batch_params = parameters[0]
        # Convert tuple to list if needed
        if isinstance(batch_params, tuple):
            batch_params = list(batch_params)
        # Create SQL object and mark as many, passing remaining args as filters
        sql_obj = SQL(statement, *parameters[1:], **kwargs).as_many(batch_params)

        self._operations.append(PipelineOperation(sql=sql_obj, operation_type="execute_many"))
        return self

    async def add_execute_script(
        self, script: "Union[str, SQL]", *filters: StatementFilter, **kwargs: Any
    ) -> "AsyncPipeline":
        """Add a script to the async pipeline."""
        if isinstance(script, SQL):
            sql_obj = script.as_script()
        else:
            sql_obj = SQL(script, *filters, _config=self.driver.config, **kwargs).as_script()

        self._operations.append(PipelineOperation(sql=sql_obj, operation_type="execute_script"))
        return self

    async def process(self, filters: "Optional[list[StatementFilter]]" = None) -> "list[SQLResult]":
        """Execute all queued operations asynchronously."""
        if not self._operations:
            return []

        # Check for native support
        if hasattr(self.driver, "_execute_pipeline_native"):
            results = await cast("Any", self.driver)._execute_pipeline_native(self._operations, **self.options)
        else:
            results = await self._execute_pipeline_simulated()

        self._results = results
        self._operations.clear()
        return cast("list[SQLResult]", results)

    async def _execute_pipeline_simulated(self) -> "list[SQLResult]":
        """Async version of simulated pipeline execution."""
        results: list[SQLResult[Any]] = []
        connection = None
        auto_transaction = False

        if not self._simulation_logged:
            logger.info(
                "%s using simulated async pipeline. Native support: %s",
                self.driver.__class__.__name__,
                self._has_native_support(),
            )
            self._simulation_logged = True

        try:
            connection = self.driver._connection()

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
        self, i: int, op: PipelineOperation, results: "list[SQLResult[Any]]", connection: Any, auto_transaction: bool
    ) -> None:
        """Execute a single async pipeline operation with error handling."""
        try:
            result: SQLResult[Any]
            if op.operation_type == "execute_script":
                result = await self.driver.execute_script(op.sql, _connection=connection)
            elif op.operation_type == "execute_many":
                result = await self.driver.execute_many(op.sql, _connection=connection)
            else:
                result = await self.driver.execute(op.sql, _connection=connection)

            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if self.continue_on_error:
                error_result = SQLResult(
                    statement=op.sql, data=[], error=e, operation_index=i, parameters=op.sql.parameters
                )
                results.append(error_result)
            else:
                if auto_transaction and hasattr(connection, "rollback"):
                    await connection.rollback()
                msg = f"Async pipeline failed at operation {i}: {e}"
                raise PipelineExecutionError(
                    msg, operation_index=i, partial_results=results, failed_operation=op
                ) from e

    def _has_native_support(self) -> bool:
        """Check if driver has native pipeline support."""
        return hasattr(self.driver, "_execute_pipeline_native")

    @property
    def operations(self) -> "list[PipelineOperation]":
        """Get the current list of queued operations."""
        return self._operations.copy()
