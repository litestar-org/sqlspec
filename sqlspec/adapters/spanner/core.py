"""Spanner adapter compiled helpers."""

import contextlib
from functools import partial
from itertools import islice
from typing import TYPE_CHECKING, Any, Literal, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from sqlspec.adapters.spanner._typing import SpannerJsonObject as JsonObject
from sqlspec.adapters.spanner._typing import SpannerTypeCode as TypeCode
from sqlspec.adapters.spanner._typing import spanner_exceptions as api_exceptions
from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    TypedParameter,
    build_statement_config_from_profile,
)
from sqlspec.driver import rows_to_dicts
from sqlspec.exceptions import (
    DeadlockError,
    NotFoundError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLConversionError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping, Sequence

    from sqlspec.adapters.spanner._typing import SpannerDirectedReadOptions as DirectedReadOptions
    from sqlspec.adapters.spanner._typing import SpannerExecuteSqlRequest as ExecuteSqlRequest
    from sqlspec.adapters.spanner._typing import SpannerRequestOptions as RequestOptions
    from sqlspec.adapters.spanner._typing import SpannerRetry as Retry
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowReturnFormat, ArrowTable

__all__ = (
    "SpannerAsyncStreamSource",
    "SpannerExecuteOptions",
    "SpannerSyncStreamSource",
    "apply_driver_features",
    "build_execute_kwargs",
    "build_param_type_signature",
    "build_profile",
    "build_session_driver_features",
    "build_statement_config",
    "chunk_mutation_rows",
    "coerce_params",
    "collect_rows",
    "create_arrow_data",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "execute_ddl_async",
    "execute_ddl_sync",
    "infer_param_types",
    "is_query_statement",
    "list_existing_table_names_async",
    "list_existing_table_names_sync",
    "pop_execute_options",
    "resolve_column_names",
    "resolve_row_plan",
    "resolve_transaction_completion",
    "run_in_transaction_async",
    "run_in_transaction_sync",
    "supports_batch_update",
    "supports_write",
)

COLUMN_CACHE_MAX_SIZE: int = 128
_MAX_MUTATIONS_PER_COMMIT = 80_000


class SpannerExecuteOptions:
    """Per-call Spanner execution options captured for a single dispatch."""

    __slots__ = ("directed_read_options", "last_statement", "query_options", "request_options", "retry", "timeout")

    def __init__(
        self,
        *,
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        retry: "Retry | None" = None,
        timeout: "float | None" = None,
        last_statement: bool = False,
    ) -> None:
        self.request_options = request_options
        self.query_options = query_options
        self.directed_read_options = directed_read_options
        self.retry = retry
        self.timeout = timeout
        self.last_statement = last_statement


def build_execute_kwargs(
    driver_features: dict[str, Any],
    pending: "SpannerExecuteOptions | None",
    *,
    for_read: bool = False,
    for_batch: bool = False,
) -> dict[str, Any]:
    """Merge driver defaults and per-call options for the native execution method."""
    kwargs: dict[str, Any] = {key: driver_features[key] for key in ("retry", "timeout") if key in driver_features}
    request_options = driver_features.get("request_options")
    if request_options is not None:
        kwargs["request_options"] = request_options
    if not for_batch:
        query_options = driver_features.get("query_options")
        if query_options is not None:
            kwargs["query_options"] = query_options
    if for_read and not for_batch:
        directed_read_options = driver_features.get("directed_read_options")
        if directed_read_options is not None:
            kwargs["directed_read_options"] = directed_read_options
    if pending is not None:
        if pending.request_options is not None:
            kwargs["request_options"] = pending.request_options
        if not for_batch and pending.query_options is not None:
            kwargs["query_options"] = pending.query_options
        if pending.retry is not None:
            kwargs["retry"] = pending.retry
        if pending.timeout is not None:
            kwargs["timeout"] = pending.timeout
        if for_read and not for_batch and pending.directed_read_options is not None:
            kwargs["directed_read_options"] = pending.directed_read_options
        if not for_read and pending.last_statement:
            kwargs["last_statement"] = True
    return kwargs


def pop_execute_options(kwargs: dict[str, Any]) -> "SpannerExecuteOptions | None":
    """Remove native execution options before processing SQL parameters."""
    if not any(
        key in kwargs
        for key in ("request_options", "query_options", "directed_read_options", "retry", "timeout", "last_statement")
    ):
        return None
    return SpannerExecuteOptions(
        request_options=kwargs.pop("request_options", None),
        query_options=kwargs.pop("query_options", None),
        directed_read_options=kwargs.pop("directed_read_options", None),
        retry=kwargs.pop("retry", None),
        timeout=kwargs.pop("timeout", None),
        last_statement=bool(kwargs.pop("last_statement", False)),
    )


def chunk_mutation_rows(
    columns: "list[str]",
    records: "list[tuple[Any, ...]]",
    coerce_row: "Callable[[dict[str, Any]], dict[str, Any] | None]",
) -> "list[list[list[Any]]]":
    """Coerce Arrow rows into chunks bounded by Spanner's mutation-group ceiling."""
    column_count = len(columns)
    max_cells = _MAX_MUTATIONS_PER_COMMIT
    chunks: list[list[list[Any]]] = []
    values: list[list[Any]] = []
    pending_cells = 0
    for record in records:
        if values and pending_cells + column_count > max_cells:
            chunks.append(values)
            values = []
            pending_cells = 0
        coerced = coerce_row({f"p{i}": value for i, value in enumerate(record)}) or {}
        values.append([coerced.get(f"p{i}") for i in range(column_count)])
        pending_cells += column_count
        if pending_cells == max_cells:
            chunks.append(values)
            values = []
            pending_cells = 0
    if values:
        chunks.append(values)
    return chunks


def build_profile() -> "DriverParameterProfile":
    """Create the Spanner driver parameter profile."""

    return DriverParameterProfile(
        name="Spanner",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        json_serializer_strategy="none",
        default_dialect="spanner",
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        custom_type_coercions=None,
        extras={},
    )


def build_statement_config() -> StatementConfig:
    """Construct the Spanner statement configuration."""
    profile = driver_profile
    return build_statement_config_from_profile(profile, statement_overrides={"dialect": "spanner"})


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply Spanner driver feature defaults."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_features.setdefault("enable_uuid_conversion", True)
    processed_features.setdefault("json_serializer", to_json)
    processed_features.setdefault("json_deserializer", from_json)
    return statement_config, processed_features


def supports_write(cursor: Any) -> bool:
    """Return True when the cursor supports DML execution."""
    try:
        _ = cursor.execute_update
    except AttributeError:
        return False
    return True


def supports_batch_update(cursor: Any) -> bool:
    """Return True when the cursor supports batch updates."""
    try:
        _ = cursor.batch_update
    except AttributeError:
        return False
    return True


def infer_param_types(params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
    """Infer Spanner param_types from Python values."""
    if not isinstance(params, dict):
        return {}
    return infer_spanner_param_types(params)


def build_param_type_signature(params: "dict[str, Any] | None") -> "tuple[tuple[str, type[Any], Any], ...]":
    """Build a hashable signature for Spanner param type inference caching.

    The declared type is part of the key because every NULL has the same runtime
    type, so two differently-typed NULLs would otherwise share a cache entry.

    Args:
        params: Parameter mapping as supplied, before coercion.

    Returns:
        Tuple signature of parameter name, runtime type, and declared type.
    """
    if not params:
        return ()
    signature: list[tuple[str, type[Any], Any]] = []
    for key, value in params.items():
        if type(value) is TypedParameter:
            signature.append((key, type(value.value), value.original_type))
        else:
            signature.append((key, type(value), None))
    return tuple(signature)


def resolve_column_names(fields: "Sequence[Any] | None", cache: "dict[int, tuple[Any, list[str]]]") -> list[str]:
    """Resolve and cache Spanner field names for row materialization."""
    if not fields:
        return []

    cache_key = id(fields)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is fields:
        return cached[1]

    column_names = [field.name for field in fields]
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (fields, column_names)
    return column_names


def resolve_row_plan(
    fields: "Sequence[Any] | None",
    cache: "dict[int, tuple[Any, list[str], tuple[tuple[int, Any], ...] | None]]",
    *,
    json_deserializer: "Callable[[str], Any]",
) -> "tuple[list[str], tuple[tuple[int, Any], ...] | None]":
    """Resolve column names and metadata-driven converters for a Spanner result."""
    if not fields:
        return [], None

    cache_key = id(fields)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is fields:
        return cached[1], cached[2]

    column_names = [getattr(field, "name", "") for field in fields]
    plan_entries: list[tuple[int, Any]] = []
    append_plan_entry = plan_entries.append
    convert_json = partial(_convert_json_row_value, json_deserializer=json_deserializer)
    for index, field in enumerate(fields):
        field_type = getattr(field, "type_", None)
        type_code = getattr(field_type, "code", None)
        if type_code == TypeCode.JSON:
            append_plan_entry((index, convert_json))

    plan = tuple(plan_entries) if plan_entries else None
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (fields, column_names, plan)
    return column_names, plan


def coerce_params(
    params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None",
    *,
    json_serializer: "Callable[[Any], str] | None" = None,
    enable_uuid_conversion: bool = True,
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats."""
    if not isinstance(params, dict):
        return None
    return coerce_params_for_spanner(
        params, json_serializer=json_serializer, enable_uuid_conversion=enable_uuid_conversion
    )


def collect_rows(
    rows: "list[Any]",
    fields: "Sequence[Any]",
    *,
    column_names: "list[str] | None" = None,
    column_plan: "tuple[tuple[int, Any], ...] | None" = None,
) -> "tuple[list[Any], list[str]]":
    """Collect Spanner rows as tuples with type conversion applied.

    When a metadata-driven column plan is unavailable, the original row list
    is returned unchanged. When a plan exists, only the planned columns are
    converted and the rows are copied once.

    Args:
        rows: Rows from result set.
        fields: Result set fields metadata.
        column_names: Optional precomputed column names.
        column_plan: Optional zero-copy column conversion plan.

    Returns:
        Tuple of (rows, column_names).
    """
    resolved_column_names = column_names if column_names is not None else [field.name for field in fields]
    if column_plan is None and all(type(row) is tuple for row in rows):
        return rows, resolved_column_names
    if column_plan is None:
        return [tuple(row) for row in rows], resolved_column_names

    data: list[Any] = []
    append = data.append
    for row in rows:
        converted_row = list(row)
        for index, converter in column_plan:
            converted_row[index] = converter(converted_row[index])
        append(tuple(converted_row))
    return data, resolved_column_names


def create_arrow_data(
    data: "list[dict[str, Any]]", return_format: "ArrowReturnFormat"
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Create Arrow data from Spanner row dictionaries.

    Args:
        data: Row dictionaries from Spanner results.
        return_format: Arrow return format.

    Returns:
        Arrow data in the requested format.
    """
    return convert_dict_to_arrow(data, return_format=return_format)


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map Spanner exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. Native google.api_core exception types (isinstance checks)
        2. Default SQLSpecError fallback

    Args:
        error: The Spanner exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    if isinstance(error, api_exceptions.AlreadyExists):
        return _create_spanner_error(error, UniqueViolationError, "resource already exists")

    if isinstance(error, api_exceptions.NotFound):
        return _create_spanner_error(error, NotFoundError, "resource not found")

    if isinstance(error, api_exceptions.InvalidArgument):
        return _create_spanner_error(error, SQLParsingError, "invalid query or argument")

    if isinstance(error, api_exceptions.PermissionDenied):
        return _create_spanner_error(error, PermissionDeniedError, "permission denied")
    if isinstance(error, api_exceptions.Unauthenticated):
        return _create_spanner_error(error, PermissionDeniedError, "authentication failed")

    if isinstance(error, api_exceptions.Aborted):
        return _create_spanner_error(error, DeadlockError, "transaction aborted")

    if isinstance(error, api_exceptions.Cancelled):
        return _create_spanner_error(error, OperationCancelledError, "operation cancelled")
    if isinstance(error, api_exceptions.DeadlineExceeded):
        return _create_spanner_error(error, QueryTimeoutError, "deadline exceeded")

    if isinstance(error, (api_exceptions.ServiceUnavailable, api_exceptions.TooManyRequests)):
        return _create_spanner_error(error, OperationalError, "service unavailable or rate limited")

    return _create_spanner_error(error, SQLSpecError, "error")


def build_session_driver_features(
    features: dict[str, Any],
    *,
    request_options: "RequestOptions | dict[str, Any] | None" = None,
    directed_read_options: "DirectedReadOptions | None" = None,
    query_options: "ExecuteSqlRequest.QueryOptions | dict[str, Any] | None" = None,
    retry: "Retry | None" = None,
    timeout: "float | None" = None,
) -> "dict[str, Any]":
    """Apply session overrides without changing the configuration's defaults.

    Args:
        features: Configuration driver features.
        request_options: Session-scoped RequestOptions.
        directed_read_options: Session-scoped DirectedReadOptions.
        query_options: Session-scoped QueryOptions.
        retry: Session-scoped retry policy.
        timeout: Session-scoped timeout in seconds.

    Returns:
        The configuration features when no override is given, otherwise a copy
        with the overrides applied.
    """
    if (
        request_options is None
        and directed_read_options is None
        and query_options is None
        and retry is None
        and timeout is None
    ):
        return features
    driver_features = dict(features)
    if request_options is not None:
        driver_features["request_options"] = request_options
    if directed_read_options is not None:
        driver_features["directed_read_options"] = directed_read_options
    if query_options is not None:
        driver_features["query_options"] = query_options
    if retry is not None:
        driver_features["retry"] = retry
    if timeout is not None:
        driver_features["timeout"] = timeout
    return driver_features


def is_query_statement(sql: str, dialect: str) -> bool:
    """Return True when a script statement returns rows.

    Args:
        sql: A single SQL statement.
        dialect: SQLGlot dialect used to parse the statement.

    Returns:
        True for queries; statements SQLGlot cannot parse are treated as
        queries when they start with ``SELECT``.
    """
    try:
        return isinstance(sqlglot.parse_one(sql, read=dialect), exp.Query)
    except SqlglotError:
        return sql.upper().strip().startswith("SELECT")


def resolve_transaction_completion(transaction: Any, *, failed: bool) -> "Literal['commit', 'rollback', 'skip']":
    """Decide how to finish a Spanner read-write transaction.

    A transaction that already committed or rolled back is skipped. After
    failure it rolls back, which discards buffered mutations and contacts the
    server only when the transaction has begun there. After success it
    commits when it has begun on the server or buffered mutations.

    Args:
        transaction: A sync or async Spanner ``Transaction``.
        failed: Whether the enclosing unit of work raised or asked to roll back.

    Returns:
        ``"commit"``, ``"rollback"``, or ``"skip"``.
    """
    if transaction.committed is not None or transaction.rolled_back:
        return "skip"
    if failed:
        return "rollback"
    if transaction._transaction_id is not None or transaction._mutations:
        return "commit"
    return "skip"


class _SpannerSyncTransactionWork:
    """Run a unit of work for the sync Spanner transaction runner."""

    __slots__ = ("_build_driver", "_fn")

    def __init__(self, build_driver: "Callable[[Any], Any]", fn: "Callable[..., Any]") -> None:
        self._build_driver = build_driver
        self._fn = fn

    def __call__(self, transaction: Any, *args: Any, **kwargs: Any) -> Any:
        driver = self._build_driver(transaction)
        try:
            return self._fn(driver, *args, **kwargs)
        except DeadlockError as exc:
            cause = exc.__cause__
            if isinstance(cause, api_exceptions.Aborted):
                raise cause from None
            raise


class _SpannerAsyncTransactionWork:
    """Run a unit of work for the async Spanner transaction runner."""

    __slots__ = ("_build_driver", "_fn")

    def __init__(self, build_driver: "Callable[[Any], Any]", fn: "Callable[..., Awaitable[Any]]") -> None:
        self._build_driver = build_driver
        self._fn = fn

    async def __call__(self, transaction: Any, *args: Any, **kwargs: Any) -> Any:
        driver = self._build_driver(transaction)
        try:
            return await self._fn(driver, *args, **kwargs)
        except DeadlockError as exc:
            cause = exc.__cause__
            if isinstance(cause, api_exceptions.Aborted):
                raise cause from None
            raise


def run_in_transaction_sync(
    database: Any, build_driver: "Callable[[Any], Any]", fn: "Callable[..., Any]", *args: Any, **kwargs: Any
) -> Any:
    """Run ``fn`` inside the sync Spanner retryable transaction runner.

    ``fn`` receives a driver bound to the runner's transaction. A
    ``DeadlockError`` caused by ``Aborted`` is unwrapped so the runner retries,
    and a terminal Google API error is mapped to a SQLSpec exception.

    Args:
        database: Sync Spanner database.
        build_driver: Factory that binds a driver to a transaction.
        fn: Unit of work called as ``fn(driver, *args, **kwargs)``.
        *args: Positional arguments for ``fn``.
        **kwargs: Keyword arguments for the runner and ``fn``.

    Returns:
        The value returned by ``fn``.

    Raises:
        SQLSpecError: The mapped error when the runner fails.
    """
    try:
        return database.run_in_transaction(_SpannerSyncTransactionWork(build_driver, fn), *args, **kwargs)
    except api_exceptions.GoogleAPICallError as exc:
        raise create_mapped_exception(exc) from exc


async def run_in_transaction_async(
    database: Any, build_driver: "Callable[[Any], Any]", fn: "Callable[..., Awaitable[Any]]", *args: Any, **kwargs: Any
) -> Any:
    """Run ``fn`` inside the async Spanner retryable transaction runner.

    ``fn`` receives a driver bound to the runner's transaction. A
    ``DeadlockError`` caused by ``Aborted`` is unwrapped so the runner retries,
    and a terminal Google API error is mapped to a SQLSpec exception.

    Args:
        database: Async Spanner database.
        build_driver: Factory that binds a driver to a transaction.
        fn: Coroutine function called as ``await fn(driver, *args, **kwargs)``.
        *args: Positional arguments for ``fn``.
        **kwargs: Keyword arguments for the runner and ``fn``.

    Returns:
        The value returned by ``fn``.

    Raises:
        SQLSpecError: The mapped error when the runner fails.
    """
    try:
        return await database.run_in_transaction(_SpannerAsyncTransactionWork(build_driver, fn), *args, **kwargs)
    except api_exceptions.GoogleAPICallError as exc:
        raise create_mapped_exception(exc) from exc


def list_existing_table_names_sync(database: Any) -> "set[str]":
    """Return the table names in a sync Spanner database's default schema.

    Args:
        database: Sync Spanner database.

    Returns:
        Table names.
    """
    return {table.table_id for table in database.list_tables()}


async def list_existing_table_names_async(database: Any) -> "set[str]":
    """Return the table names in an async Spanner database's default schema.

    Args:
        database: Async Spanner database.

    Returns:
        Table names.
    """
    names: set[str] = set()
    async for table in database.list_tables():
        names.add(table.table_id)
    return names


def execute_ddl_sync(database: Any, statements: "list[str]", *, timeout: "float | None" = None) -> None:
    """Apply DDL statements through a sync Spanner database and wait for completion.

    Args:
        database: Sync Spanner database.
        statements: DDL statements; nothing runs when empty.
        timeout: Seconds to wait for the schema operation, or None to wait indefinitely.
    """
    if not statements:
        return
    database.update_ddl(statements).result(timeout=timeout)


async def execute_ddl_async(database: Any, statements: "list[str]", *, timeout: "float | None" = None) -> None:
    """Apply DDL statements through an async Spanner database and wait for completion.

    Args:
        database: Async Spanner database.
        statements: DDL statements; nothing runs when empty.
        timeout: Seconds to wait for the schema operation, or None to wait indefinitely.
    """
    if not statements:
        return
    operation = await database.update_ddl(statements)
    await operation.result(timeout=timeout)


class _SpannerStreamSourceBase:
    """Row conversion shared by the sync and async Spanner stream sources."""

    __slots__ = (
        "_chunk_size",
        "_column_names",
        "_column_plan",
        "_driver",
        "_execute_kwargs",
        "_param_types",
        "_params",
        "_result_set",
        "_sql",
    )

    def __init__(
        self,
        driver: Any,
        sql: str,
        params: "dict[str, Any] | None",
        param_types: "dict[str, Any]",
        chunk_size: int,
        execute_kwargs: "dict[str, Any]",
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._params = params
        self._param_types = param_types
        self._chunk_size = chunk_size
        self._execute_kwargs = execute_kwargs
        self._column_names: list[str] | None = None
        self._column_plan: tuple[tuple[int, Any], ...] | None = None
        self._result_set: Any = None

    def _rows_to_chunk(self, rows: "list[Any]") -> "list[dict[str, Any]]":
        column_names = self._column_names
        if column_names is None:
            metadata = self._result_set.metadata
            fields = metadata.row_type.fields if metadata is not None else None
            if not fields:
                msg = "Result set metadata not available."
                raise SQLConversionError(msg)
            column_names, self._column_plan = self._driver._resolve_row_plan(fields)
        converted_rows, resolved_column_names = collect_rows(
            rows, (), column_names=column_names, column_plan=self._column_plan
        )
        self._column_names = resolved_column_names
        return rows_to_dicts(converted_rows, resolved_column_names)

    def _reset(self) -> None:
        self._result_set = None
        self._column_names = None
        self._column_plan = None


class SpannerSyncStreamSource(_SpannerStreamSourceBase):
    """Chunk source streaming dict rows from a sync Spanner query."""

    __slots__ = ("_row_iterator",)

    def __init__(
        self,
        driver: Any,
        sql: str,
        params: "dict[str, Any] | None",
        param_types: "dict[str, Any]",
        chunk_size: int,
        execute_kwargs: "dict[str, Any]",
    ) -> None:
        super().__init__(driver, sql, params, param_types, chunk_size, execute_kwargs)
        self._row_iterator: Generator[Any, None, None] | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            result_set = self._driver.connection.execute_sql(
                self._sql, params=self._params, param_types=self._param_types, **self._execute_kwargs
            )
            self._result_set = result_set
            self._row_iterator = cast("Generator[Any, None, None]", iter(result_set))
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        row_iterator = self._row_iterator
        if self._result_set is None or row_iterator is None:
            return []
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        with handler:
            rows = list(islice(row_iterator, self._chunk_size))
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        return self._rows_to_chunk(rows)

    def close(self, error: bool = False) -> None:
        row_iterator = self._row_iterator
        self._row_iterator = None
        if row_iterator is not None:
            row_iterator.close()
        self._reset()


class SpannerAsyncStreamSource(_SpannerStreamSourceBase):
    """Chunk source streaming dict rows from an async Spanner query."""

    __slots__ = ("_row_iterator",)

    def __init__(
        self,
        driver: Any,
        sql: str,
        params: "dict[str, Any] | None",
        param_types: "dict[str, Any]",
        chunk_size: int,
        execute_kwargs: "dict[str, Any]",
    ) -> None:
        super().__init__(driver, sql, params, param_types, chunk_size, execute_kwargs)
        self._row_iterator: AsyncGenerator[Any, None] | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        async with handler:
            result_set = await self._driver.connection.execute_sql(
                self._sql, params=self._params, param_types=self._param_types, **self._execute_kwargs
            )
            self._result_set = result_set
            self._row_iterator = cast("AsyncGenerator[Any, None]", result_set.__aiter__())
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        row_iterator = self._row_iterator
        if self._result_set is None or row_iterator is None:
            return []
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        async with handler:
            with contextlib.suppress(StopAsyncIteration):
                while len(rows) < self._chunk_size:
                    rows.append(await row_iterator.__anext__())
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        return self._rows_to_chunk(rows)

    async def close(self, error: bool = False) -> None:
        row_iterator = self._row_iterator
        self._row_iterator = None
        if row_iterator is not None:
            await row_iterator.aclose()
        self._reset()


def _convert_json_row_value(value: Any, *, json_deserializer: "Callable[[str], Any]") -> Any:
    """Convert a native Spanner JSON cell using the configured deserializer."""
    if isinstance(value, JsonObject):
        json_object = cast("Any", value)
        if json_object._is_null:
            return None
        if json_deserializer is from_json:
            if json_object._is_array:
                return list(json_object._array_value)
            if json_object._is_scalar_value:
                return json_object._simple_value
            return dict(value)
        json_value = json_object.serialize()
        if json_value is None:
            return None
    elif isinstance(value, str):
        json_value = value
    else:
        return value

    try:
        return json_deserializer(json_value)
    except (TypeError, ValueError):
        return value


def _create_spanner_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create a Spanner error instance without raising it."""
    msg = f"Spanner {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


driver_profile = build_profile()

default_statement_config = build_statement_config()
