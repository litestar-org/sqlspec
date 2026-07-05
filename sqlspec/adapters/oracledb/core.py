"""OracleDB adapter compiled helpers."""

import contextlib
import re
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, Protocol, cast

from sqlspec.adapters.oracledb._param_types import OracleBlob, OracleClob, OracleJson
from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StackResult,
    StatementConfig,
    build_statement_config_from_profile,
    create_sql_result,
)
from sqlspec.driver import rows_to_dicts
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount, is_readable

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.adapters.oracledb._typing import (
        OracleAsyncConnection,
        OracleAsyncRawCursor,
        OracleSyncConnection,
        OracleSyncRawCursor,
    )
    from sqlspec.core import SQL
    from sqlspec.driver._common import AsyncExceptionHandler, SyncExceptionHandler


class _OracleSyncStreamDriver(Protocol):
    connection: "OracleSyncConnection"

    def handle_database_exceptions(self) -> "SyncExceptionHandler": ...

    def _check_pending_exception(self, exc_handler: "SyncExceptionHandler") -> None: ...

    def _resolve_row_metadata(self, description: object) -> tuple[list[str], bool]: ...


class _OracleAsyncStreamDriver(Protocol):
    connection: "OracleAsyncConnection"

    def handle_database_exceptions(self) -> "AsyncExceptionHandler": ...

    def _check_pending_exception(self, exc_handler: "AsyncExceptionHandler") -> None: ...

    def _resolve_row_metadata(self, description: object) -> tuple[list[str], bool]: ...


__all__ = (
    "ORACLEDB_SUPPORTS_SPARSE_VECTORS",
    "SPARSE_VECTOR_MIN_DATABASE_MAJOR",
    "OracleAsyncStreamSource",
    "OracleSyncStreamSource",
    "apply_driver_features",
    "build_arrow_fetch_kwargs",
    "build_fetch_kwargs",
    "build_insert_statement",
    "build_pipeline_stack_result",
    "build_profile",
    "build_statement_config",
    "build_truncate_statement",
    "coerce_large_parameters_async",
    "coerce_large_parameters_sync",
    "collect_async_rows",
    "collect_sync_rows",
    "connection_is_thin",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "normalize_column_names",
    "normalize_execute_many_parameters_async",
    "normalize_execute_many_parameters_sync",
    "resolve_row_metadata",
    "resolve_rowcount",
    "supports_df_batches",
    "supports_direct_path_load",
)


IMPLICIT_UPPER_COLUMN_PATTERN: "re.Pattern[str]" = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
_VERSION_COMPONENTS: int = 3
TYPE_CONVERTER = OracleOutputConverter()
_LOB_TYPE_NAME_MARKERS: "tuple[str, ...]" = ("LOB", "BFILE")
_SCALAR_PASSTHROUGH_TYPES: "tuple[type[Any], ...]" = (bool, int, float, str, bytes, bytearray, type(None))
ROW_CACHE_MAX_SIZE: int = 256

# Oracle ORA error code ranges for category detection
ORA_CHECK_CONSTRAINT = 2290
ORA_INTEGRITY_RANGE_START = 2200
ORA_INTEGRITY_RANGE_END = 2300
ORA_PARSING_RANGE_START = 900
ORA_PARSING_RANGE_END = 1000
ORA_TABLESPACE_FULL = 1652

# Oracle error codes for specific exception mappings
_ERROR_CODE_MAPPING: "dict[int, tuple[type[SQLSpecError], str]]" = {
    # Integrity constraint violations
    1: (UniqueViolationError, "unique constraint violation"),
    2291: (ForeignKeyViolationError, "foreign key constraint violation"),
    2292: (ForeignKeyViolationError, "foreign key constraint violation"),
    ORA_CHECK_CONSTRAINT: (CheckViolationError, "check constraint violation"),
    1400: (NotNullViolationError, "not-null constraint violation"),
    1407: (NotNullViolationError, "not-null constraint violation"),
    # Permission/access errors
    1017: (PermissionDeniedError, "invalid username/password"),
    1031: (PermissionDeniedError, "insufficient privileges"),
    942: (PermissionDeniedError, "table or view does not exist"),
    # Connection errors
    12154: (DatabaseConnectionError, "TNS resolution failed"),
    12541: (ConnectionTimeoutError, "no listener"),
    12545: (ConnectionTimeoutError, "connect failed"),
    12514: (DatabaseConnectionError, "service not known"),
    12505: (DatabaseConnectionError, "listener rejected"),
    12170: (ConnectionTimeoutError, "connect timeout"),
    # Transaction errors
    60: (DeadlockError, "deadlock detected"),
    8176: (TransactionError, "consistent read failure"),
    # Query timeout/cancellation
    1013: (QueryTimeoutError, "user requested cancel"),
    # Data errors
    1722: (DataError, "invalid number"),
    1858: (DataError, "invalid character"),
    1840: (DataError, "data conversion error"),
    # Operational errors
    ORA_TABLESPACE_FULL: (OperationalError, "tablespace full"),
}


def _parse_version_tuple(version: str) -> "tuple[int, int, int]":
    parts = [int(part) for part in version.split(".") if part.isdigit()]
    while len(parts) < _VERSION_COMPONENTS:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def _resolve_oracledb_version() -> "tuple[int, int, int]":
    try:
        import oracledb
    except ImportError:
        return (0, 0, 0)
    try:
        version = oracledb.__version__
    except AttributeError:
        version = "0.0.0"
    return _parse_version_tuple(version)


ORACLEDB_VERSION: "tuple[int, int, int]" = _resolve_oracledb_version()
SPARSE_VECTOR_MIN_DATABASE_MAJOR: int = 23


def _resolve_sparse_vector_support() -> bool:
    """Return whether the installed python-oracledb exports SparseVector."""
    try:
        import oracledb
    except ImportError:
        return False
    return getattr(oracledb, "SparseVector", None) is not None


ORACLEDB_SUPPORTS_SPARSE_VECTORS: bool = _resolve_sparse_vector_support()


def connection_is_thin(connection: object) -> bool:
    """Return whether an Oracle connection is in Thin mode."""
    thin = getattr(connection, "thin", None)
    if thin is None:
        return True
    return bool(thin)


def supports_direct_path_load(connection: object) -> bool:
    """Return whether a connection exposes Thin-mode direct path load."""
    return connection_is_thin(connection) and hasattr(connection, "direct_path_load")


def supports_df_batches(connection: object) -> bool:
    """Return whether a connection exposes DataFrame batch fetches."""
    return hasattr(connection, "fetch_df_batches")


def build_fetch_kwargs(driver_features: "dict[str, Any]") -> "dict[str, object]":
    """Build per-statement Oracle fetch keyword arguments."""
    fetch_kwargs: dict[str, object] = {}
    fetch_lobs = driver_features.get("fetch_lobs")
    if fetch_lobs is not None:
        fetch_kwargs["fetch_lobs"] = fetch_lobs
    fetch_decimals = driver_features.get("fetch_decimals")
    if fetch_decimals is not None:
        fetch_kwargs["fetch_decimals"] = fetch_decimals
    return fetch_kwargs


def build_arrow_fetch_kwargs(driver_features: "dict[str, Any]") -> "dict[str, object]":
    """Build Oracle Arrow/DataFrame fetch keyword arguments."""
    fetch_kwargs: dict[str, object] = {}
    fetch_decimals = driver_features.get("fetch_decimals")
    if fetch_decimals is not None:
        fetch_kwargs["fetch_decimals"] = fetch_decimals
    return fetch_kwargs


def normalize_column_names(column_names: "list[str]", driver_features: "dict[str, Any]") -> "list[str]":
    should_lowercase = driver_features.get("enable_lowercase_column_names", False)
    if not should_lowercase:
        return column_names
    normalized: list[str] = []
    for name in column_names:
        if name and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name):
            normalized.append(name.lower())
        else:
            normalized.append(name)
    return normalized


def normalize_execute_many_parameters_sync(parameters: Any) -> Any:
    """Normalize parameters for Oracle executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.

    Raises:
        ValueError: When parameters are missing for executemany.
    """
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    if isinstance(parameters, tuple):
        return list(parameters)
    return parameters


def normalize_execute_many_parameters_async(parameters: Any) -> Any:
    """Normalize parameters for Oracle async executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.

    Raises:
        ValueError: When parameters are missing for executemany.
    """
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    if isinstance(parameters, tuple):
        return list(parameters)
    return parameters


def _coerce_value_sync(
    connection: Any, value: Any, *, clob_type: Any, blob_type: Any, varchar2_byte_limit: int, raw_byte_limit: int
) -> Any:
    """Route a single parameter value through wrapper-aware coercion (sync)."""
    if isinstance(value, OracleClob):
        inner = value.value
        if isinstance(inner, bytes):
            inner = inner.decode("utf-8")
        return connection.createlob(clob_type, inner)
    if isinstance(value, OracleBlob):
        inner = value.value
        if isinstance(inner, str):
            inner = inner.encode("utf-8")
        return connection.createlob(blob_type, inner)
    if isinstance(value, OracleJson):
        return value.value
    if isinstance(value, str) and len(value.encode("utf-8")) > varchar2_byte_limit:
        return connection.createlob(clob_type, value)
    if isinstance(value, (bytes, bytearray)) and len(value) > raw_byte_limit:
        return connection.createlob(blob_type, bytes(value))
    return value


async def _coerce_value_async(
    connection: Any, value: Any, *, clob_type: Any, blob_type: Any, varchar2_byte_limit: int, raw_byte_limit: int
) -> Any:
    """Async mirror of :func:`_coerce_value_sync`."""
    if isinstance(value, OracleClob):
        inner = value.value
        if isinstance(inner, bytes):
            inner = inner.decode("utf-8")
        return await connection.createlob(clob_type, inner)
    if isinstance(value, OracleBlob):
        inner = value.value
        if isinstance(inner, str):
            inner = inner.encode("utf-8")
        return await connection.createlob(blob_type, inner)
    if isinstance(value, OracleJson):
        return value.value
    if isinstance(value, str) and len(value.encode("utf-8")) > varchar2_byte_limit:
        return await connection.createlob(clob_type, value)
    if isinstance(value, (bytes, bytearray)) and len(value) > raw_byte_limit:
        return await connection.createlob(blob_type, bytes(value))
    return value


def coerce_large_parameters_sync(
    connection: Any, parameters: Any, *, clob_type: Any, blob_type: Any, varchar2_byte_limit: int, raw_byte_limit: int
) -> Any:
    """Coerce large string/bytes parameters into CLOBs/BLOBs with wrapper-aware routing.

    Routing order:
        1. ``OracleClob`` / ``OracleBlob`` / ``OracleJson`` wrappers — explicit user
            intent, length-thresholds bypassed.
        2. Plain ``str`` whose UTF-8 encoding exceeds ``varchar2_byte_limit`` → CLOB.
        3. Plain ``bytes``/``bytearray`` longer than ``raw_byte_limit`` → BLOB.

    Handles ``dict`` (named binds), ``tuple`` and ``list`` (positional binds).
    Tuples are returned as new lists when any value is rewritten so the driver's
    ``cast(... | tuple | list | dict)`` contract is preserved.

    Args:
        connection: Oracle database connection.
        parameters: Prepared parameters payload.
        clob_type: Oracle CLOB DB type (``oracledb.DB_TYPE_CLOB``).
        blob_type: Oracle BLOB DB type (``oracledb.DB_TYPE_BLOB``).
        varchar2_byte_limit: Byte-length threshold for implicit CLOB conversion.
        raw_byte_limit: Byte-length threshold for implicit BLOB conversion.

    Returns:
        Parameters payload with values routed to the correct LOB / native type.
    """
    if not parameters:
        return parameters
    if isinstance(parameters, dict):
        for param_name, param_value in parameters.items():
            parameters[param_name] = _coerce_value_sync(
                connection,
                param_value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
            )
        return parameters
    if isinstance(parameters, (list, tuple)):
        return [
            _coerce_value_sync(
                connection,
                value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
            )
            for value in parameters
        ]
    return parameters


async def coerce_large_parameters_async(
    connection: Any, parameters: Any, *, clob_type: Any, blob_type: Any, varchar2_byte_limit: int, raw_byte_limit: int
) -> Any:
    """Async mirror of :func:`coerce_large_parameters_sync`.

    Identical routing order; ``connection.createlob`` is awaited for the async
    Oracle driver.
    """
    if not parameters:
        return parameters
    if isinstance(parameters, dict):
        for param_name, param_value in parameters.items():
            parameters[param_name] = await _coerce_value_async(
                connection,
                param_value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
            )
        return parameters
    if isinstance(parameters, (list, tuple)):
        return [
            await _coerce_value_async(
                connection,
                value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
            )
            for value in parameters
        ]
    return parameters


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def build_truncate_statement(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def build_pipeline_stack_result(
    statement: "SQL",
    method: str,
    returns_rows: bool,
    parameters: Any,
    pipeline_result: Any,
    driver_features: "dict[str, Any]",
) -> "StackResult":
    """Build StackResult from Oracle pipeline output.

    Args:
        statement: Statement executed in the pipeline.
        method: Pipeline execution method name.
        returns_rows: Whether the operation returns rows.
        parameters: Prepared parameters used for execution.
        pipeline_result: Raw pipeline execution result.
        driver_features: Driver feature configuration for normalization.

    Returns:
        StackResult for the pipeline operation.
    """
    try:
        rows = pipeline_result.rows
    except AttributeError:
        rows = None
    try:
        columns = pipeline_result.columns
    except AttributeError:
        columns = None

    data: list[dict[str, Any]] | None = None
    if returns_rows:
        if not rows:
            data = []
        else:
            if columns:
                names = []
                for index, column in enumerate(columns):
                    try:
                        name = column.name
                    except AttributeError:
                        name = f"column_{index}"
                    names.append(name)
            else:
                first = rows[0]
                names = [f"column_{index}" for index in range(len(first) if isinstance(first, Sized) else 0)]
            names = normalize_column_names(names, driver_features)
            normalized_rows: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    normalized_rows.append(row)
                else:
                    normalized_rows.append(dict(zip(names, row, strict=False)))
            data = normalized_rows

    metadata: dict[str, Any] = {"pipeline_operation": method}
    try:
        warning = pipeline_result.warning
    except AttributeError:
        warning = None
    if warning is not None:
        metadata["warning"] = warning

    try:
        return_value = pipeline_result.return_value
    except AttributeError:
        return_value = None
    if return_value is not None:
        metadata["return_value"] = return_value

    try:
        rowcount = pipeline_result.rowcount
    except AttributeError:
        rowcount = None

    if isinstance(rowcount, int) and rowcount >= 0:
        rows_affected = rowcount
    elif method == "execute_many":
        try:
            rows_affected = len(parameters or ())
        except TypeError:
            rows_affected = 0
    elif method == "execute" and not returns_rows:
        rows_affected = 1
    elif returns_rows:
        rows_affected = len(data or [])
    else:
        rows_affected = 0

    sql_result = create_sql_result(statement, data=data, rows_affected=rows_affected, metadata=metadata)
    return StackResult.from_sql_result(sql_result)


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an Oracle cursor.

    Args:
        cursor: Oracle cursor with optional rowcount metadata.

    Returns:
        Rowcount value or 0 when unavailable.
    """
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int):
        return rowcount
    return 0


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply OracleDB driver feature defaults.

    Defaults ``fetch_lobs`` to ``False`` so direct LOB reads under the 1 GB
    fetch ceiling return text/bytes by default. Locators remain available for
    OUT binds, PL/SQL, and explicit ``fetch_lobs=True``.
    """
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
    features.setdefault("enable_lowercase_column_names", True)
    features.setdefault("enable_uuid_binary", True)
    features.setdefault("fetch_lobs", False)
    if "vector_return_format" not in features:
        if not NUMPY_INSTALLED:
            features["vector_return_format"] = "list"
        elif features["enable_numpy_vectors"]:
            features["vector_return_format"] = "numpy"
        else:
            features["vector_return_format"] = "array"
    features.setdefault("oracle_varchar2_byte_limit", 4000)
    features.setdefault("oracle_raw_byte_limit", 2000)
    return statement_config, features


def _description_requires_lob_coercion(description: "list[Any]") -> bool:
    """Return True when cursor metadata indicates LOB-compatible columns."""
    # Keep in sync with resolve_row_metadata's inlined cache-miss LOB detection.
    for column in description:
        try:
            type_code = column[1]
        except (TypeError, IndexError, KeyError):
            type_code = getattr(column, "type_code", None)
            if type_code is None:
                # Unknown metadata shape: keep conservative behavior.
                return True

        type_name = getattr(type_code, "name", None)
        if isinstance(type_name, str):
            upper_name = type_name.upper()
            if any(marker in upper_name for marker in _LOB_TYPE_NAME_MARKERS):
                return True
            continue

        type_text = str(type_code).upper()
        if any(marker in type_text for marker in _LOB_TYPE_NAME_MARKERS):
            return True
    return False


def resolve_row_metadata(
    description: "list[Any] | None", driver_features: "dict[str, Any]", cache: "dict[int, tuple[Any, list[str], bool]]"
) -> "tuple[list[str], bool]":
    """Resolve and cache Oracle row metadata for hot row materialization paths.

    Args:
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        cache: Driver-local metadata cache keyed by ``id(description)``.

    Returns:
        Tuple of (normalized column names, requires_lob_coercion).
    """
    if not description:
        return [], False

    cache_key = id(description)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1], cached[2]

    column_names: list[str] = []
    requires_lob_coercion = False
    for column in description:
        try:
            column_names.append(column[0])
            type_code = column[1]
        except (TypeError, IndexError, KeyError):
            column_name = getattr(column, "name", None)
            if isinstance(column_name, str):
                column_names.append(column_name)
            type_code = getattr(column, "type_code", None)
            if type_code is None:
                requires_lob_coercion = True
                continue

        if requires_lob_coercion:
            continue

        type_name = getattr(type_code, "name", None)
        if isinstance(type_name, str):
            upper_name = type_name.upper()
            if any(marker in upper_name for marker in _LOB_TYPE_NAME_MARKERS):
                requires_lob_coercion = True
            continue

        type_text = str(type_code).upper()
        if any(marker in type_text for marker in _LOB_TYPE_NAME_MARKERS):
            requires_lob_coercion = True

    normalized_column_names = normalize_column_names(column_names, driver_features)

    if len(cache) >= ROW_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (description, normalized_column_names, requires_lob_coercion)
    return normalized_column_names, requires_lob_coercion


class OracleSyncStreamSource:
    """Compiled chunk source streaming dict rows from an oracledb cursor via ``fetchmany``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor", "_driver", "_parameters", "_sql")

    def __init__(
        self,
        driver: "_OracleSyncStreamDriver",
        sql: str,
        parameters: "list[object] | tuple[object, ...] | dict[object, object] | None",
        chunk_size: int,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: OracleSyncRawCursor | None = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            cursor = self._driver.connection.cursor()
            cursor.arraysize = self._chunk_size
            cursor.prefetchrows = self._chunk_size
            cursor.execute(self._sql, self._parameters or {})
            self._cursor = cursor
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        cursor = self._cursor
        if cursor is None:
            return []
        rows: list[Any] = []
        with handler:
            rows = cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        column_names = self._column_names
        if column_names is None:
            column_names, _ = self._driver._resolve_row_metadata(cursor.description)
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    def close(self) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()


class OracleAsyncStreamSource:
    """Compiled async chunk source streaming dict rows from an oracledb cursor via ``fetchmany``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor", "_driver", "_parameters", "_sql")

    def __init__(
        self,
        driver: "_OracleAsyncStreamDriver",
        sql: str,
        parameters: "list[object] | tuple[object, ...] | dict[object, object] | None",
        chunk_size: int,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: OracleAsyncRawCursor | None = None
        self._column_names: list[str] | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        async with handler:
            cursor = self._driver.connection.cursor()
            cursor.arraysize = self._chunk_size
            cursor.prefetchrows = self._chunk_size
            await cursor.execute(self._sql, self._parameters or {})
            self._cursor = cursor
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        cursor = self._cursor
        if cursor is None:
            return []
        rows: list[Any] = []
        async with handler:
            rows = await cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        column_names = self._column_names
        if column_names is None:
            column_names, _ = self._driver._resolve_row_metadata(cursor.description)
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    async def close(self) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()


def _row_requires_lob_coercion(row: "tuple[Any, ...]") -> bool:
    """Return True when a row contains readable values that need LOB coercion."""
    for value in row:
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            continue
        if is_readable(value):
            return True
    return False


def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "tuple[Any, ...]":
    """Coerce LOB handles to concrete values for synchronous execution.

    Processes each value in the row, reading locator objects returned for
    OUT binds, PL/SQL, or explicit ``fetch_lobs=True``. Direct-fetched
    ``str``/``bytes`` values from the default path pass through unchanged.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        Tuple of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] | None = None
    for index, value in enumerate(row):
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            if coerced_values is not None:
                coerced_values.append(value)
            continue

        if is_readable(value):
            try:
                processed_value = value.read()
            except Exception:
                if coerced_values is not None:
                    coerced_values.append(value)
                continue

            if coerced_values is None:
                coerced_values = list(row[:index])
            coerced_values.append(processed_value)
            continue

        if coerced_values is not None:
            coerced_values.append(value)

    if coerced_values is None:
        return row
    return tuple(coerced_values)


async def _coerce_async_row_values(row: "tuple[Any, ...]") -> "tuple[Any, ...]":
    """Coerce LOB handles to concrete values for asynchronous execution.

    Processes each value in the row, reading locator objects returned for
    OUT binds, PL/SQL, or explicit ``fetch_lobs=True``. Direct-fetched
    ``str``/``bytes`` values from the default path pass through unchanged.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        Tuple of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] | None = None
    for index, value in enumerate(row):
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            if coerced_values is not None:
                coerced_values.append(value)
            continue

        if is_readable(value):
            try:
                processed_value = await TYPE_CONVERTER.process_lob(value)
            except Exception:
                if coerced_values is not None:
                    coerced_values.append(value)
                continue

            if coerced_values is None:
                if processed_value is value:
                    continue
                coerced_values = list(row[:index])
            coerced_values.append(processed_value)
            continue

        if coerced_values is not None:
            coerced_values.append(value)

    if coerced_values is None:
        return row
    return tuple(coerced_values)


def collect_sync_rows(
    fetched_data: "list[Any] | None",
    description: "list[Any] | None",
    driver_features: "dict[str, Any]",
    *,
    column_names: "list[str] | None" = None,
    requires_lob_coercion: "bool | None" = None,
) -> "tuple[list[tuple[Any, ...]], list[str]]":
    """Collect OracleDB sync rows as tuples with normalized column names.

    LOB locators are still read for OUT binds, PL/SQL, and explicit
    ``fetch_lobs=True``. Direct-fetched LOB values under the 1 GB ceiling
    arrive as plain strings/bytes and pass through unchanged. The raw coerced
    tuples are returned instead of dicts so that ``SQLResult`` can handle lazy
    dict materialization based on ``row_format``.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        column_names: Optional precomputed normalized column names.
        requires_lob_coercion: Optional precomputed LOB-coercion flag.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    resolved_column_names = (
        normalize_column_names([col[0] for col in description], driver_features)
        if column_names is None
        else column_names
    )
    if not fetched_data:
        return [], resolved_column_names

    if requires_lob_coercion is None:
        requires_lob_coercion = _description_requires_lob_coercion(description)
    if not requires_lob_coercion:
        first_row = fetched_data[0]
        first_row_tuple = first_row if isinstance(first_row, tuple) else tuple(first_row)
        if not _row_requires_lob_coercion(first_row_tuple):
            return cast("list[tuple[Any, ...]]", fetched_data), resolved_column_names

    data: list[tuple[Any, ...]] = []
    for row in fetched_data:
        row_tuple = row if isinstance(row, tuple) else tuple(row)
        data.append(_coerce_sync_row_values(row_tuple))
    return data, resolved_column_names


async def collect_async_rows(
    fetched_data: "list[Any] | None",
    description: "list[Any] | None",
    driver_features: "dict[str, Any]",
    *,
    column_names: "list[str] | None" = None,
    requires_lob_coercion: "bool | None" = None,
) -> "tuple[list[tuple[Any, ...]], list[str]]":
    """Collect OracleDB async rows as tuples with normalized column names.

    LOB locators are still read for OUT binds, PL/SQL, and explicit
    ``fetch_lobs=True``. Direct-fetched LOB values under the 1 GB ceiling
    arrive as plain strings/bytes and pass through unchanged. The raw coerced
    tuples are returned instead of dicts so that ``SQLResult`` can handle lazy
    dict materialization based on ``row_format``.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        column_names: Optional precomputed normalized column names.
        requires_lob_coercion: Optional precomputed LOB-coercion flag.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    resolved_column_names = (
        normalize_column_names([col[0] for col in description], driver_features)
        if column_names is None
        else column_names
    )
    if not fetched_data:
        return [], resolved_column_names

    if requires_lob_coercion is None:
        requires_lob_coercion = _description_requires_lob_coercion(description)
    if not requires_lob_coercion:
        first_row = fetched_data[0]
        first_row_tuple = first_row if isinstance(first_row, tuple) else tuple(first_row)
        if not _row_requires_lob_coercion(first_row_tuple):
            return cast("list[tuple[Any, ...]]", fetched_data), resolved_column_names

    data: list[tuple[Any, ...]] = []
    for row in fetched_data:
        row_tuple = row if isinstance(row, tuple) else tuple(row)
        data.append(await _coerce_async_row_values(row_tuple))
    return data, resolved_column_names


def _create_oracle_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from an Oracle error.

    Args:
        error: The original Oracle exception
        code: Oracle error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"Oracle {description} [ORA-{code:05d}]: {error}" if code else f"Oracle {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map Oracle exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The Oracle exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    error_obj = error.args[0] if getattr(error, "args", None) else None
    if not error_obj:
        return _create_oracle_error(error, None, SQLSpecError, "database error")

    try:
        error_code = error_obj.code
    except AttributeError:
        error_code = None
    if not error_code:
        return _create_oracle_error(error, None, SQLSpecError, "database error")

    mapping = _ERROR_CODE_MAPPING.get(error_code)
    if mapping:
        error_class, error_desc = mapping
        return _create_oracle_error(error, error_code, error_class, error_desc)

    if ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
        return _create_oracle_error(error, error_code, IntegrityError, "integrity constraint violation")

    if ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
        return _create_oracle_error(error, error_code, SQLParsingError, "SQL syntax error")

    return _create_oracle_error(error, error_code, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
    """Create the OracleDB driver parameter profile."""
    return DriverParameterProfile(
        name="OracleDB",
        default_style=ParameterStyle.NAMED_COLON,
        supported_styles={ParameterStyle.NAMED_COLON, ParameterStyle.NUMERIC, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NAMED_COLON,
        supported_execution_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions={**build_uuid_coercions(native=True)},
        default_dialect="oracle",
    )


driver_profile = build_profile()


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the OracleDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "oracle"}, json_serializer=serializer
    )


default_statement_config = build_statement_config()
