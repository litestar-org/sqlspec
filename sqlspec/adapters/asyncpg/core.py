"""AsyncPG adapter compiled helpers."""

import datetime
import importlib
import re
from typing import TYPE_CHECKING, Any, Final, NamedTuple

import asyncpg

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL, ParameterStyleConfig, StackOperation

__all__ = (
    "NormalizedStackOperation",
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "configure_parameter_serializers",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "invoke_prepared_statement",
    "parse_status",
    "register_json_codecs",
    "register_pgvector_support",
)

ASYNC_PG_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
EXPECTED_REGEX_GROUPS = 3

logger = get_logger("sqlspec.adapters.asyncpg.core")
_PGVECTOR_MISSING_LOGGED = False


class NormalizedStackOperation(NamedTuple):
    """Normalized execution metadata used for prepared stack operations."""

    operation: "StackOperation"
    statement: "SQL"
    sql: str
    parameters: "tuple[Any, ...] | dict[str, Any] | None"


PREPARED_STATEMENT_CACHE_SIZE: Final[int] = 32


def _convert_datetime_param(value: Any) -> Any:
    """Convert datetime parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    return value


def _convert_date_param(value: Any) -> Any:
    """Convert date parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value


def _convert_time_param(value: Any) -> Any:
    """Convert time parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.time.fromisoformat(value)
    return value


def _build_asyncpg_custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for AsyncPG."""

    return {
        datetime.datetime: _convert_datetime_param,
        datetime.date: _convert_date_param,
        datetime.time: _convert_time_param,
    }


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build connection configuration with non-null values only.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    return {key: value for key, value in connection_config.items() if value is not None}


def build_profile() -> "DriverParameterProfile":
    """Create the AsyncPG driver parameter profile."""

    return DriverParameterProfile(
        name="AsyncPG",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_style=ParameterStyle.NUMERIC,
        supported_execution_styles={ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions=_build_asyncpg_custom_type_coercions(),
        default_dialect="postgres",
    )


driver_profile = build_profile()


def configure_parameter_serializers(
    parameter_config: "ParameterStyleConfig",
    serializer: "Callable[[Any], str]",
    *,
    deserializer: "Callable[[str], Any] | None" = None,
) -> "ParameterStyleConfig":
    """Return a parameter configuration updated with AsyncPG JSON codecs."""

    effective_deserializer = deserializer or parameter_config.json_deserializer or from_json
    return parameter_config.replace(json_serializer=serializer, json_deserializer=effective_deserializer)


async def invoke_prepared_statement(
    prepared: Any, parameters: "tuple[Any, ...] | dict[str, Any] | list[Any] | None", *, fetch: bool
) -> Any:
    """Invoke an AsyncPG prepared statement with optional parameters.

    Args:
        prepared: AsyncPG prepared statement object.
        parameters: Prepared parameters payload.
        fetch: Whether to fetch rows.

    Returns:
        Query result or status message.
    """
    if parameters is None:
        if fetch:
            return await prepared.fetch()
        await prepared.fetch()
        return prepared.get_statusmsg()

    if isinstance(parameters, dict):
        if fetch:
            return await prepared.fetch(**parameters)
        await prepared.fetch(**parameters)
        return prepared.get_statusmsg()

    if fetch:
        return await prepared.fetch(*parameters)
    await prepared.fetch(*parameters)
    return prepared.get_statusmsg()


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncPG statement configuration with optional JSON codecs."""

    effective_serializer = json_serializer or to_json
    effective_deserializer = json_deserializer or from_json

    profile = driver_profile
    base_config = build_statement_config_from_profile(
        profile,
        statement_overrides={"dialect": "postgres"},
        json_serializer=effective_serializer,
        json_deserializer=effective_deserializer,
    )

    parameter_config = configure_parameter_serializers(
        base_config.parameter_config, effective_serializer, deserializer=effective_deserializer
    )

    return base_config.replace(parameter_config=parameter_config)


default_statement_config = build_statement_config()


async def register_json_codecs(connection: Any, encoder: Any, decoder: Any) -> None:
    """Register JSON type codecs on asyncpg connection."""
    try:
        await connection.set_type_codec("json", encoder=encoder, decoder=decoder, schema="pg_catalog")
        await connection.set_type_codec("jsonb", encoder=encoder, decoder=decoder, schema="pg_catalog")
    except Exception:
        logger.exception("Failed to register JSON type codecs")


async def register_pgvector_support(connection: Any) -> None:
    """Register pgvector extension support on asyncpg connection."""
    if not PGVECTOR_INSTALLED:
        global _PGVECTOR_MISSING_LOGGED
        if not _PGVECTOR_MISSING_LOGGED:
            logger.debug("pgvector not installed - skipping vector type support")
            _PGVECTOR_MISSING_LOGGED = True
        return

    try:
        pgvector_asyncpg = importlib.import_module("pgvector.asyncpg")
        await pgvector_asyncpg.register_vector(connection)
    except Exception:
        logger.exception("Failed to register pgvector support")


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AsyncPG driver feature defaults to statement config."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}

    serializer = processed_features.setdefault("json_serializer", to_json)
    deserializer = processed_features.setdefault("json_deserializer", from_json)
    processed_features.setdefault("enable_json_codecs", True)
    processed_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    processed_features.setdefault("enable_cloud_sql", False)
    processed_features.setdefault("enable_alloydb", False)

    parameter_config = configure_parameter_serializers(
        statement_config.parameter_config, serializer, deserializer=deserializer
    )
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_features


def parse_status(status: Any) -> int:
    """Parse AsyncPG status string to extract row count.

    AsyncPG returns status strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
    for non-SELECT operations. This method extracts the affected row count.

    Args:
        status: Status string from AsyncPG operation.

    Returns:
        Number of affected rows, or 0 if cannot parse.
    """
    if not status or not isinstance(status, str):
        return 0

    match = ASYNC_PG_STATUS_REGEX.match(status.strip())
    if match:
        groups = match.groups()
        if len(groups) >= EXPECTED_REGEX_GROUPS:
            try:
                return int(groups[-1])
            except (ValueError, IndexError):
                pass

    return 0


def _create_postgres_error(
    error: Any, code: "str | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from an asyncpg error.

    Args:
        error: The original asyncpg exception
        code: PostgreSQL SQLSTATE error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"PostgreSQL {description} [{code}]: {error}" if code else f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map asyncpg exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __aexit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The asyncpg exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    # Check specific exception types first
    if isinstance(error, asyncpg.exceptions.UniqueViolationError):
        return _create_postgres_error(error, "23505", UniqueViolationError, "unique constraint violation")
    if isinstance(error, asyncpg.exceptions.ForeignKeyViolationError):
        return _create_postgres_error(error, "23503", ForeignKeyViolationError, "foreign key constraint violation")
    if isinstance(error, asyncpg.exceptions.NotNullViolationError):
        return _create_postgres_error(error, "23502", NotNullViolationError, "not-null constraint violation")
    if isinstance(error, asyncpg.exceptions.CheckViolationError):
        return _create_postgres_error(error, "23514", CheckViolationError, "check constraint violation")
    if isinstance(error, asyncpg.exceptions.PostgresSyntaxError):
        return _create_postgres_error(error, "42601", SQLParsingError, "SQL syntax error")

    # Fall back to SQLSTATE code mapping
    error_code = error.sqlstate if has_sqlstate(error) and error.sqlstate is not None else None
    if not error_code:
        return _create_postgres_error(error, None, SQLSpecError, "database error")

    if error_code == "23505":
        return _create_postgres_error(error, error_code, UniqueViolationError, "unique constraint violation")
    if error_code == "23503":
        return _create_postgres_error(error, error_code, ForeignKeyViolationError, "foreign key constraint violation")
    if error_code == "23502":
        return _create_postgres_error(error, error_code, NotNullViolationError, "not-null constraint violation")
    if error_code == "23514":
        return _create_postgres_error(error, error_code, CheckViolationError, "check constraint violation")
    if error_code.startswith("23"):
        return _create_postgres_error(error, error_code, IntegrityError, "integrity constraint violation")
    if error_code.startswith("42"):
        return _create_postgres_error(error, error_code, SQLParsingError, "SQL syntax error")
    if error_code.startswith("08"):
        return _create_postgres_error(error, error_code, DatabaseConnectionError, "connection error")
    if error_code.startswith("40"):
        return _create_postgres_error(error, error_code, TransactionError, "transaction error")
    if error_code.startswith("22"):
        return _create_postgres_error(error, error_code, DataError, "data error")
    if error_code.startswith(("53", "54", "55", "57", "58")):
        return _create_postgres_error(error, error_code, OperationalError, "operational error")
    return _create_postgres_error(error, error_code, SQLSpecError, "database error")


def collect_rows(records: "list[Any] | None") -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect AsyncPG records into dictionaries and column names.

    Args:
        records: Records returned from asyncpg fetch.

    Returns:
        Tuple of (rows, column_names).
    """
    if not records:
        return [], []
    rows = [dict(record) for record in records]
    column_names = list(records[0].keys())
    return rows, column_names
