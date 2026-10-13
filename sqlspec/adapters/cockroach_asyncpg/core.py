"""CockroachDB AsyncPG adapter helpers."""

import re
import secrets
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr
from sqlglot import tokenize
from sqlglot.tokenizer_core import TokenType

from sqlspec.adapters.asyncpg.core import build_connection_config as asyncpg_build_connection_config
from sqlspec.adapters.asyncpg.core import configure_parameter_serializers
from sqlspec.exceptions import ImproperConfigurationError, SerializationConflictError, SQLSpecError
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.storage import StorageTelemetry

__all__ = (
    "CockroachAsyncpgRetryConfig",
    "apply_driver_features",
    "build_connection_config",
    "build_native_export",
    "build_native_import",
    "calculate_backoff_seconds",
    "is_retryable_error",
    "native_export_telemetry",
    "native_import_telemetry",
    "normalize_native_export_query",
    "validate_follower_read_staleness",
)

_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True
_MAX_CAUSE_CHAIN_DEPTH: Final[int] = 16
_SYSTEM_RANDOM: Final[secrets.SystemRandom] = secrets.SystemRandom()
_CLUSTER_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9._-]+")


@mypyc_attr(allow_interpreted_subclasses=False)
class CockroachAsyncpgRetryConfig:
    """CockroachDB asyncpg transaction retry configuration."""

    __slots__ = ("base_delay_ms", "enable_logging", "max_delay_ms", "max_retries")

    def __init__(
        self,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        base_delay_ms: float = _DEFAULT_BASE_DELAY_MS,
        max_delay_ms: float = _DEFAULT_MAX_DELAY_MS,
        enable_logging: bool = _DEFAULT_ENABLE_LOGGING,
    ) -> None:
        if max_retries < 0:
            msg = "max_retries must be non-negative"
            raise ImproperConfigurationError(msg)
        if base_delay_ms < 0:
            msg = "base_delay_ms must be non-negative"
            raise ImproperConfigurationError(msg)
        if max_delay_ms < 0:
            msg = "max_delay_ms must be non-negative"
            raise ImproperConfigurationError(msg)
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.enable_logging = enable_logging

    @classmethod
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachAsyncpgRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", _DEFAULT_MAX_RETRIES)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", _DEFAULT_BASE_DELAY_MS)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", _DEFAULT_MAX_DELAY_MS)),
            enable_logging=bool(driver_features.get("enable_retry_logging", _DEFAULT_ENABLE_LOGGING)),
        )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply CockroachDB AsyncPG driver feature defaults to statement config."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = processed_features.setdefault("json_serializer", to_json)
    deserializer = processed_features.setdefault("json_deserializer", from_json)
    processed_features.setdefault("enable_json_codecs", True)
    processed_features["enable_pgvector"] = bool(driver_features and driver_features.get("enable_pgvector") is True)
    processed_features.setdefault("enable_events", True)
    processed_features.setdefault("events_backend", "poll_queue")
    parameter_config = configure_parameter_serializers(
        statement_config.parameter_config, serializer, deserializer=deserializer
    )
    statement_config = statement_config.replace(parameter_config=parameter_config)
    return statement_config, processed_features


def build_connection_config(config: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Prepare CockroachDB AsyncPG connection config, extracting multi-region server settings."""
    normalized = normalize_connection_config(config)
    extra = normalized.pop("extra", None)
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            if value is not None:
                normalized.setdefault(key, value)
    result = asyncpg_build_connection_config(normalized)
    server_settings = dict(result.get("server_settings") or {})
    if "application_name" in result:
        server_settings.setdefault("application_name", str(result.pop("application_name")))
    if "cluster" in result:
        cluster = result.pop("cluster")
        if cluster:
            name = str(cluster)
            if _CLUSTER_NAME_PATTERN.fullmatch(name) is None:
                msg = (
                    f"CockroachDB cluster name {name!r} may contain only letters, "
                    "digits, hyphens, underscores, and periods."
                )
                raise ImproperConfigurationError(msg)
            routing = f"--cluster={name}"
            existing = server_settings.get("options")
            server_settings["options"] = f"{existing} {routing}" if existing else routing
    if "default_transaction_use_follower_reads" in result:
        val = result.pop("default_transaction_use_follower_reads")
        if not isinstance(val, bool):
            msg = "default_transaction_use_follower_reads must be a boolean"
            raise ImproperConfigurationError(msg)
        server_settings.setdefault("default_transaction_use_follower_reads", "on" if val else "off")
    for key in ("results_buffer_size",):
        if key not in result:
            continue
        value = result.pop(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ImproperConfigurationError(msg)
        server_settings.setdefault(key, str(value))
    if server_settings:
        result["server_settings"] = server_settings
    return result


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry.

    Translated errors arrive as ``SerializationConflictError`` with no SQLSTATE
    attribute, so the class check comes first, and the SQLSTATE branch covers
    raw driver errors raised outside the translation seam.

    The cause chain is also walked because CockroachDB reports a serialization
    failure at COMMIT for the write-skew case, and transaction control wraps the
    driver error with ``raise ... from e``, which keeps the original reachable.
    The walk descends only through SQLSpec's own errors, so an exception the
    caller raised from a conflict is treated as the deliberate abort it is
    rather than being retried against a database that will never accept it.

    Args:
        error: The exception raised by the transaction body or its commit.

    Returns:
        True when the transaction should be retried.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    depth = 0
    while current is not None and id(current) not in seen and depth < _MAX_CAUSE_CHAIN_DEPTH:
        seen.add(id(current))
        depth += 1
        if isinstance(current, SerializationConflictError):
            return True
        if has_sqlstate(current) and str(current.sqlstate) == "40001":
            return True
        if not isinstance(current, SQLSpecError):
            return False
        next_exc = cast("BaseException | None", cast("Any", current).__cause__)
        if next_exc is None and not getattr(current, "__suppress_context__", False):
            next_exc = cast("BaseException | None", cast("Any", current).__context__)
        current = next_exc
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachAsyncpgRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds.

    The exponential term is capped before jitter is applied so that delays stay
    spread out once the cap is reached, rather than collapsing onto a single
    value at the exact moment contention is highest.
    """
    capped_ms: float = min(config.base_delay_ms * (2**attempt), config.max_delay_ms)
    if capped_ms <= 0.0:
        return 0.0
    return _SYSTEM_RANDOM.uniform(capped_ms / 2.0, capped_ms) / 1000.0


_STALENESS_LITERAL: Final[re.Pattern[str]] = re.compile(r"'[^'\\;]+'")
_STALENESS_FUNCTION: Final[re.Pattern[str]] = re.compile(
    r"(?:follower_read_timestamp|with_max_staleness|with_min_timestamp)"
    r"\(\s*(?:'[^'\\;]*'\s*(?:,\s*(?:'[^'\\;]*'|true|false)\s*)*)?\)",
    re.IGNORECASE,
)


def validate_follower_read_staleness(staleness: str) -> str:
    """Validate a follower-read staleness clause.

    ``AS OF SYSTEM TIME`` accepts no placeholders, so the value is interpolated
    into the statement and must be restricted to the literal and function forms
    CockroachDB documents.

    Args:
        staleness: Interval or timestamp literal, or a staleness function call.

    Returns:
        The trimmed staleness clause.

    Raises:
        ImproperConfigurationError: If the value matches no accepted form.
    """
    candidate = staleness.strip()
    if _STALENESS_LITERAL.fullmatch(candidate) or _STALENESS_FUNCTION.fullmatch(candidate):
        return candidate
    msg = (
        "default_staleness must be a quoted interval or timestamp literal such as \"'-10s'\", "
        "or one of follower_read_timestamp(), with_max_staleness(...), with_min_timestamp(...)."
    )
    raise ImproperConfigurationError(msg)


def build_native_export(
    query: str, parameters: "list[Any]", uri: str, file_format: str, options: "dict[str, Any]"
) -> "tuple[str, list[Any]]":
    """Wrap compiled query SQL without embedding destination or CSV values."""
    format_sql = {"csv": "CSV", "parquet": "PARQUET"}[file_format]
    values = [*parameters, uri]
    command = "EXPORT INTO " + format_sql + " " + ("$" + str(len(values)))
    if file_format == "csv" and "nullas" in options:
        values.append(options["nullas"])
        command += " WITH nullas = " + ("$" + str(len(values)))
    return command + " FROM (" + query.rstrip().removesuffix(";") + "\n)", values


def build_native_import(table: str, uri: str, file_format: str, options: "dict[str, Any]") -> "tuple[str, list[Any]]":
    """Quote the target identifier and bind explicit CSV conventions."""
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise ValueError(msg)
    target = ".".join(quote_identifier(part) for part in parts)
    format_sql = {"csv": "CSV", "parquet": "PARQUET"}[file_format]
    values: list[Any] = [uri]
    command = "IMPORT INTO " + target + " " + format_sql + " DATA (" + "$1" + ")"
    clauses = []
    if file_format == "csv":
        for key in ("skip", "nullif"):
            if key in options:
                values.append(str(options[key]))
                clauses.append(key + " = " + ("$" + str(len(values))))
    if clauses:
        command += " WITH " + ", ".join(clauses)
    return command, values


def native_export_telemetry(
    rows: "list[dict[str, Any]]", destination: str, backend: str, file_format: str
) -> "StorageTelemetry":
    """Retain measured export metadata and generated relative filenames."""
    return {
        "destination": destination,
        "backend": backend,
        "format": file_format,
        "rows_processed": sum(int(row["rows"]) for row in rows),
        "bytes_processed": sum(int(row["bytes"]) for row in rows),
        "extra": {"files": [str(row["filename"]) for row in rows]},
    }


def native_import_telemetry(
    rows: "list[dict[str, Any]]", table: str, backend: str, file_format: str
) -> "StorageTelemetry":
    """Expose import job metadata without treating logical bytes as file size."""
    row = rows[0]
    if row["status"] != "succeeded":
        msg = "Native storage import did not succeed"
        raise ValueError(msg)
    return {
        "destination": table,
        "backend": backend,
        "format": file_format,
        "rows_processed": int(row["rows"]),
        "extra": {"job_id": row["job_id"], "status": row["status"]},
    }


def normalize_native_export_query(query: str) -> str | None:
    """Remove a terminal delimiter, preserving comments; refuse raw scripts."""
    if ";" not in query:
        return query
    tokens = tokenize(query, read="postgres")
    delimiters = [token for token in tokens if token.token_type == TokenType.SEMICOLON]
    if not delimiters:
        return query
    if len(delimiters) != 1 or delimiters[0] is not tokens[-1]:
        return None
    delimiter = delimiters[0]
    return query[: delimiter.start] + query[delimiter.end + 1 :]
