"""CockroachDB psycopg adapter compiled helpers."""

import random
import re
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr
from sqlglot import tokenize
from sqlglot.tokenizer_core import TokenType

from sqlspec.adapters.psycopg.core import apply_driver_features, build_statement_config, driver_profile
from sqlspec.exceptions import ImproperConfigurationError, SerializationConflictError
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec.storage import StorageTelemetry

__all__ = (
    "CockroachPsycopgRetryConfig",
    "apply_driver_features",
    "build_native_export",
    "build_native_import",
    "build_statement_config",
    "calculate_backoff_seconds",
    "driver_profile",
    "is_retryable_error",
    "native_export_telemetry",
    "native_import_telemetry",
    "normalize_native_export_query",
    "validate_follower_read_staleness",
)

# Retry configuration defaults (module-level for mypyc compatibility)
_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True


# Keep this in sync with cockroach_asyncpg.core.CockroachAsyncpgRetryConfig.
@mypyc_attr(allow_interpreted_subclasses=False)
class CockroachPsycopgRetryConfig:
    """CockroachDB psycopg transaction retry configuration."""

    __slots__ = ("base_delay_ms", "enable_logging", "max_delay_ms", "max_retries")

    def __init__(
        self,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        base_delay_ms: float = _DEFAULT_BASE_DELAY_MS,
        max_delay_ms: float = _DEFAULT_MAX_DELAY_MS,
        enable_logging: bool = _DEFAULT_ENABLE_LOGGING,
    ) -> None:
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.enable_logging = enable_logging

    @classmethod
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachPsycopgRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", _DEFAULT_MAX_RETRIES)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", _DEFAULT_BASE_DELAY_MS)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", _DEFAULT_MAX_DELAY_MS)),
            enable_logging=bool(driver_features.get("enable_retry_logging", _DEFAULT_ENABLE_LOGGING)),
        )


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry.

    Translated errors arrive as ``SerializationConflictError`` with no SQLSTATE
    attribute, so the class check comes first, and the SQLSTATE branch covers
    raw driver errors raised outside the translation seam.

    The cause chain is also walked because CockroachDB reports a serialization
    failure at COMMIT for the write-skew case, and transaction control wraps the
    driver error with ``raise ... from e``, which keeps the original reachable.

    Args:
        error: The exception raised by the transaction body or its commit.

    Returns:
        True when the transaction should be retried.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, SerializationConflictError):
            return True
        if has_sqlstate(current) and str(current.sqlstate) == "40001":
            return True
        current = cast("BaseException | None", cast("Any", current).__cause__)
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachPsycopgRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds.

    The exponential term is capped before jitter is applied so that delays stay
    spread out once the cap is reached, rather than collapsing onto a single
    value at the exact moment contention is highest.
    """
    capped_ms: float = min(config.base_delay_ms * (2**attempt), config.max_delay_ms)
    if capped_ms <= 0.0:
        return 0.0
    return random.uniform(capped_ms / 2.0, capped_ms) / 1000.0  # noqa: S311


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
    if not parameters:
        query = query.replace("%", "%%")
    values: list[Any] = [uri]
    command = "EXPORT INTO " + format_sql + " " + "%s"
    if file_format == "csv" and "nullas" in options:
        values.append(options["nullas"])
        command += " WITH nullas = " + "%s"
    return command + " FROM (" + query.rstrip().removesuffix(";") + "\n)", [*values, *parameters]


def build_native_import(table: str, uri: str, file_format: str, options: "dict[str, Any]") -> "tuple[str, list[Any]]":
    """Quote the target identifier and bind explicit CSV conventions."""
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise ValueError(msg)
    target = ".".join(quote_identifier(part) for part in parts).replace("%", "%%")
    format_sql = {"csv": "CSV", "parquet": "PARQUET"}[file_format]
    values: list[Any] = [uri]
    command = "IMPORT INTO " + target + " " + format_sql + " DATA (" + "%s" + ")"
    clauses = []
    if file_format == "csv":
        for key in ("skip", "nullif"):
            if key in options:
                values.append(str(options[key]))
                clauses.append(key + " = " + "%s")
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
