"""CockroachDB AsyncPG adapter helpers."""

import secrets
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr
from sqlglot import tokenize
from sqlglot.tokenizer_core import TokenType

from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec.storage import StorageTelemetry

__all__ = (
    "CockroachAsyncpgRetryConfig",
    "build_native_export",
    "build_native_import",
    "calculate_backoff_seconds",
    "is_retryable_error",
    "native_export_telemetry",
    "native_import_telemetry",
    "normalize_native_export_query",
)

# Retry configuration defaults (module-level for mypyc compatibility)
_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True


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


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry."""
    if has_sqlstate(error):
        return str(error.sqlstate) == "40001"
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachAsyncpgRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds."""
    base: float = config.base_delay_ms * (2**attempt)
    scale: int = 1000
    max_jitter: int = max(int(base * scale), 0)
    jitter: float = secrets.randbelow(max_jitter + 1) / scale if max_jitter else 0.0
    delay_ms: float = min(base + jitter, config.max_delay_ms)
    return delay_ms / 1000.0


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
