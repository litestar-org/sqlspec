"""Oracle extension-table storage feature gates."""

import logging
from collections.abc import Mapping
from typing import Any, cast

from typing_extensions import TypedDict

from sqlspec.adapters.oracledb.data_dictionary import OracleStorageCapabilities, OracleVersionCache
from sqlspec.utils.logging import get_logger, log_with_context

logger = get_logger("sqlspec.adapters.oracledb.storage")

ORACLE_DEFAULT_HASH_PARTITIONS = 16
ORACLE_MIN_HASH_PARTITIONS = 2
ORACLE_RANGE_INTERVALS = {
    "day": "NUMTODSINTERVAL(1, 'DAY')",
    "week": "NUMTODSINTERVAL(7, 'DAY')",
    "month": "NUMTOYMINTERVAL(1, 'MONTH')",
    "year": "NUMTOYMINTERVAL(1, 'YEAR')",
}
ORACLE_COMPRESSION_CLAUSES = {
    "basic": "ROW STORE COMPRESS BASIC",
    "oltp": "ROW STORE COMPRESS ADVANCED",
    "advanced": "ROW STORE COMPRESS ADVANCED",
    "query_low": "COLUMN STORE COMPRESS FOR QUERY LOW",
    "query_high": "COLUMN STORE COMPRESS FOR QUERY HIGH",
    "archive_low": "COLUMN STORE COMPRESS FOR ARCHIVE LOW",
    "archive_high": "COLUMN STORE COMPRESS FOR ARCHIVE HIGH",
}


class _OracleStorageFeatureReport(TypedDict):
    """Storage clauses applied or degraded for one table."""

    applied: tuple[str, ...]
    clause: str
    degraded: tuple[dict[str, str], ...]


def _resolve_oracle_storage_capabilities_sync(driver: Any) -> OracleStorageCapabilities:
    """Resolve storage options through the driver's pool-cached data dictionary."""
    return cast("OracleStorageCapabilities", driver.data_dictionary.get_storage_capabilities(driver))


async def _resolve_oracle_storage_capabilities_async(driver: Any) -> OracleStorageCapabilities:
    """Resolve storage options through the driver's pool-cached data dictionary."""
    return cast("OracleStorageCapabilities", await driver.data_dictionary.get_storage_capabilities(driver))


def _oracle_table_feature_report(
    config: Any,
    extension_name: str,
    settings: Mapping[str, Any],
    table_kind: str,
    *,
    in_memory: bool,
    hash_partition_key: str,
    range_partition_key: str,
    table_options_key: str | None = None,
) -> _OracleStorageFeatureReport:
    """Return gated Oracle table clauses and a structured degradation report."""
    capabilities, unavailable_reason = _cached_storage_capabilities(config)
    clauses: list[str] = []
    applied: list[str] = []
    degraded: list[dict[str, str]] = []

    compression = settings.get("compression")
    if isinstance(compression, Mapping) and compression.get("enabled"):
        algorithm = str(compression.get("algorithm") or "advanced").lower()
        try:
            compression_clause = ORACLE_COMPRESSION_CLAUSES[algorithm]
        except KeyError as exc:
            supported = ", ".join(sorted(ORACLE_COMPRESSION_CLAUSES))
            msg = f"Unsupported Oracle compression algorithm {algorithm!r}. Supported values: {supported}"
            raise ValueError(msg) from exc
        capability = "basic_compression" if algorithm == "basic" else "advanced_compression"
        _apply_or_degrade(
            clauses,
            applied,
            degraded,
            capabilities,
            capability,
            compression_clause,
            unavailable_reason,
            extension_name,
            table_kind,
        )

    if in_memory:
        _apply_or_degrade(
            clauses,
            applied,
            degraded,
            capabilities,
            "in_memory",
            "INMEMORY PRIORITY HIGH",
            unavailable_reason,
            extension_name,
            table_kind,
        )

    resolved_options_key = table_options_key or (
        "events_table_options" if table_kind == "events" else f"{table_kind}_table_options"
    )
    table_options = settings.get(resolved_options_key)
    if table_options:
        clauses.append(str(table_options).strip())
        applied.append("table_options")

    partitioning = settings.get("partitioning")
    if isinstance(partitioning, Mapping):
        partition_clause = _oracle_partition_clause(
            partitioning, table_kind, hash_partition_key, range_partition_key
        )
        if partition_clause:
            _apply_or_degrade(
                clauses,
                applied,
                degraded,
                capabilities,
                "partitioning",
                partition_clause,
                unavailable_reason,
                extension_name,
                table_kind,
            )

    clause = ""
    if clauses:
        clause = " " + " ".join(clauses).replace("'", "''")
    return {"applied": tuple(applied), "clause": clause, "degraded": tuple(degraded)}


def _cached_storage_capabilities(config: Any) -> "tuple[OracleStorageCapabilities, str | None]":
    """Read capabilities only from the config's shared Oracle cache."""
    cache = getattr(config, "_oracle_version_cache", None)
    if not isinstance(cache, OracleVersionCache) or not cache.storage_capabilities_resolved:
        return _assumed_storage_capabilities(), None
    reason = cache.storage_capabilities_reason
    return cache.storage_capabilities, reason


def _unsupported_capabilities() -> OracleStorageCapabilities:
    return {
        "advanced_compression": False,
        "basic_compression": False,
        "in_memory": False,
        "partitioning": False,
    }


def _assumed_storage_capabilities() -> OracleStorageCapabilities:
    """Preserve configured clauses for offline DDL rendering before preparation."""
    return {"advanced_compression": True, "basic_compression": True, "in_memory": True, "partitioning": True}


def _apply_or_degrade(
    clauses: list[str],
    applied: list[str],
    degraded: list[dict[str, str]],
    capabilities: OracleStorageCapabilities,
    optimization: str,
    clause: str,
    unavailable_reason: str | None,
    extension_name: str,
    table_kind: str,
) -> None:
    """Append a supported clause or record and log its degradation."""
    if capabilities.get(optimization, False):
        clauses.append(clause)
        applied.append(optimization)
        return
    reason = unavailable_reason or "unlicensed_or_unavailable"
    degraded.append({"optimization": optimization, "reason": reason})
    log_with_context(
        logger,
        logging.WARNING,
        "oracle.storage.optimization.degraded",
        extension=extension_name,
        optimization=optimization,
        reason=reason,
        table_kind=table_kind,
    )


def _oracle_partition_clause(
    partitioning: Mapping[str, Any], table_kind: str, hash_partition_key: str, range_partition_key: str
) -> str:
    strategy = str(partitioning.get("strategy") or "").lower()
    if not strategy:
        return ""
    table_key = partitioning.get(f"{table_kind}_partition_key")
    configured_key = table_key if table_key is not None else partitioning.get("partition_key")
    if strategy == "hash":
        partition_key = _validate_oracle_identifier(str(configured_key or hash_partition_key), "partition key")
        partition_count = partitioning.get(
            "partition_count", partitioning.get("partitions", ORACLE_DEFAULT_HASH_PARTITIONS)
        )
        if not isinstance(partition_count, int) or partition_count < ORACLE_MIN_HASH_PARTITIONS:
            msg = "Oracle hash partitioning requires partition_count >= 2"
            raise ValueError(msg)
        return f"PARTITION BY HASH ({partition_key}) PARTITIONS {partition_count}"
    if strategy == "range":
        partition_key = _validate_oracle_identifier(str(configured_key or range_partition_key), "partition key")
        interval = str(partitioning.get("interval") or "month").lower()
        interval_sql = ORACLE_RANGE_INTERVALS.get(interval)
        if interval_sql is None:
            supported = ", ".join(sorted(ORACLE_RANGE_INTERVALS))
            msg = f"Unsupported Oracle range partition interval {interval!r}. Supported values: {supported}"
            raise ValueError(msg)
        initial_less_than = str(partitioning.get("initial_less_than") or "TIMESTAMP '2000-01-01 00:00:00'")
        return f"PARTITION BY RANGE ({partition_key}) INTERVAL ({interval_sql}) (PARTITION p_initial VALUES LESS THAN ({initial_less_than}))"
    msg = f"Unsupported Oracle partitioning strategy {strategy!r}. Supported values: hash, range"
    raise ValueError(msg)


def _validate_oracle_identifier(value: str, field_name: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        msg = f"Oracle {field_name} must be a simple SQL identifier"
        raise ValueError(msg)
    return value
