"""Retention helpers for ADK stores.

Standalone functions to prune aged sessions, purge expired events, sweep scoped
memories, and prune aged artifact versions, decoupled from any framework worker
so retention can run from a cron job, a CLI, or a task queue.

Storage-level upkeep such as vacuuming, checkpointing, or refreshing optimizer
statistics is deliberately out of scope. Those are operator decisions that need
elevated privileges and their own scheduling, and several cannot run inside the
transaction a store session provides.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from typing_extensions import TypedDict

from sqlspec.extensions.adk._config_utils import _adk_adapter_store_class
from sqlspec.utils.sync_tools import await_

__all__ = (
    "PruneReport",
    "prune_artifacts",
    "prune_artifacts_sync",
    "prune_events",
    "prune_events_sync",
    "prune_memory",
    "prune_memory_sync",
    "prune_sessions",
    "prune_sessions_sync",
    "prune_user_state",
    "prune_user_state_sync",
)


class PruneReport(TypedDict):
    """Performance telemetry for a pruning operation."""

    deleted_count: int
    elapsed_ms: float
    table: str


def _resolve_session_store(target: Any) -> Any:
    """Resolve an ADK session store instance from target."""
    if hasattr(target, "delete_idle_sessions") or hasattr(target, "delete_expired_events"):
        return target
    if hasattr(target, "extension_config") or hasattr(target, "provide_session"):
        store_cls = _adk_adapter_store_class(target, "ADKStore")
        return store_cls(target)
    msg = f"Cannot resolve ADK session store from target of type {type(target).__name__}"
    raise TypeError(msg)


def _resolve_memory_store(target: Any) -> Any:
    """Resolve an ADK memory store instance from target."""
    if hasattr(target, "delete_entries_older_than"):
        return target
    if hasattr(target, "extension_config") or hasattr(target, "provide_session"):
        store_cls = _adk_adapter_store_class(target, "ADKMemoryStore")
        return store_cls(target)
    msg = f"Cannot resolve ADK memory store from target of type {type(target).__name__}"
    raise TypeError(msg)


def _resolve_artifact_service(target: Any) -> Any:
    """Resolve a storage-aware ADK artifact service instance from target.

    Args:
        target: Candidate artifact service.

    Returns:
        The artifact service.

    Raises:
        TypeError: If target does not expose both the retention method and its metadata store.
    """
    if hasattr(target, "delete_artifacts_older_than") and hasattr(target, "store"):
        return target
    msg = (
        f"Cannot resolve ADK artifact service from target of type {type(target).__name__}. "
        "Artifact pruning removes content objects as well as metadata rows, so it requires a "
        "storage-aware artifact service rather than a database config or a bare metadata store."
    )
    raise TypeError(msg)


def _ensure_positive_days(value: Any, parameter: str) -> int:
    """Validate a retention age expressed in whole days.

    Args:
        value: Candidate age value.
        parameter: Name of the keyword argument being validated.

    Returns:
        The validated age in days.

    Raises:
        ValueError: If the value is not a positive integer.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        msg = f"{parameter} must be a positive integer, got {value!r}"
        raise ValueError(msg)
    return value


async def _call_store_method(store: Any, method_name: str, *args: Any, **kwargs: Any) -> int:
    """Call a store method handling both async and sync implementations."""
    method = getattr(store, method_name)
    result = method(*args, **kwargs)
    if hasattr(result, "__await__"):
        deleted = await result
    else:
        deleted = result
    return int(deleted) if deleted is not None else 0


async def prune_sessions(target: Any, *, idle_days: int = 30, app_name: str | None = None) -> PruneReport:
    """Prune sessions that have been idle longer than specified days.

    Args:
        target: ADKStore instance, DatabaseConfig, or DriverAdapter.
        idle_days: Number of days of inactivity before a session is pruned.
        app_name: Optional application name to limit pruning.

    Returns:
        PruneReport containing deleted row count and timing.

    Raises:
        ValueError: If ``idle_days`` is not a positive integer.
    """
    retention_days = _ensure_positive_days(idle_days, "idle_days")
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "session_table", "adk_session")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = await _call_store_method(store, "delete_idle_sessions", cutoff, app_name=app_name)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


async def prune_events(target: Any, *, older_than_days: int = 90, app_name: str | None = None) -> PruneReport:
    """Purge event history records older than specified days.

    Args:
        target: ADKStore instance, DatabaseConfig, or DriverAdapter.
        older_than_days: Number of days to retain event history.
        app_name: Optional application name to limit purge.

    Returns:
        PruneReport containing deleted row count and timing.

    Raises:
        ValueError: If ``older_than_days`` is not a positive integer.
    """
    retention_days = _ensure_positive_days(older_than_days, "older_than_days")
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "events_table", "adk_event")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = await _call_store_method(store, "delete_expired_events", cutoff, app_name=app_name)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


async def prune_memory(
    target: Any,
    *,
    older_than_days: int = 90,
    app_name: str | None = None,
    scope: Literal["all", "user", "app"] = "user",
) -> PruneReport:
    """Prune memory entries older than specified days.

    Args:
        target: ADKMemoryStore instance, DatabaseConfig, or DriverAdapter.
        older_than_days: Number of days to retain memory records.
        app_name: Optional application name to limit pruning.
        scope: Scope level to prune ('user', 'app', or 'all'). Default: 'user'.

    Returns:
        PruneReport containing deleted row count and timing.

    Raises:
        ValueError: If ``older_than_days`` is not a positive integer.
    """
    retention_days = _ensure_positive_days(older_than_days, "older_than_days")
    start = time.perf_counter()
    store = _resolve_memory_store(target)
    table_name = getattr(store, "memory_table", "adk_memory")
    scope_param = None if scope == "all" else scope
    deleted = await _call_store_method(
        store, "delete_entries_older_than", retention_days, app_name=app_name, scope=scope_param
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


async def prune_user_state(target: Any, *, idle_days: int = 180, app_name: str | None = None) -> PruneReport:
    """Prune user state entries that have been inactive longer than specified days.

    Args:
        target: ADKStore instance, DatabaseConfig, or DriverAdapter.
        idle_days: Inactivity threshold in days.
        app_name: Optional application name filter.

    Returns:
        PruneReport containing deleted row count and timing.

    Raises:
        ValueError: If ``idle_days`` is not a positive integer.
    """
    retention_days = _ensure_positive_days(idle_days, "idle_days")
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "user_state_table", "adk_user_state")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = await _call_store_method(store, "delete_idle_user_states", cutoff, app_name=app_name)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


async def prune_artifacts(target: Any, *, older_than_days: int = 90, app_name: str | None = None) -> PruneReport:
    """Prune artifact versions created more than the specified number of days ago.

    Metadata rows are deleted first, then the content objects they referenced are
    removed on a best-effort basis. ``deleted_count`` is the number of artifact
    version rows deleted, not the number of distinct filenames. Content deletions
    that fail are logged per canonical URI and version by the service; rerunning
    this helper cannot retry them because their metadata is already gone.

    Args:
        target: SQLSpecArtifactService instance. A database config or bare
            metadata store is rejected because it lacks storage context.
        older_than_days: Number of days of artifact versions to retain.
        app_name: Optional application name to limit pruning.

    Returns:
        PruneReport containing deleted version-row count and timing.

    Raises:
        ValueError: If ``older_than_days`` is not a positive integer.
    """
    retention_days = _ensure_positive_days(older_than_days, "older_than_days")
    start = time.perf_counter()
    service = _resolve_artifact_service(target)
    table_name = getattr(service.store, "artifact_table", "adk_artifact")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = await _call_store_method(service, "delete_artifacts_older_than", cutoff, app_name=app_name)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


prune_sessions_sync = await_(prune_sessions)
prune_events_sync = await_(prune_events)
prune_memory_sync = await_(prune_memory)
prune_user_state_sync = await_(prune_user_state)
prune_artifacts_sync = await_(prune_artifacts)
