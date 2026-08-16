"""Modular maintenance, pruning, and compaction engine for ADK stores.

Provides standalone functions to prune aged sessions, purge expired events,
sweep scoped memories, and execute dialect-specific table maintenance without
framework worker coupling.
"""

import time
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from typing_extensions import TypedDict

from sqlspec.extensions.adk._config_utils import (
    _adk_adapter_store_class,
    _adk_memory_store_config,
    _adk_session_store_config,
)
from sqlspec.utils.sync_tools import async_, await_

__all__ = (
    "MaintenanceReport",
    "PruneReport",
    "maintain_tables",
    "maintain_tables_sync",
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


class MaintenanceReport(TypedDict):
    """Execution telemetry for database table maintenance."""

    operations: dict[str, Any]
    total_elapsed_ms: float


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
    """
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "session_table", "adk_session")
    cutoff = datetime.now(timezone.utc) - timedelta(days=idle_days)
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
    """
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "events_table", "adk_event")
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
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
    """
    start = time.perf_counter()
    store = _resolve_memory_store(target)
    table_name = getattr(store, "memory_table", "adk_memory")
    scope_param = None if scope == "all" else scope
    deleted = await _call_store_method(
        store, "delete_entries_older_than", older_than_days, app_name=app_name, scope=scope_param
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
    """
    start = time.perf_counter()
    store = _resolve_session_store(target)
    table_name = getattr(store, "user_state_table", "adk_user_state")
    cutoff = datetime.now(timezone.utc) - timedelta(days=idle_days)
    deleted = await _call_store_method(store, "delete_idle_user_states", cutoff, app_name=app_name)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return PruneReport(deleted_count=deleted, elapsed_ms=elapsed_ms, table=str(table_name))


async def _maintain_sqlite(session_context: Any, vacuum: bool, analyze: bool, ops: dict[str, Any]) -> None:
    if hasattr(session_context, "__aenter__"):
        async with session_context as driver:
            if vacuum:
                await driver.execute("PRAGMA incremental_vacuum;")
                ops["incremental_vacuum"] = "executed"
            if analyze:
                await driver.execute("PRAGMA optimize;")
                ops["optimize"] = "executed"
    else:

        def _sync() -> None:
            with session_context as driver:
                if vacuum:
                    driver.execute("PRAGMA incremental_vacuum;")
                    ops["incremental_vacuum"] = "executed"
                if analyze:
                    driver.execute("PRAGMA optimize;")
                    ops["optimize"] = "executed"

        await async_(_sync)()


async def _maintain_duckdb(session_context: Any, ops: dict[str, Any]) -> None:
    if hasattr(session_context, "__aenter__"):
        async with session_context as driver:
            await driver.execute("CHECKPOINT;")
            ops["checkpoint"] = "executed"
    else:

        def _sync() -> None:
            with session_context as driver:
                driver.execute("CHECKPOINT;")
                ops["checkpoint"] = "executed"

        await async_(_sync)()


async def _exec_pg_table_async(driver: Any, tbl: str, action: str, ops: dict[str, Any]) -> None:
    try:
        await driver.execute(f"{action} {tbl};")
        ops[f"{action}_{tbl}"] = "executed"
    except Exception as e:
        ops[f"{action}_{tbl}"] = f"skipped: {e}"


def _exec_pg_table_sync(driver: Any, tbl: str, action: str, ops: dict[str, Any]) -> None:
    try:
        driver.execute(f"{action} {tbl};")
        ops[f"{action}_{tbl}"] = "executed"
    except Exception as e:
        ops[f"{action}_{tbl}"] = f"skipped: {e}"


async def _maintain_postgres(
    session_context: Any, tables: Sequence[str], vacuum: bool, analyze: bool, ops: dict[str, Any]
) -> None:
    action = "VACUUM ANALYZE" if (vacuum and analyze) else ("VACUUM" if vacuum else "ANALYZE")
    if hasattr(session_context, "__aenter__"):
        async with session_context as driver:
            for tbl in tables:
                await _exec_pg_table_async(driver, tbl, action, ops)
    else:

        def _sync() -> None:
            with session_context as driver:
                for tbl in tables:
                    _exec_pg_table_sync(driver, tbl, action, ops)

        await async_(_sync)()


async def _exec_oracle_table_async(driver: Any, tbl: str, ops: dict[str, Any]) -> None:
    try:
        await driver.execute(f"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, UPPER('{tbl}')); END;")
        ops[f"gather_stats_{tbl}"] = "executed"
    except Exception as e:
        ops[f"gather_stats_{tbl}"] = f"skipped: {e}"


def _exec_oracle_table_sync(driver: Any, tbl: str, ops: dict[str, Any]) -> None:
    try:
        driver.execute(f"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, UPPER('{tbl}')); END;")
        ops[f"gather_stats_{tbl}"] = "executed"
    except Exception as e:
        ops[f"gather_stats_{tbl}"] = f"skipped: {e}"


async def _maintain_oracle(session_context: Any, tables: Sequence[str], analyze: bool, ops: dict[str, Any]) -> None:
    if not analyze:
        return
    if hasattr(session_context, "__aenter__"):
        async with session_context as driver:
            for tbl in tables:
                await _exec_oracle_table_async(driver, tbl, ops)
    else:

        def _sync() -> None:
            with session_context as driver:
                for tbl in tables:
                    _exec_oracle_table_sync(driver, tbl, ops)

        await async_(_sync)()


async def _exec_mysql_table_async(driver: Any, tbl: str, action: str, ops: dict[str, Any]) -> None:
    try:
        await driver.execute(f"{action} {tbl};")
        ops[f"{action}_{tbl}"] = "executed"
    except Exception as e:
        ops[f"{action}_{tbl}"] = f"skipped: {e}"


def _exec_mysql_table_sync(driver: Any, tbl: str, action: str, ops: dict[str, Any]) -> None:
    try:
        driver.execute(f"{action} {tbl};")
        ops[f"{action}_{tbl}"] = "executed"
    except Exception as e:
        ops[f"{action}_{tbl}"] = f"skipped: {e}"


async def _maintain_mysql(session_context: Any, tables: Sequence[str], vacuum: bool, ops: dict[str, Any]) -> None:
    action = "OPTIMIZE TABLE" if vacuum else "ANALYZE TABLE"
    if hasattr(session_context, "__aenter__"):
        async with session_context as driver:
            for tbl in tables:
                await _exec_mysql_table_async(driver, tbl, action, ops)
    else:

        def _sync() -> None:
            with session_context as driver:
                for tbl in tables:
                    _exec_mysql_table_sync(driver, tbl, action, ops)

        await async_(_sync)()


async def maintain_tables(
    target: Any,
    *,
    vacuum: bool = True,
    analyze: bool = True,
    tables: Sequence[str] | None = None,
) -> MaintenanceReport:
    """Execute dialect-specific maintenance (vacuum, analyze, checkpoint, optimize) on ADK tables.

    Args:
        target: ADKStore, DatabaseConfig, or DriverAdapter.
        vacuum: Whether to run vacuum / storage compaction.
        analyze: Whether to update database optimizer statistics.
        tables: Optional explicit sequence of table names to maintain.

    Returns:
        MaintenanceReport with execution details per operation.
    """
    start = time.perf_counter()
    config = getattr(target, "config", target)
    statement_config = getattr(config, "statement_config", None)
    dialect = getattr(statement_config, "dialect", "") if statement_config else ""
    dialect_str = str(dialect).lower()

    if tables is None:
        if hasattr(config, "extension_config") and isinstance(getattr(config, "extension_config", None), dict):
            s_cfg = _adk_session_store_config(config)
            m_cfg = _adk_memory_store_config(config)
            tables = (
                s_cfg["session_table"],
                s_cfg["events_table"],
                s_cfg["app_state_table"],
                s_cfg["user_state_table"],
                s_cfg["metadata_table"],
                m_cfg["memory_table"],
            )
        else:
            tables = ("adk_session", "adk_event", "adk_app_state", "adk_user_state", "adk_memory")

    ops: dict[str, Any] = {}
    session_context = config.provide_session() if hasattr(config, "provide_session") else target

    if "sqlite" in dialect_str:
        await _maintain_sqlite(session_context, vacuum, analyze, ops)
    elif "duckdb" in dialect_str:
        await _maintain_duckdb(session_context, ops)
    elif "postgres" in dialect_str or "cockroach" in dialect_str:
        await _maintain_postgres(session_context, tables, vacuum, analyze, ops)
    elif "oracle" in dialect_str:
        await _maintain_oracle(session_context, tables, analyze, ops)
    elif "mysql" in dialect_str:
        await _maintain_mysql(session_context, tables, vacuum, ops)
    else:
        ops["status"] = f"no-op for dialect {dialect_str}"

    total_elapsed = (time.perf_counter() - start) * 1000.0
    return MaintenanceReport(operations=ops, total_elapsed_ms=total_elapsed)


prune_sessions_sync = await_(prune_sessions)
prune_events_sync = await_(prune_events)
prune_memory_sync = await_(prune_memory)
prune_user_state_sync = await_(prune_user_state)
maintain_tables_sync = await_(maintain_tables)
