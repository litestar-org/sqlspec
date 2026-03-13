"""Compiled helpers for shared configuration runtime behavior."""

import asyncio
import threading
from typing import TYPE_CHECKING, Any, TypeVar

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


__all__ = (
    "build_default_statement_config",
    "build_postgres_extension_probe_names",
    "close_async_pool",
    "close_sync_pool",
    "create_async_pool",
    "create_sync_pool",
    "resolve_postgres_extension_state",
    "resolve_runtime_statement_config",
    "seed_runtime_driver_features",
)

PoolT = TypeVar("PoolT")


def build_default_statement_config(default_dialect: str) -> StatementConfig:
    """Build the default statement config for a base config class."""
    return StatementConfig(
        dialect=default_dialect,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
        ),
    )


def seed_runtime_driver_features(
    driver_features: "dict[str, Any] | None", storage_capabilities: "dict[str, Any] | None"
) -> "dict[str, Any]":
    """Clone and seed driver feature state used on the runtime hot path."""
    seeded_features = dict(driver_features) if driver_features else {}
    if storage_capabilities is not None:
        seeded_features.setdefault("storage_capabilities", storage_capabilities)
    return seeded_features


def build_postgres_extension_probe_names(driver_features: "dict[str, Any] | None") -> "list[str]":
    """Return enabled PostgreSQL extension names to probe on first connection."""
    if driver_features is None:
        return []

    extensions: list[str] = []
    if driver_features.get("enable_pgvector", False):
        extensions.append("vector")
    if driver_features.get("enable_paradedb", False):
        extensions.append("pg_search")
    return extensions


def resolve_postgres_extension_state(
    statement_config: StatementConfig,
    driver_features: "dict[str, Any] | None",
    detected_extensions: "set[str] | None" = None,
) -> "tuple[StatementConfig, bool, bool]":
    """Resolve detected PostgreSQL extension flags and promoted dialect."""
    detected = detected_extensions or set()
    pgvector_available = bool(driver_features and driver_features.get("enable_pgvector", False) and "vector" in detected)
    paradedb_available = bool(
        driver_features and driver_features.get("enable_paradedb", False) and "pg_search" in detected
    )

    if statement_config.dialect == "postgres":
        if paradedb_available:
            statement_config = statement_config.replace(dialect="paradedb")
        elif pgvector_available:
            statement_config = statement_config.replace(dialect="pgvector")

    return statement_config, pgvector_available, paradedb_available


def resolve_runtime_statement_config(
    statement_config: StatementConfig | None,
    configured_statement_config: StatementConfig | None,
    default_config: StatementConfig,
) -> StatementConfig:
    """Resolve the effective runtime statement config for a session."""
    if statement_config is not None:
        return statement_config
    if configured_statement_config is not None:
        return configured_statement_config
    return default_config


def create_sync_pool(
    connection_instance: "PoolT | None",
    lock: threading.Lock,
    get_connection_instance: "Callable[[], PoolT | None]",
    create_pool: "Callable[[], PoolT]",
    emit_pool_create: "Callable[[PoolT], None]",
) -> PoolT:
    """Create a sync pool once, using the existing lock/emit hooks."""
    if connection_instance is not None:
        return connection_instance

    with lock:
        existing_pool = get_connection_instance()
        if existing_pool is not None:
            return existing_pool
        pool = create_pool()
        emit_pool_create(pool)
        return pool


def close_sync_pool(
    connection_instance: "PoolT | None",
    close_pool: "Callable[[], None]",
    emit_pool_destroy: "Callable[[PoolT], None]",
) -> None:
    """Close a sync pool and emit teardown hooks."""
    close_pool()
    if connection_instance is not None:
        emit_pool_destroy(connection_instance)


async def create_async_pool(
    connection_instance: "PoolT | None",
    lock: asyncio.Lock,
    get_connection_instance: "Callable[[], PoolT | None]",
    create_pool: "Callable[[], Awaitable[PoolT]]",
    emit_pool_create: "Callable[[PoolT], None]",
) -> PoolT:
    """Create an async pool once, using the existing lock/emit hooks."""
    if connection_instance is not None:
        return connection_instance

    async with lock:
        existing_pool = get_connection_instance()
        if existing_pool is not None:
            return existing_pool
        pool = await create_pool()
        emit_pool_create(pool)
        return pool


async def close_async_pool(
    connection_instance: "PoolT | None",
    close_pool: "Callable[[], Awaitable[None]]",
    emit_pool_destroy: "Callable[[PoolT], None]",
) -> None:
    """Close an async pool and emit teardown hooks."""
    await close_pool()
    if connection_instance is not None:
        emit_pool_destroy(connection_instance)
