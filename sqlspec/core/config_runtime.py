"""Compiled helpers for shared configuration runtime behavior."""

import asyncio
import threading
from typing import TYPE_CHECKING, Any, TypeVar

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


__all__ = (
    "build_default_statement_config",
    "close_async_pool",
    "close_sync_pool",
    "create_async_pool",
    "create_sync_pool",
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
