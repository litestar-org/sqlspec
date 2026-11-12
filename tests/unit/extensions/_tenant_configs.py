"""Tenant-enabled SQLite configurations that run the real tenant lease lifecycle."""

import sqlite3
from typing import Any, ClassVar

import aiosqlite

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

__all__ = ("AsyncRecordingStrategy", "SyncRecordingStrategy", "TenantAiosqliteConfig", "TenantSqliteConfig", "seen")


def seen(payload: TenantContextPayload) -> str:
    """Describe a checked-out payload for assertions."""
    return "<bypass>" if payload.is_bypass else payload.tenant_id


class SyncRecordingStrategy:
    """Apply nothing; the test lease records the payload of each acquisition."""

    def __init__(self) -> None:
        self.checkouts: list[str] = []

    def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        return TenantLeaseState(payload)

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        return None


class AsyncRecordingStrategy:
    """Apply nothing; the test lease records the payload of each acquisition."""

    def __init__(self) -> None:
        self.checkouts: list[str] = []

    async def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        return TenantLeaseState(payload)

    async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        return None


class _SqliteTenantLease(SyncTenantLease):
    __slots__ = ("_checkouts",)

    def __init__(self, config: Any, strategy: Any) -> None:
        super().__init__(strategy)
        self._checkouts: list[str] = strategy.checkouts

    def _acquire(self) -> Any:
        self._checkouts.append(seen(TenantContext.require(self.tenant_key)))
        return sqlite3.connect(":memory:", check_same_thread=False)

    def _release(self, connection: Any) -> None:
        connection.close()

    def _invalidate(self, connection: Any) -> None:
        connection.close()

    def _finish_transaction(self, connection: Any, exc_type: "type[BaseException] | None") -> None:
        connection.rollback()


class _AiosqliteTenantLease(AsyncTenantLease):
    __slots__ = ("_checkouts",)

    def __init__(self, config: Any, strategy: Any) -> None:
        super().__init__(strategy)
        self._checkouts: list[str] = strategy.checkouts

    async def _acquire(self) -> Any:
        self._checkouts.append(seen(await TenantContext.arequire(self.tenant_key)))
        return await aiosqlite.connect(":memory:")

    async def _release(self, connection: Any) -> None:
        await connection.close()

    async def _invalidate(self, connection: Any) -> None:
        await connection.close()

    async def _finish_transaction(self, connection: Any, exc_type: "type[BaseException] | None") -> None:
        await connection.rollback()


class TenantSqliteConfig(SqliteConfig):
    """SQLite configuration whose sessions and connections run tenant leases."""

    supports_tenant_strategy: ClassVar[bool] = True
    tenant_lease_type: ClassVar[type[Any] | None] = _SqliteTenantLease


class TenantAiosqliteConfig(AiosqliteConfig):
    """aiosqlite configuration whose sessions and connections run tenant leases."""

    supports_tenant_strategy: ClassVar[bool] = True
    tenant_lease_type: ClassVar[type[Any] | None] = _AiosqliteTenantLease
    _connection_context_class: ClassVar[Any] = AsyncPoolConnectionContext
    _session_factory_class: ClassVar[Any] = AsyncPoolSessionFactory
