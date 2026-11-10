"""Public typed feature dictionaries accept disabled tenant strategies."""

from collections.abc import Mapping
from typing import assert_type

from sqlspec.adapters.asyncpg import AsyncpgDriverFeatures
from sqlspec.adapters.psqlpy import PsqlpyDriverFeatures
from sqlspec.adapters.psycopg import PsycopgDriverFeatures
from sqlspec.core.tenant import AsyncTenantStrategy, SyncTenantStrategy, TenantLeaseState
from sqlspec.utils.tenant import TenantContextPayload

asyncpg_features: AsyncpgDriverFeatures = {"tenant_strategy": None}
psycopg_features: PsycopgDriverFeatures = {"tenant_strategy": None}
psqlpy_features: PsqlpyDriverFeatures = {"tenant_strategy": None}


class BaselineSyncStrategy:
    """Supply a baseline mapping through the public structural protocol."""

    __slots__ = ()

    def checkout(self, connection: object, payload: "TenantContextPayload") -> "Mapping[str, str | None]":
        return dict(payload.variables)

    def checkin(self, connection: object, state: "TenantLeaseState") -> None:
        return None


class BaselineAsyncStrategy:
    """Supply an awaited baseline mapping through the public structural protocol."""

    __slots__ = ()

    async def checkout(self, connection: object, payload: "TenantContextPayload") -> "Mapping[str, str | None]":
        return dict(payload.variables)

    async def checkin(self, connection: object, state: "TenantLeaseState") -> None:
        return None


def _accept_sync(strategy: "SyncTenantStrategy") -> "SyncTenantStrategy":
    return strategy


def _accept_async(strategy: "AsyncTenantStrategy") -> "AsyncTenantStrategy":
    return strategy


mapping_sync_strategy = _accept_sync(BaselineSyncStrategy())
mapping_async_strategy = _accept_async(BaselineAsyncStrategy())


async def check_baseline_return_types(payload: "TenantContextPayload") -> None:
    assert_type(mapping_sync_strategy.checkout(object(), payload), TenantLeaseState | Mapping[str, str | None])
    assert_type(await mapping_async_strategy.checkout(object(), payload), TenantLeaseState | Mapping[str, str | None])
