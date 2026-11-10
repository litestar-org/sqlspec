"""Verified Oracle tenant identifiers and administrator-provisioned contexts."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from sqlspec.adapters.oracledb.core import (
    OracleSessionContextConfig,
    _validate_oracle_identifier,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
)
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "ORACLE_TENANT_IDENTIFIER_MAX_BYTES",
    "OracleAsyncIdentifierStrategy",
    "OracleAsyncVPDStrategy",
    "OracleSyncIdentifierStrategy",
    "OracleSyncVPDStrategy",
    "build_oracle_async_tenant_lease",
    "build_oracle_sync_tenant_lease",
    "compose_tenant_session_hook",
    "tenant_client_identifier",
    "validate_oracle_tenant_hook",
)

ORACLE_TENANT_IDENTIFIER_MAX_BYTES: Final = 64
_CONTEXT_QUERY: Final = "SELECT SYS_CONTEXT(:namespace, :attribute) FROM DUAL"
_IDENTIFIER_QUERY: Final = "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL"


def tenant_client_identifier() -> str | None:
    """Return the active tenant identity without truncating Oracle's identifier.

    Raises:
        TenantContextError: The identity exceeds Oracle's 64-byte limit.
    """
    tenant_id = TenantContext.get_tenant_id()
    if tenant_id is not None and len(tenant_id.encode("utf-8")) > ORACLE_TENANT_IDENTIFIER_MAX_BYTES:
        msg = "Oracle tenant identifiers must fit within 64 UTF-8 bytes"
        raise TenantContextError(msg)
    return tenant_id


def compose_tenant_session_hook(
    existing: OracleSessionContextConfig | Mapping[str, Any] | None,
) -> OracleSessionContextConfig:
    """Copy session options and reserve the client identifier for tenant identity.

    Raises:
        ImproperConfigurationError: An existing client identifier conflicts.
    """
    source = OracleSessionContextConfig(**existing) if isinstance(existing, Mapping) else existing
    if source is not None and source.client_identifier not in (None, tenant_client_identifier):
        msg = "Oracle tenant strategy conflicts with session_context_hook.client_identifier"
        raise ImproperConfigurationError(msg)
    values = {name: getattr(source, name) for name in OracleSessionContextConfig.__slots__} if source else {}
    values.update(client_identifier=tenant_client_identifier, clear_identifier=True, reset_on_release=True)
    return OracleSessionContextConfig(**values)


class OracleSyncIdentifierStrategy:
    """Apply an Oracle client identifier, preserving configured session hooks.

    Client identifiers are metadata. Database policies must enforce row access.
    """

    __slots__ = ("_hook", "source_hook")

    def __init__(self, hook: OracleSessionContextConfig | Mapping[str, Any] | None = None) -> None:
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
        self._hook = compose_tenant_session_hook(self.source_hook)

    def checkout(self, connection: "OracleSyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply the captured payload and verify the identifier on the server."""
        with TenantContext.scope(payload):
            tenant_client_identifier()
            execute_session_checkout_sync(connection, self._hook)
        with connection.cursor() as cursor:
            cursor.execute(_IDENTIFIER_QUERY)
            row = cursor.fetchone()
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    def checkin(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        """Clear the identifier and verify that the server has no tenant value."""
        execute_session_release_sync(connection, self._hook)
        with connection.cursor() as cursor:
            cursor.execute(_IDENTIFIER_QUERY)
            row = cursor.fetchone()
        if row is None or row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)


class OracleAsyncIdentifierStrategy:
    """Apply and verify a client identifier on an asynchronous Oracle connection."""

    __slots__ = ("_hook", "source_hook")

    def __init__(self, hook: OracleSessionContextConfig | Mapping[str, Any] | None = None) -> None:
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
        self._hook = compose_tenant_session_hook(self.source_hook)

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply the captured payload and verify the identifier on the server."""
        with TenantContext.scope(payload):
            tenant_client_identifier()
            await execute_session_checkout_async(connection, self._hook)
        with connection.cursor() as cursor:
            await cursor.execute(_IDENTIFIER_QUERY)
            row = await cursor.fetchone()
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    async def checkin(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        """Clear the identifier and verify that the server has no tenant value."""
        await execute_session_release_async(connection, self._hook)
        with connection.cursor() as cursor:
            await cursor.execute(_IDENTIFIER_QUERY)
            row = await cursor.fetchone()
        if row is None or row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)


def validate_oracle_tenant_hook(strategy: Any, hook: OracleSessionContextConfig | None) -> None:
    """Reject a hook that a tenant strategy would silently bypass."""
    if strategy is not None and hook is not None and getattr(strategy, "source_hook", None) != hook:
        msg = "Oracle tenant strategy must be constructed with the configured session_context_hook"
        raise ImproperConfigurationError(msg)


def build_oracle_sync_tenant_lease(config: "OracleSyncConfig") -> "SyncTenantLease":
    """Wrap Oracle pool callbacks with verified tenant cleanup and physical drop."""
    pool: Any = None

    def acquire() -> Any:
        nonlocal pool
        pool = config.provide_pool()
        return pool.acquire()

    def release(connection: Any) -> None:
        pool.release(connection)

    def invalidate(connection: Any) -> None:
        pool.drop(connection)

    def finish(connection: Any, _exc_type: type[BaseException] | None) -> None:
        connection.rollback()

    return SyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish,
    )


def build_oracle_async_tenant_lease(config: "OracleAsyncConfig") -> "AsyncTenantLease":
    """Wrap asynchronous Oracle pool callbacks and dispose unverified sessions."""
    pool: Any = None

    async def acquire() -> Any:
        nonlocal pool
        pool = await config.provide_pool()
        return await pool.acquire()

    async def release(connection: Any) -> None:
        await pool.release(connection)

    async def invalidate(connection: Any) -> None:
        await pool.drop(connection)

    async def finish(connection: Any, _exc_type: type[BaseException] | None) -> None:
        await connection.rollback()

    return AsyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish,
    )


def _validate_vpd_settings(set_procedure: str, clear_procedure: str, namespace: str, attribute: str) -> None:
    for label, value in (("set_procedure", set_procedure), ("clear_procedure", clear_procedure)):
        if not isinstance(value, str):
            msg = f"Oracle tenant {label} must be an identifier"
            raise ImproperConfigurationError(msg)
        for segment in value.split("."):
            _validate_oracle_identifier(segment, label)
    _validate_oracle_identifier(namespace, "namespace")
    _validate_oracle_identifier(attribute, "attribute")


class OracleSyncVPDStrategy(OracleSyncIdentifierStrategy):
    """Apply a tenant through an administrator-provisioned trusted context package."""

    __slots__ = ("_attribute", "_clear_procedure", "_namespace", "_set_procedure")

    def __init__(
        self,
        *,
        set_procedure: str,
        clear_procedure: str,
        namespace: str,
        attribute: str,
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        _validate_vpd_settings(set_procedure, clear_procedure, namespace, attribute)
        self._set_procedure = set_procedure
        self._clear_procedure = clear_procedure
        self._namespace = namespace
        self._attribute = attribute

    def checkout(self, connection: "OracleSyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Set and verify the trusted context after binding the client identifier."""
        state = super().checkout(connection, payload)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"BEGIN {self._set_procedure}(:tenant_id); END;", {"tenant_id": payload.tenant_id})
                cursor.execute(_CONTEXT_QUERY, {"namespace": self._namespace, "attribute": self._attribute})
                row = cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be set"
            raise TenantContextError(msg) from exc
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle trusted tenant context verification failed"
            raise TenantContextError(msg)
        return state

    def checkin(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        """Clear and verify the trusted context before clearing the identifier."""
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"BEGIN {self._clear_procedure}; END;")
                cursor.execute(_CONTEXT_QUERY, {"namespace": self._namespace, "attribute": self._attribute})
                row = cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be cleared"
            raise TenantCleanupError(msg) from exc
        if row is None or row[0] is not None:
            msg = "Oracle trusted tenant context cleanup verification failed"
            raise TenantCleanupError(msg)
        super().checkin(connection, state)


class OracleAsyncVPDStrategy(OracleAsyncIdentifierStrategy):
    """Apply a trusted application context on an asynchronous Oracle connection."""

    __slots__ = ("_attribute", "_clear_procedure", "_namespace", "_set_procedure")

    def __init__(
        self,
        *,
        set_procedure: str,
        clear_procedure: str,
        namespace: str,
        attribute: str,
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        _validate_vpd_settings(set_procedure, clear_procedure, namespace, attribute)
        self._set_procedure = set_procedure
        self._clear_procedure = clear_procedure
        self._namespace = namespace
        self._attribute = attribute

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Set and verify the trusted context after binding the client identifier."""
        state = await super().checkout(connection, payload)
        try:
            with connection.cursor() as cursor:
                await cursor.execute(f"BEGIN {self._set_procedure}(:tenant_id); END;", {"tenant_id": payload.tenant_id})
                await cursor.execute(_CONTEXT_QUERY, {"namespace": self._namespace, "attribute": self._attribute})
                row = await cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be set"
            raise TenantContextError(msg) from exc
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle trusted tenant context verification failed"
            raise TenantContextError(msg)
        return state

    async def checkin(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        """Clear and verify the trusted context before clearing the identifier."""
        try:
            with connection.cursor() as cursor:
                await cursor.execute(f"BEGIN {self._clear_procedure}; END;")
                await cursor.execute(_CONTEXT_QUERY, {"namespace": self._namespace, "attribute": self._attribute})
                row = await cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be cleared"
            raise TenantCleanupError(msg) from exc
        if row is None or row[0] is not None:
            msg = "Oracle trusted tenant context cleanup verification failed"
            raise TenantCleanupError(msg)
        await super().checkin(connection, state)
