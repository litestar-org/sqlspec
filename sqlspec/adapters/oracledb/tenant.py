"""Verified Oracle tenant identifiers and administrator-provisioned contexts."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from sqlspec.adapters.oracledb.core import (
    OracleSessionContextConfig,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
)
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "ORACLE_TENANT_IDENTIFIER_MAX_BYTES",
    "OracleAsyncIdentifierStrategy",
    "OracleSyncIdentifierStrategy",
    "compose_tenant_session_hook",
    "tenant_client_identifier",
)

ORACLE_TENANT_IDENTIFIER_MAX_BYTES: Final = 64
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
