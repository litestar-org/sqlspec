"""Verified Oracle tenant identifiers and administrator-provisioned contexts."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.adapters.oracledb.core import (
    OracleSessionContextConfig,
    _validate_oracle_identifier,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
)
from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease, await_owned_release
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
    from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "ORACLE_TENANT_IDENTIFIER_MAX_BYTES",
    "OracleAsyncIdentifierStrategy",
    "OracleAsyncTenantLease",
    "OracleAsyncVPDStrategy",
    "OracleSyncIdentifierStrategy",
    "OracleSyncTenantLease",
    "OracleSyncVPDStrategy",
    "bind_oracle_tenant_hook",
    "compose_tenant_session_hook",
    "tenant_client_identifier",
    "validate_oracle_tenant_hook",
)

ORACLE_TENANT_IDENTIFIER_MAX_BYTES: Final = 64
_IDENTIFIER_QUERY: Final = "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL"
_TENANT_STATE_QUERY: Final = (
    "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER'), SYS_CONTEXT(:namespace, :attribute) FROM DUAL"
)


def tenant_client_identifier() -> str | None:
    """Return the active tenant identity without truncating Oracle's identifier.

    Raises:
        TenantContextError: The identity exceeds Oracle's 64-byte limit.
    """
    tenant_id: str | None = TenantContext.get_tenant_id()
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
    if source is None:
        source = OracleSessionContextConfig()
    if source.client_identifier not in (None, tenant_client_identifier):
        msg = "Oracle tenant strategy conflicts with session_context_hook.client_identifier"
        raise ImproperConfigurationError(msg)
    return OracleSessionContextConfig(
        module=source.module,
        action=source.action,
        client_identifier=tenant_client_identifier,
        ecid=source.ecid,
        edition=source.edition,
        time_zone=source.time_zone,
        nls_parameters=source.nls_parameters,
        contexts=source.contexts,
        context_attributes=source.context_attributes,
        ebs_apps_initialize=source.ebs_apps_initialize,
        mo_global_policy=source.mo_global_policy,
        apex_session=source.apex_session,
        reset_package=source.reset_package,
        clear_identifier=True,
        clear_context_namespaces=source.clear_context_namespaces,
        apex_detach=source.apex_detach,
        reset_on_release=True,
    )


@mypyc_attr(allow_interpreted_subclasses=True)
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
        self._apply_identifier(connection, payload)
        with connection.cursor() as cursor:
            cursor.execute(_IDENTIFIER_QUERY)
            row = cursor.fetchone()
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    def checkin(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        """Clear the identifier and verify that the server has no tenant value."""
        self._clear_identifier(connection, state)
        with connection.cursor() as cursor:
            cursor.execute(_IDENTIFIER_QUERY)
            row = cursor.fetchone()
        if row is None or row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)

    def _apply_identifier(self, connection: "OracleSyncConnection", payload: "TenantContextPayload") -> None:
        with TenantContext.bind(payload):
            tenant_client_identifier()
            execute_session_checkout_sync(connection, self._hook)

    def _clear_identifier(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        with TenantContext.bind(state.payload):
            execute_session_release_sync(connection, self._hook)


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncIdentifierStrategy:
    """Apply and verify a client identifier on an asynchronous Oracle connection."""

    __slots__ = ("_hook", "source_hook")

    def __init__(self, hook: OracleSessionContextConfig | Mapping[str, Any] | None = None) -> None:
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
        self._hook = compose_tenant_session_hook(self.source_hook)

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply the captured payload and verify the identifier on the server."""
        await self._apply_identifier(connection, payload)
        with connection.cursor() as cursor:
            await cursor.execute(_IDENTIFIER_QUERY)
            row = await cursor.fetchone()
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    async def checkin(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        """Clear the identifier and verify that the server has no tenant value."""
        await self._clear_identifier(connection, state)
        with connection.cursor() as cursor:
            await cursor.execute(_IDENTIFIER_QUERY)
            row = await cursor.fetchone()
        if row is None or row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)

    async def _apply_identifier(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> None:
        with TenantContext.bind(payload):
            tenant_client_identifier()
            await execute_session_checkout_async(connection, self._hook)

    async def _clear_identifier(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        with TenantContext.bind(state.payload):
            await execute_session_release_async(connection, self._hook)


def validate_oracle_tenant_hook(strategy: Any, hook: OracleSessionContextConfig | Mapping[str, Any] | None) -> None:
    """Reject a hook that a tenant strategy would silently bypass."""
    normalized_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
    if (
        strategy is not None
        and normalized_hook is not None
        and getattr(strategy, "source_hook", None) != normalized_hook
    ):
        msg = "Oracle tenant strategy must be constructed with the configured session_context_hook"
        raise ImproperConfigurationError(msg)


def bind_oracle_tenant_hook(strategy: Any, hook: OracleSessionContextConfig | Mapping[str, Any] | None) -> Any:
    """Attach ``session_context_hook`` to built-in Oracle strategies when unset and validate consistency."""
    normalized_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
    if strategy is None or normalized_hook is None:
        return strategy
    if (
        isinstance(strategy, (OracleSyncIdentifierStrategy, OracleAsyncIdentifierStrategy))
        and strategy.source_hook is None
    ):
        if type(strategy) is OracleSyncIdentifierStrategy:
            return OracleSyncIdentifierStrategy(normalized_hook)
        if type(strategy) is OracleAsyncIdentifierStrategy:
            return OracleAsyncIdentifierStrategy(normalized_hook)
        if type(strategy) is OracleSyncVPDStrategy:
            return OracleSyncVPDStrategy(
                set_procedure=strategy._set_procedure,
                clear_procedure=strategy._clear_procedure,
                namespace=strategy._namespace,
                attribute=strategy._context_attribute,
                hook=normalized_hook,
            )
        if type(strategy) is OracleAsyncVPDStrategy:
            return OracleAsyncVPDStrategy(
                set_procedure=strategy._set_procedure,
                clear_procedure=strategy._clear_procedure,
                namespace=strategy._namespace,
                attribute=strategy._context_attribute,
                hook=normalized_hook,
            )
        strategy.source_hook = normalized_hook
        strategy._hook = compose_tenant_session_hook(normalized_hook)
        return strategy
    validate_oracle_tenant_hook(strategy, normalized_hook)
    return strategy


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncTenantLease(SyncTenantLease):
    """Wrap Oracle pool hooks with verified tenant cleanup and physical drop.

    Bypass leases skip the tenant strategy but still apply and release the
    configured ``session_context_hook``, matching non-tenant sessions.
    """

    __slots__ = ("_bypass_hook", "_config", "_pool")

    def __init__(self, config: Any, strategy: "SyncTenantStrategy", *, session: bool = False) -> None:
        super().__init__(strategy)
        self._config = config
        self._pool: Any = None
        self._bypass_hook: Any = None

    def _acquire(self) -> Any:
        self._pool = self._config.provide_pool()
        connection = self._pool.acquire()
        hook = self._config.driver_features.get("session_context_hook") if TenantContext.is_bypassed() else None
        if hook is not None:
            try:
                execute_session_checkout_sync(connection, hook)
            except BaseException as exc:
                try:
                    self._invalidate(connection)
                except BaseException as disposal_error:
                    exc.add_note(f"Tenant connection invalidation failed: {disposal_error!r}")
                raise
        self._bypass_hook = hook
        return connection

    def _release(self, connection: Any) -> None:
        hook, self._bypass_hook = self._bypass_hook, None
        if hook is not None:
            execute_session_release_sync(connection, hook)
        self._pool.release(connection)

    def _invalidate(self, connection: Any) -> None:
        self._pool.drop(connection)

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        connection.rollback()


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncTenantLease(AsyncTenantLease):
    """Wrap asynchronous Oracle pool hooks and dispose unverified sessions.

    Bypass leases skip the tenant strategy but still apply and release the
    configured ``session_context_hook``, matching non-tenant sessions.
    """

    __slots__ = ("_bypass_hook", "_config", "_pool")

    def __init__(self, config: Any, strategy: "AsyncTenantStrategy", *, session: bool = False) -> None:
        super().__init__(strategy)
        self._config = config
        self._pool: Any = None
        self._bypass_hook: Any = None

    async def _acquire(self) -> Any:
        self._pool = await self._config.provide_pool()
        connection = await self._pool.acquire()
        hook = self._config.driver_features.get("session_context_hook") if TenantContext.is_bypassed() else None
        if hook is not None:
            setup_error: BaseException | None = None
            try:
                await execute_session_checkout_async(connection, hook)
            except BaseException as exc:
                setup_error = exc
            if setup_error is not None:
                try:
                    await await_owned_release(self._invalidate(connection))
                except BaseException as disposal_error:
                    setup_error.add_note(f"Tenant connection invalidation failed: {disposal_error!r}")
                raise setup_error
        self._bypass_hook = hook
        return connection

    async def _release(self, connection: Any) -> None:
        hook, self._bypass_hook = self._bypass_hook, None
        if hook is not None:
            await execute_session_release_async(connection, hook)
        await self._pool.release(connection)

    async def _invalidate(self, connection: Any) -> None:
        await self._pool.drop(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        await connection.rollback()


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncVPDStrategy(OracleSyncIdentifierStrategy):
    """Apply a tenant through an administrator-provisioned trusted context package."""

    __slots__ = ("_clear_procedure", "_context_attribute", "_namespace", "_set_procedure")

    def __init__(
        self,
        *,
        set_procedure: object,
        clear_procedure: object,
        namespace: str,
        attribute: str,
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        self._set_procedure, self._clear_procedure = _validate_vpd_settings(
            set_procedure, clear_procedure, namespace, attribute
        )
        self._namespace = namespace
        self._context_attribute = attribute

    def checkout(self, connection: "OracleSyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Bind the identifier, set the trusted context, and verify both in one read."""
        self._apply_identifier(connection, payload)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"BEGIN {self._set_procedure}(:tenant_id); END;", {"tenant_id": payload.tenant_id})
                cursor.execute(_TENANT_STATE_QUERY, self._context_binds())
                row = cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be set"
            raise with_cause(TenantContextError(msg), exc)
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        if row[1] != payload.tenant_id:
            msg = "Oracle trusted tenant context verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    def checkin(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        """Clear the trusted context, then the identifier, and verify both in one read."""
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"BEGIN {self._clear_procedure}; END;")
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be cleared"
            raise with_cause(TenantCleanupError(msg), exc)
        self._clear_identifier(connection, state)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_TENANT_STATE_QUERY, self._context_binds())
                row = cursor.fetchone()
        except Exception as exc:
            msg = "Oracle tenant cleanup could not be verified"
            raise with_cause(TenantCleanupError(msg), exc)
        if row is None or row[1] is not None:
            msg = "Oracle trusted tenant context cleanup verification failed"
            raise TenantCleanupError(msg)
        if row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)

    def _context_binds(self) -> dict[str, str]:
        return {"namespace": self._namespace, "attribute": self._context_attribute}


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncVPDStrategy(OracleAsyncIdentifierStrategy):
    """Apply a trusted application context on an asynchronous Oracle connection."""

    __slots__ = ("_clear_procedure", "_context_attribute", "_namespace", "_set_procedure")

    def __init__(
        self,
        *,
        set_procedure: object,
        clear_procedure: object,
        namespace: str,
        attribute: str,
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        self._set_procedure, self._clear_procedure = _validate_vpd_settings(
            set_procedure, clear_procedure, namespace, attribute
        )
        self._namespace = namespace
        self._context_attribute = attribute

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Bind the identifier, set the trusted context, and verify both in one read."""
        await self._apply_identifier(connection, payload)
        try:
            with connection.cursor() as cursor:
                await cursor.execute(f"BEGIN {self._set_procedure}(:tenant_id); END;", {"tenant_id": payload.tenant_id})
                await cursor.execute(_TENANT_STATE_QUERY, self._context_binds())
                row = await cursor.fetchone()
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be set"
            raise with_cause(TenantContextError(msg), exc)
        if row is None or row[0] != payload.tenant_id:
            msg = "Oracle tenant identifier verification failed"
            raise TenantContextError(msg)
        if row[1] != payload.tenant_id:
            msg = "Oracle trusted tenant context verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    async def checkin(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        """Clear the trusted context, then the identifier, and verify both in one read."""
        try:
            with connection.cursor() as cursor:
                await cursor.execute(f"BEGIN {self._clear_procedure}; END;")
        except Exception as exc:
            msg = "Oracle trusted tenant context could not be cleared"
            raise with_cause(TenantCleanupError(msg), exc)
        await self._clear_identifier(connection, state)
        try:
            with connection.cursor() as cursor:
                await cursor.execute(_TENANT_STATE_QUERY, self._context_binds())
                row = await cursor.fetchone()
        except Exception as exc:
            msg = "Oracle tenant cleanup could not be verified"
            raise with_cause(TenantCleanupError(msg), exc)
        if row is None or row[1] is not None:
            msg = "Oracle trusted tenant context cleanup verification failed"
            raise TenantCleanupError(msg)
        if row[0] is not None:
            msg = "Oracle tenant identifier cleanup verification failed"
            raise TenantCleanupError(msg)

    def _context_binds(self) -> dict[str, str]:
        return {"namespace": self._namespace, "attribute": self._context_attribute}


def _validate_vpd_procedure(value: object, label: str) -> str:
    if not isinstance(value, str):
        msg = f"Oracle tenant {label} must be an identifier"
        raise ImproperConfigurationError(msg)
    for segment in value.split("."):
        _validate_oracle_identifier(segment, label)
    return value


def _validate_vpd_settings(
    set_procedure: object, clear_procedure: object, namespace: str, attribute: str
) -> tuple[str, str]:
    set_procedure = _validate_vpd_procedure(set_procedure, "set_procedure")
    clear_procedure = _validate_vpd_procedure(clear_procedure, "clear_procedure")
    _validate_oracle_identifier(namespace, "namespace")
    _validate_oracle_identifier(attribute, "attribute")
    return set_procedure, clear_procedure
