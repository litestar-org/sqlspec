"""Verified Oracle tenant identifiers and administrator-provisioned contexts."""

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal, TypeAlias, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.oracledb._typing import oracledb_create_pipeline as create_oracle_pipeline
from sqlspec.adapters.oracledb.core import (
    OracleAcquireOptions,
    OracleEndUserSecurityConfig,
    OracleSessionContextConfig,
    _validate_oracle_identifier,
    apply_end_user_security_checkout,
    apply_oracle_transaction_priority,
    clear_end_user_security_release,
    clear_oracle_otel_trace_context,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
    extract_pipeline_single_row,
    forget_oracle_session_state,
    has_callable_alter_session_fields,
    prepare_session_checkout_pipeline,
    prepare_session_release_pipeline,
    synthesize_session_state_tag,
)
from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease, await_owned_disposal
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload
from sqlspec.utils.type_guards import has_pipeline_capability

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
    from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy

__all__ = (
    "ORACLE_DEEPSEC_IDENTITY_PROVIDERS",
    "ORACLE_TENANT_IDENTIFIER_MAX_BYTES",
    "OracleAsyncClientContextStrategy",
    "OracleAsyncEndUserSecurityStrategy",
    "OracleAsyncIdentifierStrategy",
    "OracleAsyncTenantLease",
    "OracleAsyncVPDStrategy",
    "OracleDeepSecIdentityProvider",
    "OracleShardResolver",
    "OracleSyncClientContextStrategy",
    "OracleSyncEndUserSecurityStrategy",
    "OracleSyncIdentifierStrategy",
    "OracleSyncTenantLease",
    "OracleSyncVPDStrategy",
    "bind_oracle_tenant_hook",
    "compose_tenant_session_hook",
    "resolve_tenant_shard_keys",
    "tenant_client_identifier",
    "validate_oracle_tenant_hook",
    "validate_oracle_tenant_shard_features",
)

ORACLE_TENANT_IDENTIFIER_MAX_BYTES: Final = 64
_IDENTIFIER_QUERY: Final = "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL"
_TENANT_STATE_QUERY: Final = (
    "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER'), SYS_CONTEXT(:namespace, :attribute) FROM DUAL"
)


OracleShardResolver: TypeAlias = Callable[[TenantContextPayload], tuple[Sequence[Any] | None, Sequence[Any] | None]]
OracleDeepSecIdentityProvider: TypeAlias = Literal["database", "oci", "entra", "obo"]
ORACLE_DEEPSEC_IDENTITY_PROVIDERS: Final[frozenset[str]] = frozenset({"database", "oci", "entra", "obo"})


def _validate_variable_keys(keys: Sequence[str]) -> tuple[str, ...]:
    if isinstance(keys, (str, bytes)):
        msg = "Oracle tenant variable_keys must be a sequence of identifiers"
        raise ImproperConfigurationError(msg)
    for key in keys:
        _validate_oracle_identifier(key, "variable key")
    return tuple(keys)


def _tenant_variables(payload: TenantContextPayload, keys: Sequence[str], *, secrets: bool = False) -> dict[str, str]:
    permitted = set(keys)
    if secrets:
        permitted.update({"end_user_token", "end_user_key"})
    undeclared = set(payload.variables) - permitted
    if undeclared:
        msg = f"Undeclared Oracle tenant variables: {', '.join(sorted(undeclared))}"
        raise TenantContextError(msg)
    return {key: str(payload.variables[key]) for key in keys if key in payload.variables}


def validate_oracle_tenant_shard_features(driver_features: Mapping[str, Any]) -> None:
    """Reject ambiguous or malformed tenant shard-routing settings."""
    resolver = driver_features.get("shard_resolver")
    if resolver is not None and not callable(resolver):
        msg = "Oracle shard_resolver must be callable"
        raise ImproperConfigurationError(msg)
    variable = driver_features.get("sharding_key_variable")
    super_variable = driver_features.get("super_sharding_key_variable")
    for value in (variable, super_variable):
        if value is not None and (not isinstance(value, str) or not value):
            msg = "Oracle shard variable names must be nonempty strings"
            raise ImproperConfigurationError(msg)
    if sum(bool(value) for value in (resolver, variable, driver_features.get("use_tenant_id_as_sharding_key"))) > 1:
        msg = "Choose one Oracle tenant shard resolver, shard variable, or tenant identity"
        raise ImproperConfigurationError(msg)
    if super_variable and not (resolver or variable or driver_features.get("use_tenant_id_as_sharding_key")):
        msg = "Oracle super-shard variable requires a primary shard source"
        raise ImproperConfigurationError(msg)


def resolve_tenant_shard_keys(
    payload: TenantContextPayload, driver_features: Mapping[str, Any], options: OracleAcquireOptions | None = None
) -> OracleAcquireOptions | None:
    """Merge tenant shard-routing sources before a pooled socket is acquired."""
    options = options or OracleAcquireOptions()
    if payload.is_bypass or options.sharding_key is not None:
        return options
    shard: Any = None
    super_shard: Any = None
    resolver = driver_features.get("shard_resolver")
    if resolver is not None:
        shard, super_shard = resolver(payload)
    elif driver_features.get("use_tenant_id_as_sharding_key"):
        shard = [payload.tenant_id]
    for feature, is_super in (("sharding_key_variable", False), ("super_sharding_key_variable", True)):
        key = driver_features.get(feature)
        if key is not None:
            if key not in payload.variables:
                msg = f"Missing Oracle tenant shard variable: {key}"
                raise TenantContextError(msg)
            value = payload.variables[key]
            normalized = list(value) if isinstance(value, (list, tuple)) else [value]
            if is_super:
                super_shard = normalized
            else:
                shard = normalized
    if shard is None and super_shard is None:
        return options
    return OracleAcquireOptions(
        sharding_key=shard,
        super_sharding_key=super_shard,
        tag=options.tag,
        match_any_tag=options.match_any_tag,
        cclass=options.cclass,
        purity=options.purity,
        transaction_priority=options.transaction_priority,
    )


def _skip_tenant_alter(connection: Any, hook: OracleSessionContextConfig) -> bool:
    if getattr(connection, "thin", None) is not False or has_callable_alter_session_fields(hook):
        return False
    tag = synthesize_session_state_tag(hook)
    return tag is not None and connection.tag == tag


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncClientContextStrategy:
    """Bind tenant attributes through the zero-round-trip CLIENTCONTEXT API."""

    __slots__ = ("_tenant_attribute", "_variable_keys", "source_hook")

    def __init__(
        self,
        attribute: str = "TENANT_ID",
        *,
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        _validate_oracle_identifier(attribute, "attribute")
        self._tenant_attribute = attribute
        self._variable_keys = _validate_variable_keys(variable_keys)
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook

    def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        """Attach tenant attributes and apply the captured session hook."""
        variables = _tenant_variables(payload, self._variable_keys)
        variables[self._tenant_attribute] = payload.tenant_id
        with TenantContext.bind(payload):
            if self.source_hook is not None:
                execute_session_checkout_sync(connection, self.source_hook)
            cast("Any", connection).set_app_context("CLIENTCONTEXT", **variables)
        return TenantLeaseState(payload)

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Clear tenant attributes before returning the socket to its pool."""
        cast("Any", connection).clear_app_context("CLIENTCONTEXT")
        with TenantContext.bind(state.payload):
            if self.source_hook is not None:
                execute_session_release_sync(connection, self.source_hook)


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncClientContextStrategy:
    """Bind tenant attributes through asynchronous CLIENTCONTEXT piggybacking."""

    __slots__ = ("_tenant_attribute", "_variable_keys", "source_hook")

    def __init__(
        self,
        attribute: str = "TENANT_ID",
        *,
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        _validate_oracle_identifier(attribute, "attribute")
        self._tenant_attribute = attribute
        self._variable_keys = _validate_variable_keys(variable_keys)
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook

    async def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        """Attach tenant attributes and apply the captured session hook."""
        variables = _tenant_variables(payload, self._variable_keys)
        variables[self._tenant_attribute] = payload.tenant_id
        with TenantContext.bind(payload):
            if self.source_hook is not None:
                await execute_session_checkout_async(connection, self.source_hook)
            cast("Any", connection).set_app_context("CLIENTCONTEXT", **variables)
        return TenantLeaseState(payload)

    async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Clear tenant attributes before returning the socket to its pool."""
        cast("Any", connection).clear_app_context("CLIENTCONTEXT")
        with TenantContext.bind(state.payload):
            if self.source_hook is not None:
                await execute_session_release_async(connection, self.source_hook)


def _deepsec_tenant_config(strategy: Any, payload: TenantContextPayload) -> OracleEndUserSecurityConfig:
    variables = _tenant_variables(payload, strategy._variable_keys, secrets=True)
    if strategy._tenant_attribute is not None:
        variables[strategy._tenant_attribute] = payload.tenant_id
    identity: Any = payload.variables.get("end_user_token")
    if strategy._identity_provider in {"oci", "entra", "obo"} and not identity:
        msg = "Oracle token identity providers require the end_user_token tenant variable"
        raise TenantContextError(msg)
    if identity is None:
        identity = (payload.tenant_id, payload.variables.get("end_user_key"))
    roles = strategy._data_roles(payload) if callable(strategy._data_roles) else strategy._data_roles
    return OracleEndUserSecurityConfig(
        strategy._database_access_token, end_user_identity=identity, data_roles=roles, attributes=variables
    )


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncEndUserSecurityStrategy:
    """Bind captured tenant identity to a native Deep Data Security context."""

    __slots__ = (
        "_data_roles",
        "_database_access_token",
        "_identity_provider",
        "_tenant_attribute",
        "_variable_keys",
        "source_hook",
    )

    def __init__(
        self,
        database_access_token: str | Callable[[], str],
        *,
        identity_provider: OracleDeepSecIdentityProvider | str | None = None,
        data_roles: Sequence[str] | Callable[[TenantContextPayload], Sequence[str] | None] | None = None,
        attribute: str | None = "TENANT_ID",
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        OracleEndUserSecurityConfig(database_access_token)
        if identity_provider is not None and identity_provider not in ORACLE_DEEPSEC_IDENTITY_PROVIDERS:
            msg = "Invalid Oracle DeepSec identity_provider"
            raise ImproperConfigurationError(msg)
        if attribute is not None:
            _validate_oracle_identifier(attribute, "attribute")
        self._tenant_attribute = attribute
        self._data_roles = data_roles
        self._database_access_token = database_access_token
        self._identity_provider = identity_provider
        self._variable_keys = _validate_variable_keys(variable_keys)
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook

    def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        """Attach a security context without a SQL cursor or thread-local secrets."""
        security = _deepsec_tenant_config(self, payload)
        with TenantContext.bind(payload):
            if self.source_hook is not None:
                execute_session_checkout_sync(connection, self.source_hook)
            apply_end_user_security_checkout(connection, security)
        return TenantLeaseState(payload)

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Clear credentials regardless of the source hook's reset policy."""
        clear_end_user_security_release(connection)
        with TenantContext.bind(state.payload):
            if self.source_hook is not None:
                execute_session_release_sync(connection, self.source_hook)


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncEndUserSecurityStrategy:
    """Bind captured tenant identity to a native Deep Data Security context."""

    __slots__ = (
        "_data_roles",
        "_database_access_token",
        "_identity_provider",
        "_tenant_attribute",
        "_variable_keys",
        "source_hook",
    )

    def __init__(
        self,
        database_access_token: str | Callable[[], str],
        *,
        identity_provider: OracleDeepSecIdentityProvider | str | None = None,
        data_roles: Sequence[str] | Callable[[TenantContextPayload], Sequence[str] | None] | None = None,
        attribute: str | None = "TENANT_ID",
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        OracleEndUserSecurityConfig(database_access_token)
        if identity_provider is not None and identity_provider not in ORACLE_DEEPSEC_IDENTITY_PROVIDERS:
            msg = "Invalid Oracle DeepSec identity_provider"
            raise ImproperConfigurationError(msg)
        if attribute is not None:
            _validate_oracle_identifier(attribute, "attribute")
        self._tenant_attribute = attribute
        self._data_roles = data_roles
        self._database_access_token = database_access_token
        self._identity_provider = identity_provider
        self._variable_keys = _validate_variable_keys(variable_keys)
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook

    async def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        """Attach a security context without a SQL cursor or thread-local secrets."""
        security = _deepsec_tenant_config(self, payload)
        with TenantContext.bind(payload):
            if self.source_hook is not None:
                await execute_session_checkout_async(connection, self.source_hook)
            apply_end_user_security_checkout(connection, security)
        return TenantLeaseState(payload)

    async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Clear credentials regardless of the source hook's reset policy."""
        clear_end_user_security_release(connection)
        with TenantContext.bind(state.payload):
            if self.source_hook is not None:
                await execute_session_release_async(connection, self.source_hook)


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
        standby_max_data_delay=source.standby_max_data_delay,
        adg_redirect_dml=source.adg_redirect_dml,
        sync_with_primary=source.sync_with_primary,
        clientinfo=source.clientinfo,
        current_schema=source.current_schema,
        dbop=source.dbop,
        end_user_security=source.end_user_security,
        transaction_priority=source.transaction_priority,
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

    __slots__ = ("_hook", "_variable_keys", "source_hook")

    def __init__(
        self, hook: OracleSessionContextConfig | Mapping[str, Any] | None = None, *, variable_keys: Sequence[str] = ()
    ) -> None:
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
        self._hook = compose_tenant_session_hook(self.source_hook)
        self._variable_keys = _validate_variable_keys(variable_keys)

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
            variables = _tenant_variables(payload, self._variable_keys)
            if variables:
                cast("Any", connection).set_app_context("CLIENTCONTEXT", **variables)
            execute_session_checkout_sync(
                connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
            )

    def _clear_identifier(self, connection: "OracleSyncConnection", state: TenantLeaseState) -> None:
        with TenantContext.bind(state.payload):
            if self._variable_keys:
                cast("Any", connection).clear_app_context("CLIENTCONTEXT")
            execute_session_release_sync(connection, self._hook)


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncIdentifierStrategy:
    """Apply and verify a client identifier on an asynchronous Oracle connection."""

    __slots__ = ("_hook", "_variable_keys", "source_hook")

    def __init__(
        self, hook: OracleSessionContextConfig | Mapping[str, Any] | None = None, *, variable_keys: Sequence[str] = ()
    ) -> None:
        self.source_hook = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
        self._hook = compose_tenant_session_hook(self.source_hook)
        self._variable_keys = _validate_variable_keys(variable_keys)

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply the captured payload and verify the identifier on the server."""
        if has_pipeline_capability(connection):
            variables = _tenant_variables(payload, self._variable_keys)
            with TenantContext.bind(payload):
                tenant_client_identifier()
                sql, binds = prepare_session_checkout_pipeline(
                    connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
                )
                if variables:
                    cast("Any", connection).set_app_context("CLIENTCONTEXT", **variables)
                pipeline = create_oracle_pipeline()
                if sql is not None:
                    pipeline.add_execute(sql, binds)
                pipeline.add_fetchone(_IDENTIFIER_QUERY)
                try:
                    results = await connection.run_pipeline(pipeline, continue_on_error=False)
                    row = extract_pipeline_single_row(results[-1])
                except Exception as exc:
                    msg = "Oracle tenant identifier pipeline checkout failed"
                    raise with_cause(TenantContextError(msg), exc)
        else:
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
        if has_pipeline_capability(connection):
            with TenantContext.bind(state.payload):
                sql, binds, _ = prepare_session_release_pipeline(connection, self._hook, defer_identifier_reset=False)
                if self._variable_keys:
                    cast("Any", connection).clear_app_context("CLIENTCONTEXT")
                pipeline = create_oracle_pipeline()
                if sql is not None:
                    pipeline.add_execute(sql, binds)
                pipeline.add_fetchone(_IDENTIFIER_QUERY)
                try:
                    results = await connection.run_pipeline(pipeline, continue_on_error=False)
                    row = extract_pipeline_single_row(results[-1])
                except Exception as exc:
                    msg = "Oracle tenant identifier pipeline cleanup failed"
                    raise with_cause(TenantCleanupError(msg), exc)
        else:
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
            variables = _tenant_variables(payload, self._variable_keys)
            if variables:
                cast("Any", connection).set_app_context("CLIENTCONTEXT", **variables)
            await execute_session_checkout_async(
                connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
            )

    async def _clear_identifier(self, connection: "OracleAsyncConnection", state: TenantLeaseState) -> None:
        with TenantContext.bind(state.payload):
            if self._variable_keys:
                cast("Any", connection).clear_app_context("CLIENTCONTEXT")
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
    """Attach hooks to hookless built-ins while preserving every strategy option."""
    normalized = OracleSessionContextConfig(**hook) if isinstance(hook, Mapping) else hook
    if strategy is None or normalized is None:
        return strategy
    if getattr(strategy, "source_hook", None) is not None:
        validate_oracle_tenant_hook(strategy, normalized)
        return strategy
    if type(strategy) in (OracleSyncVPDStrategy, OracleAsyncVPDStrategy):
        return type(strategy)(
            set_procedure=strategy._set_procedure,
            clear_procedure=strategy._clear_procedure,
            namespace=strategy._namespace,
            attribute=strategy._context_attribute,
            variable_keys=strategy._vpd_variable_keys,
            hook=normalized,
        )
    if type(strategy) in (OracleSyncIdentifierStrategy, OracleAsyncIdentifierStrategy):
        return type(strategy)(normalized, variable_keys=strategy._variable_keys)
    if type(strategy) in (OracleSyncClientContextStrategy, OracleAsyncClientContextStrategy):
        return type(strategy)(strategy._tenant_attribute, variable_keys=strategy._variable_keys, hook=normalized)
    if type(strategy) in (OracleSyncEndUserSecurityStrategy, OracleAsyncEndUserSecurityStrategy):
        return type(strategy)(
            strategy._database_access_token,
            identity_provider=strategy._identity_provider,
            data_roles=strategy._data_roles,
            attribute=strategy._tenant_attribute,
            variable_keys=strategy._variable_keys,
            hook=normalized,
        )
    validate_oracle_tenant_hook(strategy, normalized)
    return strategy


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncTenantLease(SyncTenantLease):
    """Wrap Oracle pool hooks with verified tenant cleanup and physical drop.

    Bypass leases skip the tenant strategy but still apply and release the
    configured ``session_context_hook``, matching non-tenant sessions.
    """

    __slots__ = ("_acquire_options", "_bypass_hook", "_config", "_pool", "_priority_baseline", "_release_tag")

    def __init__(
        self, config: Any, strategy: "SyncTenantStrategy", acquire_options: OracleAcquireOptions | None = None
    ) -> None:
        super().__init__(strategy)
        self._config = config
        self._pool: Any = None
        self._bypass_hook: Any = None
        self._acquire_options = acquire_options
        self._release_tag: str | None = None
        self._priority_baseline: Any = None

    def _acquire(self) -> Any:
        payload = TenantContext.require(self.tenant_key)
        options = resolve_tenant_shard_keys(payload, self._config.driver_features, self._acquire_options)
        self._pool = self._config.provide_pool()
        kwargs = self._config._build_pool_acquire_kwargs(
            options, pool=self._pool, session_hook=self._config.driver_features.get("session_context_hook")
        )
        connection = self._pool.acquire(**kwargs)
        self._release_tag = kwargs.get("tag") if getattr(self._pool, "thin", None) is False else None
        priority = options.transaction_priority if options is not None else None
        if priority is None:
            priority = self._config.connection_config.get("transaction_priority")
        if priority is not None:
            self._priority_baseline = getattr(connection, "transaction_priority", None)
            try:
                apply_oracle_transaction_priority(connection, priority)
            except BaseException:
                self._invalidate(connection)
                raise
        bypass = payload.is_bypass
        hook = self._config.driver_features.get("session_context_hook") if bypass else None
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
            with TenantContext.bind(TenantContextPayload.bypass()):
                execute_session_release_sync(connection, hook)
        if self._priority_baseline is not None:
            apply_oracle_transaction_priority(connection, self._priority_baseline)
        clear_oracle_otel_trace_context(connection)
        if getattr(connection, "thin", None) is False and self._release_tag is not None:
            self._pool.release(connection, tag=self._release_tag)
        else:
            self._pool.release(connection)

    def _invalidate(self, connection: Any) -> None:
        forget_oracle_session_state(connection)
        self._pool.drop(connection)

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        connection.rollback()


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleAsyncTenantLease(AsyncTenantLease):
    """Wrap asynchronous Oracle pool hooks and dispose unverified sessions.

    Bypass leases skip the tenant strategy but still apply and release the
    configured ``session_context_hook``, matching non-tenant sessions.
    """

    __slots__ = ("_acquire_options", "_bypass_hook", "_config", "_pool", "_priority_baseline", "_release_tag")

    def __init__(
        self, config: Any, strategy: "AsyncTenantStrategy", acquire_options: OracleAcquireOptions | None = None
    ) -> None:
        super().__init__(strategy)
        self._config = config
        self._pool: Any = None
        self._bypass_hook: Any = None
        self._acquire_options = acquire_options
        self._release_tag: str | None = None
        self._priority_baseline: Any = None

    async def _acquire(self) -> Any:
        payload = await TenantContext.arequire(self.tenant_key)
        options = resolve_tenant_shard_keys(payload, self._config.driver_features, self._acquire_options)
        self._pool = await self._config.provide_pool()
        kwargs = self._config._build_pool_acquire_kwargs(
            options, pool=self._pool, session_hook=self._config.driver_features.get("session_context_hook")
        )
        connection = await self._pool.acquire(**kwargs)
        self._release_tag = kwargs.get("tag") if getattr(self._pool, "thin", None) is False else None
        priority = options.transaction_priority if options is not None else None
        if priority is None:
            priority = self._config.connection_config.get("transaction_priority")
        if priority is not None:
            self._priority_baseline = getattr(connection, "transaction_priority", None)
            try:
                apply_oracle_transaction_priority(connection, priority)
            except BaseException:
                await await_owned_disposal(self._invalidate(connection))
                raise
        bypass = payload.is_bypass
        hook = self._config.driver_features.get("session_context_hook") if bypass else None
        if hook is not None:
            setup_error: BaseException | None = None
            try:
                await execute_session_checkout_async(connection, hook)
            except BaseException as exc:
                setup_error = exc
            if setup_error is not None:
                disposal_error = await await_owned_disposal(self._invalidate(connection))
                if disposal_error is not None:
                    setup_error.add_note(f"Tenant connection invalidation failed: {disposal_error!r}")
                raise setup_error
        self._bypass_hook = hook
        return connection

    async def _release(self, connection: Any) -> None:
        hook, self._bypass_hook = self._bypass_hook, None
        if hook is not None:
            with TenantContext.bind(TenantContextPayload.bypass()):
                await execute_session_release_async(connection, hook)
        if self._priority_baseline is not None:
            apply_oracle_transaction_priority(connection, self._priority_baseline)
        clear_oracle_otel_trace_context(connection)
        if getattr(connection, "thin", None) is False and self._release_tag is not None:
            await self._pool.release(connection, tag=self._release_tag)
        else:
            await self._pool.release(connection)

    async def _invalidate(self, connection: Any) -> None:
        forget_oracle_session_state(connection)
        await self._pool.drop(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        await connection.rollback()


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleSyncVPDStrategy(OracleSyncIdentifierStrategy):
    """Apply a tenant through an administrator-provisioned trusted context package."""

    __slots__ = ("_clear_procedure", "_context_attribute", "_namespace", "_set_procedure", "_vpd_variable_keys")

    def __init__(
        self,
        *,
        set_procedure: object,
        clear_procedure: object,
        namespace: str,
        attribute: str,
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        self._set_procedure, self._clear_procedure = _validate_vpd_settings(
            set_procedure, clear_procedure, namespace, attribute
        )
        self._namespace = namespace
        self._context_attribute = attribute
        self._vpd_variable_keys = _validate_variable_keys(variable_keys)

    def checkout(self, connection: "OracleSyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Bind the identifier, set the trusted context, and verify both in one read."""
        variables = _tenant_variables(payload, self._vpd_variable_keys)
        with TenantContext.bind(payload):
            execute_session_checkout_sync(
                connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
            )
        binds = {
            "tenant_id": payload.tenant_id,
            **{f"var_{idx}": variables[key] for idx, key in enumerate(self._vpd_variable_keys) if key in variables},
        }
        placeholders = ", ".join(f":{key}" for key in binds)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"BEGIN {self._set_procedure}({placeholders}); END;", binds)
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

    __slots__ = ("_clear_procedure", "_context_attribute", "_namespace", "_set_procedure", "_vpd_variable_keys")

    def __init__(
        self,
        *,
        set_procedure: object,
        clear_procedure: object,
        namespace: str,
        attribute: str,
        variable_keys: Sequence[str] = (),
        hook: OracleSessionContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(hook)
        self._set_procedure, self._clear_procedure = _validate_vpd_settings(
            set_procedure, clear_procedure, namespace, attribute
        )
        self._namespace = namespace
        self._context_attribute = attribute
        self._vpd_variable_keys = _validate_variable_keys(variable_keys)

    async def checkout(self, connection: "OracleAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Bind the identifier, set the trusted context, and verify both in one read."""
        variables = _tenant_variables(payload, self._vpd_variable_keys)
        binds = {
            "tenant_id": payload.tenant_id,
            **{f"var_{idx}": variables[key] for idx, key in enumerate(self._vpd_variable_keys) if key in variables},
        }
        placeholders = ", ".join(f":{key}" for key in binds)
        if has_pipeline_capability(connection):
            with TenantContext.bind(payload):
                tenant_client_identifier()
                sql, hook_binds = prepare_session_checkout_pipeline(
                    connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
                )
                pipeline = create_oracle_pipeline()
                if sql is not None:
                    pipeline.add_execute(sql, hook_binds)
                pipeline.add_execute(f"BEGIN {self._set_procedure}({placeholders}); END;", binds)
                pipeline.add_fetchone(_TENANT_STATE_QUERY, self._context_binds())
                try:
                    results = await connection.run_pipeline(pipeline, continue_on_error=False)
                    row = extract_pipeline_single_row(results[-1])
                except Exception as exc:
                    msg = "Oracle trusted tenant pipeline checkout failed"
                    raise with_cause(TenantContextError(msg), exc)
        else:
            with TenantContext.bind(payload):
                await execute_session_checkout_async(
                    connection, self._hook, skip_alter_session=_skip_tenant_alter(connection, self._hook)
                )
            try:
                with connection.cursor() as cursor:
                    await cursor.execute(f"BEGIN {self._set_procedure}({placeholders}); END;", binds)
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
        if has_pipeline_capability(connection):
            with TenantContext.bind(state.payload):
                sql, binds, deferred = prepare_session_release_pipeline(
                    connection, self._hook, defer_identifier_reset=True
                )
                pipeline = create_oracle_pipeline()
                pipeline.add_execute(f"BEGIN {self._clear_procedure}; END;")
                if sql is not None:
                    pipeline.add_execute(sql, binds)
                pipeline.add_fetchone(_TENANT_STATE_QUERY, self._context_binds())
                try:
                    results = await connection.run_pipeline(pipeline, continue_on_error=False)
                    row = extract_pipeline_single_row(results[-1])
                except Exception as exc:
                    msg = "Oracle trusted tenant pipeline cleanup failed"
                    raise with_cause(TenantCleanupError(msg), exc)
                finally:
                    if deferred is not None:
                        deferred()
        else:
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
