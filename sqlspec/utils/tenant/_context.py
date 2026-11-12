"""Immutable tenant identities and explicit context scopes."""

from collections.abc import Awaitable, Callable, Collection, Mapping, MutableMapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from inspect import isawaitable
from types import MappingProxyType
from typing import Any, Final, TypeAlias, cast, overload
from uuid import UUID

from sqlspec.exceptions import ImproperConfigurationError, TenantContextError

__all__ = (
    "MAX_TENANT_ID_BYTES",
    "SQLSpecTenantMiddleware",
    "TenantBinding",
    "TenantContext",
    "TenantContextPayload",
    "TenantExtractor",
    "TenantExtractorValue",
    "TenantRequestBinding",
    "TenantScope",
    "coerce_tenant_payload",
    "is_tenant_bypass_route",
    "single_membership_tenant_extractor",
    "tenant_bypass",
    "tenant_context_var",
)

MAX_TENANT_ID_BYTES: Final = 256
_TENANT_BYPASS_ATTR: Final = "__sqlspec_tenant_bypass__"
_MISSING_TENANT_MESSAGE: Final = "A tenant scope or explicit bypass is required before acquiring a connection"
_SYNC_RESOLUTION_MESSAGE: Final = (
    "This request resolves its tenant asynchronously; use await TenantContext.aget() / arequire(), "
    "an async session, or a synchronous tenant_extractor"
)


@dataclass(frozen=True, slots=True)
class TenantContextPayload:
    """Snapshot of a tenant identity and its session variables."""

    tenant_id: str = ""
    variables: Mapping[str, str] = field(default_factory=dict, hash=False)
    is_bypass: bool = False

    def __init__(
        self, tenant_id: str | UUID = "", variables: Mapping[str, str] | None = None, *, is_bypass: bool = False
    ) -> None:
        raw_identity = cast("object", tenant_id)
        identity = str(raw_identity) if isinstance(raw_identity, UUID) else raw_identity
        if not isinstance(identity, str):
            msg = "Tenant identity must be a string or UUID"
            raise TenantContextError(msg)
        normalized_id: str = identity
        snapshot = dict(cast("Mapping[object, object]", variables or {}))
        if is_bypass:
            if normalized_id or snapshot:
                msg = "Tenant bypass cannot contain an identity or variables"
                raise TenantContextError(msg)
        else:
            try:
                identity_bytes = normalized_id.encode("utf-8")
            except UnicodeEncodeError as exc:
                msg = "Tenant identity must be valid UTF-8"
                error = TenantContextError(msg)
                error.__cause__ = exc
                raise error
            if not normalized_id.strip() or "\x00" in normalized_id or len(identity_bytes) > MAX_TENANT_ID_BYTES:
                msg = "Tenant identity must be nonblank, NUL-free, and at most 256 UTF-8 bytes"
                raise TenantContextError(msg)
        for key, value in snapshot.items():
            if not isinstance(key, str) or not isinstance(value, str) or "\x00" in key or "\x00" in value:
                msg = "Tenant variable keys and values must be NUL-free strings"
                raise TenantContextError(msg)
        object.__setattr__(self, "tenant_id", normalized_id)
        object.__setattr__(self, "variables", MappingProxyType(cast("dict[str, str]", snapshot)))
        object.__setattr__(self, "is_bypass", is_bypass)

    @classmethod
    def bypass(cls) -> "TenantContextPayload":
        """Create an explicit bypass without tenant identity or variables."""
        return cls(is_bypass=True)


class TenantRequestBinding:
    """Resolve a framework request's tenant lazily, once per database configuration.

    A plugin binds one instance at request start. Nothing runs until a lease, a session, or
    ``TenantContext`` asks for the tenant; the request-specific resolver then runs once per
    configuration key and its result is memoized. A resolver that yields no tenant falls back to
    the ambient value captured when the request started. After ``freeze()`` the binding only
    answers from memoized results, so work that outlives the response never reads request state.
    """

    __slots__ = ("_ambient", "_frozen", "_keys", "_memo", "_missing", "_resolver")

    def __init__(
        self,
        resolver: Callable[[Any], object],
        *,
        keys: Sequence[Any],
        ambient: "TenantContextPayload | TenantRequestBinding | None" = None,
        missing: Callable[[Any], None] | None = None,
    ) -> None:
        """Configure request resolution.

        Args:
            resolver: Callable returning the route tenant for a configuration key as a payload,
                string, UUID, or None, or an awaitable of one.
            keys: Tenant-enabled configuration keys; the first is the default key.
            ambient: Tenant state bound outside the request, used when route resolution yields none.
            missing: Callable raising the framework's unauthenticated or forbidden response for a key.
        """
        self._resolver = resolver
        self._keys = tuple(keys)
        self._ambient = ambient
        self._missing = missing
        self._memo: dict[int, TenantContextPayload | None] = {}
        self._frozen = False

    @property
    def frozen(self) -> bool:
        """Return whether request state may no longer be read."""
        return self._frozen

    def _key(self, key: Any) -> Any:
        if key is None and self._keys:
            return self._keys[0]
        return key

    def _ambient_value(self, key: Any) -> TenantContextPayload | None:
        ambient = self._ambient
        if isinstance(ambient, TenantRequestBinding):
            return ambient.resolve(key)
        return ambient

    async def _ambient_value_async(self, key: Any) -> TenantContextPayload | None:
        ambient = self._ambient
        if isinstance(ambient, TenantRequestBinding):
            return await ambient.aresolve(key)
        return ambient

    def resolve(self, key: Any = None) -> TenantContextPayload | None:
        """Return the tenant for ``key``, resolving it synchronously on first use.

        Raises:
            ImproperConfigurationError: The request resolves its tenant asynchronously.
        """
        key = self._key(key)
        memo_key = id(key)
        if memo_key in self._memo:
            return self._memo[memo_key]
        if self._frozen:
            return self._ambient_value(key)
        raw = self._resolver(key)
        if isawaitable(raw):
            close = getattr(raw, "close", None)
            if callable(close):
                close()
            raise ImproperConfigurationError(_SYNC_RESOLUTION_MESSAGE)
        payload = coerce_tenant_payload(raw)
        if payload is None:
            payload = self._ambient_value(key)
        self._memo[memo_key] = payload
        return payload

    async def aresolve(self, key: Any = None) -> TenantContextPayload | None:
        """Return the tenant for ``key``, awaiting an asynchronous resolver on first use."""
        key = self._key(key)
        memo_key = id(key)
        if memo_key in self._memo:
            return self._memo[memo_key]
        if self._frozen:
            return await self._ambient_value_async(key)
        raw = self._resolver(key)
        if isawaitable(raw):
            raw = await raw
        payload = coerce_tenant_payload(raw)
        if payload is None:
            payload = await self._ambient_value_async(key)
        self._memo[memo_key] = payload
        return payload

    def _require_resolved(self, key: Any, payload: TenantContextPayload | None) -> TenantContextPayload:
        if payload is not None:
            return payload
        if not self._frozen and self._missing is not None:
            self._missing(self._key(key))
        raise TenantContextError(_MISSING_TENANT_MESSAGE)

    def require(self, key: Any = None) -> TenantContextPayload:
        """Return the tenant for ``key`` or reject the request.

        Raises:
            TenantContextError: No tenant resolved and no framework response applies.
        """
        return self._require_resolved(key, self.resolve(key))

    async def arequire(self, key: Any = None) -> TenantContextPayload:
        """Return the tenant for ``key`` or reject the request, awaiting asynchronous resolution.

        Raises:
            TenantContextError: No tenant resolved and no framework response applies.
        """
        return self._require_resolved(key, await self.aresolve(key))

    def freeze(self) -> None:
        """Resolve every pending key from request state, then stop reading it.

        Called when work is handed off past the response. Resolution failures leave the key
        without a tenant instead of failing the response.
        """
        for key in self._keys:
            if id(key) not in self._memo:
                try:
                    self.resolve(key)
                except Exception:
                    self._memo[id(key)] = None
        self._frozen = True

    async def afreeze(self) -> None:
        """Resolve every pending key from request state asynchronously, then stop reading it."""
        for key in self._keys:
            if id(key) not in self._memo:
                try:
                    await self.aresolve(key)
                except Exception:
                    self._memo[id(key)] = None
        self._frozen = True


tenant_context_var: ContextVar[TenantContextPayload | TenantRequestBinding | None] = ContextVar(
    "sqlspec_tenant_context", default=None
)


class TenantContext:
    """Manage tenant state in the current task or explicitly copied context."""

    __slots__ = ()

    @classmethod
    def get(cls) -> TenantContextPayload | None:
        """Return the current payload, including explicit bypass state.

        Inside a framework request this resolves the request tenant on first use.
        """
        value = tenant_context_var.get()
        if isinstance(value, TenantRequestBinding):
            return value.resolve()
        return value

    @classmethod
    async def aget(cls) -> TenantContextPayload | None:
        """Return the current payload, awaiting asynchronous request resolution."""
        value = tenant_context_var.get()
        if isinstance(value, TenantRequestBinding):
            return await value.aresolve()
        return value

    @classmethod
    def require(cls, key: Any = None) -> TenantContextPayload:
        """Return the current payload or fail.

        Args:
            key: Database configuration whose request tenant is required.

        Returns:
            The bound payload, including explicit bypass.

        Raises:
            TenantContextError: No tenant is bound outside a framework request.
        """
        value = tenant_context_var.get()
        if isinstance(value, TenantRequestBinding):
            return value.require(key)
        if value is None:
            raise TenantContextError(_MISSING_TENANT_MESSAGE)
        return value

    @classmethod
    async def arequire(cls, key: Any = None) -> TenantContextPayload:
        """Return the current payload or fail, awaiting asynchronous request resolution.

        Args:
            key: Database configuration whose request tenant is required.

        Returns:
            The bound payload, including explicit bypass.

        Raises:
            TenantContextError: No tenant is bound outside a framework request.
        """
        value = tenant_context_var.get()
        if isinstance(value, TenantRequestBinding):
            return await value.arequire(key)
        if value is None:
            raise TenantContextError(_MISSING_TENANT_MESSAGE)
        return value

    @classmethod
    def bind_request(cls, binding: TenantRequestBinding) -> Token[TenantContextPayload | TenantRequestBinding | None]:
        """Bind a lazy request resolver and return its restoration token."""
        return tenant_context_var.set(binding)

    @classmethod
    def current_binding(cls) -> "TenantContextPayload | TenantRequestBinding | None":
        """Return the raw bound value without resolving a request binding."""
        return tenant_context_var.get()

    @classmethod
    def get_tenant_id(cls) -> str | None:
        """Return the tenant identity, or None when unset or bypassed."""
        payload = cls.get()
        return None if payload is None or payload.is_bypass else payload.tenant_id

    @classmethod
    def is_bypassed(cls) -> bool:
        """Return whether the caller explicitly bypassed tenant isolation."""
        payload = cls.get()
        return payload is not None and payload.is_bypass

    @overload
    @classmethod
    def set(cls, payload: TenantContextPayload) -> Token[TenantContextPayload | TenantRequestBinding | None]: ...

    @overload
    @classmethod
    def set(cls, payload: None) -> Token[TenantContextPayload | TenantRequestBinding | None]: ...

    @classmethod
    def set(cls, payload: object) -> Token[TenantContextPayload | TenantRequestBinding | None]:
        """Set or clear the payload and return its restoration token.

        Raises:
            TenantContextError: The value is neither a payload nor None.
        """
        if payload is not None and not isinstance(payload, TenantContextPayload):
            msg = "Tenant context requires a TenantContextPayload or None"
            raise TenantContextError(msg)
        return tenant_context_var.set(payload)

    @classmethod
    def reset(cls, token: Token[TenantContextPayload | TenantRequestBinding | None]) -> None:
        """Restore a token in its original context."""
        tenant_context_var.reset(token)

    @classmethod
    def scope(
        cls, tenant_id: "str | UUID | TenantContextPayload", *, variables: Mapping[str, str] | None = None
    ) -> "TenantScope":
        """Bind a validated payload and restore the caller on every exit.

        Args:
            tenant_id: Tenant identity or an existing immutable payload.
            variables: Session variables overriding values in an existing payload.

        Returns:
            A synchronous and asynchronous context manager yielding the active payload.
        """
        if isinstance(tenant_id, TenantContextPayload):
            payload = (
                tenant_id
                if variables is None
                else TenantContextPayload(
                    tenant_id.tenant_id, {**tenant_id.variables, **variables}, is_bypass=tenant_id.is_bypass
                )
            )
        else:
            payload = TenantContextPayload(tenant_id, variables)
        return TenantScope(payload)

    @classmethod
    def bypass(cls) -> "TenantScope":
        """Bind explicit bypass state for the duration of the scope.

        Returns:
            A synchronous and asynchronous context manager yielding the bypass payload.
        """
        return TenantScope(TenantContextPayload.bypass())

    @classmethod
    def bind(cls, payload: "TenantContextPayload | None") -> "TenantBinding":
        """Bind an already resolved payload, or its absence, around connection work.

        Args:
            payload: Resolved payload, or None to clear tenant state for the block.

        Returns:
            A synchronous context manager restoring the caller on every exit.
        """
        return TenantBinding(payload)


class TenantScope:
    """Bind one payload for a ``with`` or ``async with`` block."""

    __slots__ = ("_payload", "_token")

    def __init__(self, payload: TenantContextPayload) -> None:
        self._payload = payload
        self._token: Token[TenantContextPayload | TenantRequestBinding | None] | None = None

    def __enter__(self) -> TenantContextPayload:
        if self._token is not None:
            msg = "Tenant scope is already active"
            raise TenantContextError(msg)
        self._token = TenantContext.set(self._payload)
        return self._payload

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        token = self._token
        self._token = None
        if token is not None:
            TenantContext.reset(token)

    async def __aenter__(self) -> TenantContextPayload:
        return self.__enter__()

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)


class TenantBinding:
    """Bind a resolved payload or None for a ``with`` block."""

    __slots__ = ("_payload", "_token")

    def __init__(self, payload: TenantContextPayload | None) -> None:
        self._payload = payload
        self._token: Token[TenantContextPayload | TenantRequestBinding | None] | None = None

    def __enter__(self) -> None:
        if self._token is not None:
            msg = "Tenant binding is already active"
            raise TenantContextError(msg)
        self._token = TenantContext.set(self._payload)

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        token = self._token
        self._token = None
        if token is not None:
            TenantContext.reset(token)


TenantExtractorValue: TypeAlias = TenantContextPayload | str | UUID | None
TenantExtractor: TypeAlias = Callable[
    [MutableMapping[str, Any]], TenantExtractorValue | Awaitable[TenantExtractorValue]
]


def coerce_tenant_payload(value: object) -> TenantContextPayload | None:
    """Validate an application-authorized tenant extractor result.

    Args:
        value: Verified tenant identity, immutable payload, explicit bypass, or None.

    Returns:
        An immutable payload, preserving None and existing payloads.

    Raises:
        TenantContextError: The result is an unsupported type or invalid identity.
    """
    if value is None or isinstance(value, TenantContextPayload):
        return value
    if isinstance(value, (str, UUID)):
        return TenantContextPayload(value)
    msg = "Tenant extractors must return a payload, string, UUID, or None"
    raise TenantContextError(msg)


class SQLSpecTenantMiddleware:
    """Bind an application-authorized tenant for an entire ASGI invocation."""

    __slots__ = ("app", "extractor")

    def __init__(self, app: Any, *, extractor: TenantExtractor) -> None:
        """Configure the ASGI application and trusted tenant extractor."""
        if not callable(extractor):
            msg = "tenant extractor must be callable"
            raise ImproperConfigurationError(msg)
        self.app = app
        self.extractor = extractor

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        """Bind HTTP and WebSocket contexts and restore the caller on every exit."""
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        raw = self.extractor(scope)
        if isawaitable(raw):
            raw = await raw
        token = TenantContext.set(coerce_tenant_payload(raw))
        try:
            await self.app(scope, receive, send)
        finally:
            TenantContext.reset(token)


def tenant_bypass(handler: Any) -> Any:
    """Mark a route handler as an explicit tenant bypass.

    Framework plugins resolve a bypass payload for marked routes, so public routes such as
    login or health checks can use tenant-enabled sessions without a tenant.

    Args:
        handler: Route function or handler object.

    Returns:
        The same handler.
    """
    setattr(handler, _TENANT_BYPASS_ATTR, True)
    return handler


def is_tenant_bypass_route(handler: Any) -> bool:
    """Return whether ``tenant_bypass`` marked a route handler or its wrapped function."""
    if handler is None:
        return False
    if getattr(handler, _TENANT_BYPASS_ATTR, False) is True:
        return True
    for attribute in ("fn", "__wrapped__", "func", "endpoint"):
        inner = getattr(handler, attribute, None)
        if inner is not None and inner is not handler and getattr(inner, _TENANT_BYPASS_ATTR, False) is True:
            return True
    return False


def single_membership_tenant_extractor(
    memberships: Callable[[MutableMapping[str, Any]], Collection[str]],
) -> TenantExtractor:
    """Select a tenant only when the application reports exactly one membership.

    Args:
        memberships: Trusted accessor for memberships verified by the application.

    Returns:
        An extractor that refuses to choose between multiple memberships.
    """

    def extract(scope: MutableMapping[str, Any]) -> TenantContextPayload | None:
        values = memberships(scope)
        return TenantContextPayload(next(iter(values))) if len(values) == 1 else None

    return extract
