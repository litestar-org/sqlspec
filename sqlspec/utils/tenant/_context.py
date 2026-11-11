"""Immutable tenant identities and explicit context scopes."""

from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, MutableMapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from inspect import isawaitable, iscoroutinefunction
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
    "TenantScope",
    "async_tenant_bypass",
    "async_tenant_scope",
    "coerce_tenant_payload",
    "shared_tenant_extractor",
    "single_membership_tenant_extractor",
    "tenant_context_var",
)

MAX_TENANT_ID_BYTES: Final = 256


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


tenant_context_var: ContextVar[TenantContextPayload | None] = ContextVar("sqlspec_tenant_context", default=None)


class TenantContext:
    """Manage tenant state in the current task or explicitly copied context."""

    __slots__ = ()

    @classmethod
    def get(cls) -> TenantContextPayload | None:
        """Return the current payload, including explicit bypass state."""
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
    def set(cls, payload: TenantContextPayload) -> Token[TenantContextPayload | None]: ...

    @overload
    @classmethod
    def set(cls, payload: None) -> Token[TenantContextPayload | None]: ...

    @classmethod
    def set(cls, payload: object) -> Token[TenantContextPayload | None]:
        """Set or clear the payload and return its restoration token.

        Raises:
            TenantContextError: The value is neither a payload nor None.
        """
        if payload is not None and not isinstance(payload, TenantContextPayload):
            msg = "Tenant context requires a TenantContextPayload or None"
            raise TenantContextError(msg)
        return tenant_context_var.set(payload)

    @classmethod
    def reset(cls, token: Token[TenantContextPayload | None]) -> None:
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
        self._token: Token[TenantContextPayload | None] | None = None

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
        self._token: Token[TenantContextPayload | None] | None = None

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


def async_tenant_scope(
    tenant_id: "str | UUID | TenantContextPayload", *, variables: Mapping[str, str] | None = None
) -> TenantScope:
    """Bind tenant state across awaits and restore it on cancellation.

    Args:
        tenant_id: Tenant identity or an existing immutable payload.
        variables: Session variables overriding values in an existing payload.

    Returns:
        An asynchronous context manager yielding the active payload.
    """
    return TenantContext.scope(tenant_id, variables=variables)


def async_tenant_bypass() -> TenantScope:
    """Bind explicit bypass state across awaits and restore it on exit.

    Returns:
        An asynchronous context manager yielding the bypass payload.
    """
    return TenantContext.bypass()


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


def shared_tenant_extractor(
    states: Iterable[Any], *, framework: str, synchronous: bool = False
) -> Callable[..., Any] | None:
    """Select the one tenant extractor shared by a framework plugin's injected configurations.

    Args:
        states: Per-configuration plugin states exposing ``disable_di``, ``tenant_extractor``,
            ``session_key`` and ``config``.
        framework: Framework name used in error messages.
        synchronous: Reject coroutine extractors.

    Returns:
        The shared extractor, or None when no configuration declares one.

    Raises:
        ImproperConfigurationError: A tenant configuration lacks an extractor, an extractor
            is not callable, or configurations declare different extractors.
    """
    selected: Callable[..., Any] | None = None
    for state in states:
        if state.disable_di:
            continue
        extractor = state.tenant_extractor
        if extractor is None:
            if state.config.driver_features.get("tenant_strategy") is not None:
                msg = f"{framework} session {state.session_key!r} requires tenant_extractor for tenant strategy"
                raise ImproperConfigurationError(msg)
            continue
        if not callable(extractor):
            msg = (
                f"{framework} tenant_extractor must be a synchronous callable"
                if synchronous
                else f"{framework} tenant_extractor must be callable"
            )
            raise ImproperConfigurationError(msg)
        if synchronous and (iscoroutinefunction(extractor) or iscoroutinefunction(type(extractor).__call__)):
            msg = f"{framework} tenant_extractor must be a synchronous callable"
            raise ImproperConfigurationError(msg)
        if selected is not None and selected is not extractor:
            msg = f"{framework} configurations must share the same tenant_extractor"
            raise ImproperConfigurationError(msg)
        selected = extractor
    return selected
