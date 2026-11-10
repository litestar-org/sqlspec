"""Immutable tenant identities and explicit context scopes."""

from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterator, Mapping, MutableMapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from inspect import isawaitable
from types import MappingProxyType
from typing import Any, Final, TypeAlias
from uuid import UUID

from sqlspec.exceptions import ImproperConfigurationError, TenantContextError

__all__ = (
    "MAX_TENANT_ID_BYTES",
    "SQLSpecTenantMiddleware",
    "TenantContext",
    "TenantContextPayload",
    "TenantExtractor",
    "TenantExtractorValue",
    "async_tenant_bypass",
    "async_tenant_scope",
    "coerce_tenant_payload",
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
        normalized_id = str(tenant_id) if isinstance(tenant_id, UUID) else tenant_id
        if not isinstance(normalized_id, str):
            msg = "Tenant identity must be a string or UUID"
            raise TenantContextError(msg)
        snapshot = dict(variables or {})
        if is_bypass:
            if normalized_id or snapshot:
                msg = "Tenant bypass cannot contain an identity or variables"
                raise TenantContextError(msg)
        else:
            try:
                identity_bytes = normalized_id.encode("utf-8")
            except UnicodeEncodeError as exc:
                msg = "Tenant identity must be valid UTF-8"
                raise TenantContextError(msg) from exc
            if not normalized_id.strip() or "\x00" in normalized_id or len(identity_bytes) > MAX_TENANT_ID_BYTES:
                msg = "Tenant identity must be nonblank, NUL-free, and at most 256 UTF-8 bytes"
                raise TenantContextError(msg)
        for key, value in snapshot.items():
            if not isinstance(key, str) or not isinstance(value, str) or "\x00" in key or "\x00" in value:
                msg = "Tenant variable keys and values must be NUL-free strings"
                raise TenantContextError(msg)
        object.__setattr__(self, "tenant_id", normalized_id)
        object.__setattr__(self, "variables", MappingProxyType(snapshot))
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

    @classmethod
    def set(cls, payload: TenantContextPayload | None) -> Token[TenantContextPayload | None]:
        """Set or clear the payload and return its restoration token."""
        if payload is not None and not isinstance(payload, TenantContextPayload):
            msg = "Tenant context requires a TenantContextPayload or None"
            raise TenantContextError(msg)
        return tenant_context_var.set(payload)

    @classmethod
    def reset(cls, token: Token[TenantContextPayload | None]) -> None:
        """Restore a token in its original context."""
        tenant_context_var.reset(token)

    @classmethod
    @contextmanager
    def scope(
        cls, tenant_id: str | UUID | TenantContextPayload, *, variables: Mapping[str, str] | None = None
    ) -> Iterator[TenantContextPayload]:
        """Bind a validated payload and restore the caller on every exit.

        Args:
            tenant_id: Tenant identity or an existing immutable payload.
            variables: Session variables overriding values in an existing payload.

        Yields:
            The active tenant payload.
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
        token = cls.set(payload)
        try:
            yield payload
        finally:
            cls.reset(token)

    @classmethod
    @contextmanager
    def bypass(cls) -> Iterator[TenantContextPayload]:
        """Bind explicit bypass state for the duration of the scope.

        Yields:
            The active bypass payload.
        """
        with cls.scope(TenantContextPayload.bypass()) as payload:
            yield payload


@asynccontextmanager
async def async_tenant_scope(
    tenant_id: str | UUID | TenantContextPayload, *, variables: Mapping[str, str] | None = None
) -> AsyncIterator[TenantContextPayload]:
    """Bind tenant state across awaits and restore it on cancellation.

    Args:
        tenant_id: Tenant identity or an existing immutable payload.
        variables: Session variables overriding values in an existing payload.

    Yields:
        The active tenant payload.
    """
    with TenantContext.scope(tenant_id, variables=variables) as payload:
        yield payload


@asynccontextmanager
async def async_tenant_bypass() -> AsyncIterator[TenantContextPayload]:
    """Bind explicit bypass state across awaits and restore it on exit.

    Yields:
        The active bypass payload.
    """
    with TenantContext.bypass() as payload:
        yield payload


TenantExtractorValue: TypeAlias = TenantContextPayload | str | UUID | None
TenantExtractor: TypeAlias = Callable[
    [MutableMapping[str, Any]], TenantExtractorValue | Awaitable[TenantExtractorValue]
]


def coerce_tenant_payload(value: TenantExtractorValue) -> TenantContextPayload | None:
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
