"""Immutable tenant identities and explicit context scopes."""

from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final
from uuid import UUID

from sqlspec.exceptions import TenantContextError

__all__ = (
    "MAX_TENANT_ID_BYTES",
    "TenantContext",
    "TenantContextPayload",
    "async_tenant_bypass",
    "async_tenant_scope",
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
        elif (
            not normalized_id.strip()
            or "\x00" in normalized_id
            or len(normalized_id.encode("utf-8")) > MAX_TENANT_ID_BYTES
        ):
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
