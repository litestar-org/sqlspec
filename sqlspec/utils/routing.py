"""Request-scoped routing directives and framework-independent ASGI middleware."""

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from contextvars import ContextVar, Token
from dataclasses import KW_ONLY, dataclass
from http.cookies import CookieError, SimpleCookie
from inspect import isawaitable
from math import isfinite
from types import MappingProxyType, TracebackType
from typing import Any, Final, Literal, TypeAlias, final

from sqlspec.exceptions import RoutingContextError
from sqlspec.utils.logging import get_logger

__all__ = (
    "DEFAULT_BIND_KEY",
    "RoutingContext",
    "RoutingContextPayload",
    "RoutingEntry",
    "RoutingMode",
    "RoutingScope",
    "SQLSpecRoutingMiddleware",
)

RoutingMode: TypeAlias = Literal["auto", "primary", "replica"]
DEFAULT_BIND_KEY: Final[str] = "__default__"
_ROUTING_VAR: ContextVar["RoutingContextPayload | None"] = ContextVar("sqlspec_routing_context", default=None)


def _normalize_bind_key(bind_key: str | None) -> str:
    return bind_key or DEFAULT_BIND_KEY


@final
@dataclass(frozen=True, slots=True)
class RoutingEntry:
    """Immutable routing directive for a logical database bind.

    Args:
        mode: Requested routing mode.
        bind_group: Optional replica group name.
        causal_token: Adapter-specific causal consistency token.
        max_staleness_seconds: Maximum configured replica staleness.
        pinned_by_write: Whether this directive follows a write.
    """

    mode: RoutingMode = "auto"
    _: KW_ONLY
    bind_group: str | None = None
    causal_token: str | None = None
    max_staleness_seconds: float | None = None
    pinned_by_write: bool = False

    def __post_init__(self) -> None:
        if self.mode not in {"auto", "primary", "replica"}:
            msg = f"Invalid routing mode {self.mode!r}"
            raise RoutingContextError(msg)
        if self.bind_group == "":
            msg = "bind_group must be a non-empty string when provided"
            raise RoutingContextError(msg)
        if self.mode == "primary" and self.bind_group is not None and not self.pinned_by_write:
            msg = "bind_group cannot be combined with mode='primary'"
            raise RoutingContextError(msg)
        if self.max_staleness_seconds is not None and (
            not isfinite(self.max_staleness_seconds) or self.max_staleness_seconds < 0
        ):
            msg = "max_staleness_seconds must be finite and >= 0"
            raise RoutingContextError(msg)


@final
class RoutingContextPayload:
    """Immutable snapshot of routing directives indexed by logical bind key."""

    __slots__ = ("_entries",)

    def __init__(self, entries: Mapping[str, RoutingEntry] | None = None) -> None:
        """Copy and freeze the supplied routing directives."""
        self._entries = MappingProxyType(dict(entries or {}))

    @property
    def entries(self) -> Mapping[str, RoutingEntry]:
        """Return the immutable bind-to-directive mapping."""
        return self._entries

    def get(self, bind_key: str | None = None) -> RoutingEntry | None:
        """Resolve a bind directive, falling back to the default directive."""
        return self._entries.get(_normalize_bind_key(bind_key), self._entries.get(DEFAULT_BIND_KEY))

    def with_entry(self, bind_key: str | None, entry: RoutingEntry) -> "RoutingContextPayload":
        """Return a new payload replacing only the selected bind directive."""
        return RoutingContextPayload({**self._entries, _normalize_bind_key(bind_key): entry})

    def with_write_pin(
        self, bind_key: str | None = None, *, causal_token: str | None = None
    ) -> "RoutingContextPayload":
        """Return a primary-pinned snapshot retaining the bind's causal metadata."""
        old = self.get(bind_key) or RoutingEntry()
        return self.with_entry(
            bind_key,
            RoutingEntry(
                "primary",
                bind_group=old.bind_group,
                causal_token=old.causal_token if causal_token is None else causal_token,
                max_staleness_seconds=old.max_staleness_seconds,
                pinned_by_write=True,
            ),
        )


@final
class RoutingScope:
    """Bind a routing snapshot for a synchronous or asynchronous block."""

    __slots__ = ("_payload", "_token")

    def __init__(self, payload: RoutingContextPayload | None) -> None:
        """Prepare a scope, inheriting current state when payload is omitted."""
        self._payload = payload
        self._token: Token[RoutingContextPayload | None] | None = None

    def __enter__(self) -> RoutingContextPayload:
        if self._token is not None:
            msg = "RoutingScope cannot be entered twice concurrently"
            raise RoutingContextError(msg)
        payload = self._payload if self._payload is not None else RoutingContext.current() or RoutingContextPayload()
        self._token = RoutingContext.set(payload)
        return payload

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._token is not None:
            RoutingContext.reset(self._token)
            self._token = None

    async def __aenter__(self) -> RoutingContextPayload:
        return self.__enter__()

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)


class RoutingContext:
    """Access routing directives and bind request-local consistency scopes."""

    __slots__ = ()

    @classmethod
    def current(cls) -> RoutingContextPayload | None:
        """Return the active routing snapshot, or None outside a scope."""
        return _ROUTING_VAR.get()

    @classmethod
    def get_entry(cls, bind_key: str | None = None) -> RoutingEntry | None:
        """Resolve the directive for a logical bind key."""
        payload = cls.current()
        return payload.get(bind_key) if payload is not None else None

    @classmethod
    def mark_write(cls, bind_key: str | None = None, *, causal_token: str | None = None) -> bool:
        """Pin an active scope to primary; return False for unscoped workers."""
        payload = cls.current()
        if payload is None:
            return False
        cls.set(payload.with_write_pin(bind_key, causal_token=causal_token))
        return True

    @classmethod
    def set(cls, payload: RoutingContextPayload | None) -> Token[RoutingContextPayload | None]:
        """Bind a routing snapshot and return its restoration token."""
        return _ROUTING_VAR.set(payload)

    @classmethod
    def reset(cls, token: Token[RoutingContextPayload | None]) -> None:
        """Restore the snapshot associated with a previous binding token."""
        _ROUTING_VAR.reset(token)

    @classmethod
    def clear(cls) -> None:
        """Clear routing state in the current execution context."""
        _ROUTING_VAR.set(None)

    @classmethod
    def scope(cls, payload: RoutingContextPayload | None = None) -> RoutingScope:
        """Create a sync/async scope, inheriting current directives by default."""
        return RoutingScope(payload)

    @classmethod
    def primary(cls, bind_key: str | None = None, *, causal_token: str | None = None) -> RoutingScope:
        """Create a scope explicitly routing a bind to primary."""
        payload = cls.current() or RoutingContextPayload()
        return cls.scope(payload.with_entry(bind_key, RoutingEntry("primary", causal_token=causal_token)))

    @classmethod
    def replica(
        cls,
        bind_key: str | None = None,
        *,
        bind_group: str | None = None,
        causal_token: str | None = None,
        max_staleness_seconds: float | None = None,
    ) -> RoutingScope:
        """Create a scope requesting a replica with optional consistency bounds."""
        payload = cls.current() or RoutingContextPayload()
        return cls.scope(
            payload.with_entry(
                bind_key,
                RoutingEntry(
                    "replica",
                    bind_group=bind_group,
                    causal_token=causal_token,
                    max_staleness_seconds=max_staleness_seconds,
                ),
            )
        )

    @classmethod
    def auto(cls, bind_key: str | None = None) -> RoutingScope:
        """Create a scope allowing automatic routing for a bind."""
        payload = cls.current() or RoutingContextPayload()
        return cls.scope(payload.with_entry(bind_key, RoutingEntry()))


class SQLSpecRoutingMiddleware:
    """Bind ASGI request routing from cookies, headers, then a custom extractor."""

    __slots__ = ("_app", "_bind_key", "_extractor", "_routing_header", "_strict_headers", "_write_pin_cookie")

    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        bind_key: str | None = None,
        routing_header: str = "x-sqlspec-routing",
        write_pin_cookie: str | None = None,
        strict_headers: bool = False,
        extractor: Callable[
            [MutableMapping[str, Any]],
            RoutingContextPayload | RoutingEntry | Awaitable[RoutingContextPayload | RoutingEntry | None] | None,
        ]
        | None = None,
    ) -> None:
        """Configure request routing hints and optional custom extraction."""
        self._app = app
        self._bind_key = bind_key
        self._extractor = extractor
        self._routing_header = routing_header.lower().encode("ascii")
        self._write_pin_cookie = write_pin_cookie
        self._strict_headers = strict_headers

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[MutableMapping[str, Any]]],
        send: Callable[[MutableMapping[str, Any]], Awaitable[None]],
    ) -> None:
        if scope.get("type") not in {"http", "websocket"}:
            await self._app(scope, receive, send)
            return
        payload = RoutingContext.current() or RoutingContextPayload()
        headers = {key.lower(): value.decode("latin-1") for key, value in scope.get("headers", ())}
        if self._write_pin_cookie is not None and b"cookie" in headers:
            cookie = SimpleCookie()
            try:
                cookie.load(headers[b"cookie"])
            except CookieError:
                cookie = SimpleCookie()
            morsel = cookie.get(self._write_pin_cookie)
            if morsel is not None and morsel.value:
                value = morsel.value
                payload = payload.with_write_pin(
                    self._bind_key, causal_token=None if value in {"1", "true", "True"} else value
                )
        value = headers.get(self._routing_header, "").strip()
        if value:
            if value in {"auto", "primary", "replica"}:
                entry = RoutingEntry("primary" if value == "primary" else "replica" if value == "replica" else "auto")
                payload = payload.with_entry(self._bind_key, entry)
            elif value.startswith("replica:") and value[8:]:
                payload = payload.with_entry(self._bind_key, RoutingEntry("replica", bind_group=value[8:]))
            elif self._strict_headers:
                msg = f"Invalid routing header {value!r}"
                raise RoutingContextError(msg)
            else:
                get_logger("sqlspec.routing").debug("Ignoring invalid routing header %r", value)
        if self._extractor is not None:
            result = self._extractor(scope)
            if isawaitable(result):
                result = await result
            if isinstance(result, RoutingContextPayload):
                payload = result
            elif isinstance(result, RoutingEntry):
                payload = payload.with_entry(self._bind_key, result)
        token = RoutingContext.set(payload)
        try:
            await self._app(scope, receive, send)
        finally:
            RoutingContext.reset(token)
