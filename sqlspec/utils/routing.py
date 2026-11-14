"""Request-scoped routing directives and framework-independent ASGI middleware."""

import base64
import hashlib
import hmac
import time
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from contextvars import ContextVar, Token
from dataclasses import KW_ONLY, dataclass
from http.cookies import CookieError, SimpleCookie
from inspect import isawaitable
from math import isfinite
from types import MappingProxyType, TracebackType
from typing import Any, Final, Literal, TypeAlias, final

from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError
from sqlspec.utils.logging import get_logger

__all__ = (
    "DEFAULT_BIND_KEY",
    "RoutingContext",
    "RoutingContextPayload",
    "RoutingEntry",
    "RoutingMode",
    "RoutingScope",
    "SQLSpecRoutingMiddleware",
    "apply_request_routing_hints",
    "extract_asgi_routing_headers",
    "is_valid_routing_token",
    "sign_write_pin",
    "validate_routing_middleware_settings",
    "verify_write_pin",
)

RoutingMode: TypeAlias = Literal["auto", "primary", "replica"]
DEFAULT_BIND_KEY: Final[str] = "__default__"
_MAX_ROUTING_TOKEN_LENGTH: Final[int] = 256
_MAX_SIGNED_PIN_LENGTH: Final[int] = 1024
_SIGNED_PIN_PART_COUNT: Final[int] = 5
_MIN_PRINTABLE_ASCII: Final[int] = 32
_ASCII_DEL: Final[int] = 127
_COOKIE_HEADER_BYTES: Final[bytes] = b"cookie"
_ROUTING_VAR: ContextVar["RoutingContextPayload | None"] = ContextVar("sqlspec_routing_context", default=None)


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
        if self.bind_group is not None and not _is_valid_routing_token(self.bind_group):
            msg = "bind_group must be a non-empty printable string of at most 256 characters"
            raise RoutingContextError(msg)
        if self.causal_token is not None and not _is_valid_routing_token(self.causal_token):
            msg = "causal_token must be a non-empty printable string of at most 256 characters"
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

    __slots__ = (
        "_app",
        "_bind_key",
        "_extractor",
        "_routing_header",
        "_routing_secret",
        "_strict_headers",
        "_write_pin_cookie",
    )

    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        bind_key: str | None = None,
        routing_header: str | None = "x-sqlspec-routing",
        write_pin_cookie: str | None = None,
        routing_secret: str | bytes | None = None,
        strict_headers: bool = False,
        extractor: Callable[
            [MutableMapping[str, Any]],
            RoutingContextPayload | RoutingEntry | Awaitable[RoutingContextPayload | RoutingEntry | None] | None,
        ]
        | None = None,
    ) -> None:
        """Configure request routing hints and optional custom extraction."""
        _, normalized_header, normalized_cookie, normalized_secret = validate_routing_middleware_settings(
            enable_routing_middleware=True,
            routing_header=routing_header,
            write_pin_cookie=write_pin_cookie,
            routing_secret=routing_secret,
        )
        self._app = app
        self._bind_key = bind_key
        self._extractor = extractor
        self._routing_header = normalized_header.lower().encode("ascii") if normalized_header is not None else None
        self._write_pin_cookie = normalized_cookie
        self._routing_secret = normalized_secret
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
        cookie_header, routing_header_value = extract_asgi_routing_headers(
            scope, self._routing_header, include_cookies=self._write_pin_cookie is not None
        )
        payload = apply_request_routing_hints(
            payload,
            bind_key=self._bind_key,
            routing_header_value=routing_header_value,
            cookie_header=cookie_header,
            write_pin_cookie=self._write_pin_cookie,
            routing_secret=self._routing_secret,
            strict_headers=self._strict_headers,
        )
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


def sign_write_pin(
    secret: str | bytes,
    *,
    ttl_seconds: float = 5.0,
    bind_key: str | None = None,
    causal_token: str | None = None,
    clock: Callable[[], float] = time.time,
) -> str:
    """Create an expiring HMAC-SHA256 write-pin cookie value bound to ``bind_key``.

    Args:
        secret: Shared HMAC secret key (non-empty ``str`` or ``bytes``).
        ttl_seconds: Positive finite lifetime in seconds before the pin expires.
        bind_key: Logical database bind key protected by this pin.
        causal_token: Optional adapter causal token (such as WAL LSN or GTID set).
        clock: Wall-clock epoch provider in seconds.

    Returns:
        Cookie-safe ASCII token verifiable with :func:`verify_write_pin`.
    """
    secret_bytes = _normalize_secret(secret)
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, (int, float))
        or not isfinite(ttl_seconds)
        or ttl_seconds <= 0
    ):
        msg = "ttl_seconds must be a finite positive number"
        raise ImproperConfigurationError(msg)
    if bind_key is not None and not is_valid_routing_token(bind_key):
        msg = "bind_key must be a non-empty printable string of at most 256 characters"
        raise RoutingContextError(msg)
    if causal_token is not None and not is_valid_routing_token(causal_token):
        msg = "causal_token must be a non-empty printable string of at most 256 characters"
        raise RoutingContextError(msg)
    exp_ms = int((clock() + float(ttl_seconds)) * 1000)
    encoded_bind = _b64url_encode(_normalize_bind_key(bind_key).encode("utf-8"))
    encoded_causal = _b64url_encode(causal_token.encode("utf-8")) if causal_token is not None else ""
    body = f"v1.{exp_ms}.{encoded_bind}.{encoded_causal}"
    signature = _b64url_encode(hmac.new(secret_bytes, body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{signature}"


def verify_write_pin(
    token: str, secret: str | bytes, *, bind_key: str | None = None, clock: Callable[[], float] = time.time
) -> tuple[bool, str | None]:
    """Verify an HMAC-signed write-pin cookie value and check expiration and bind key.

    Args:
        token: Raw cookie value supplied by the client.
        secret: Shared HMAC secret key (non-empty ``str`` or ``bytes``).
        bind_key: Logical database bind key expected for this check.
        clock: Wall-clock epoch provider in seconds.

    Returns:
        Tuple ``(is_valid, causal_token)``; ``(False, None)`` when invalid or expired.
    """
    secret_bytes = _normalize_secret(secret)
    if not isinstance(token, str) or not token or len(token) > _MAX_SIGNED_PIN_LENGTH:
        return False, None
    try:
        token.encode("ascii")
    except UnicodeEncodeError:
        return False, None
    parts = token.split(".")
    if len(parts) != _SIGNED_PIN_PART_COUNT or parts[0] != "v1" or not parts[1].isdigit():
        return False, None
    body = f"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}"
    expected_sig = _b64url_encode(hmac.new(secret_bytes, body.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(parts[4], expected_sig):
        return False, None
    if int(parts[1]) <= int(clock() * 1000):
        return False, None
    raw_bind = _b64url_decode(parts[2])
    if raw_bind is None:
        return False, None
    try:
        decoded_bind = raw_bind.decode("utf-8")
    except UnicodeDecodeError:
        return False, None
    if decoded_bind != _normalize_bind_key(bind_key):
        return False, None
    if not parts[3]:
        return True, None
    raw_causal = _b64url_decode(parts[3])
    if raw_causal is None:
        return False, None
    try:
        decoded_causal = raw_causal.decode("utf-8")
    except UnicodeDecodeError:
        return False, None
    if not is_valid_routing_token(decoded_causal):
        return False, None
    return True, decoded_causal


def validate_routing_middleware_settings(
    *,
    enable_routing_middleware: Any = True,
    routing_header: Any = None,
    write_pin_cookie: Any = None,
    routing_secret: Any = None,
) -> tuple[bool, str | None, str | None, bytes | None]:
    """Validate framework routing middleware options and normalize header, cookie, and secret values."""
    if not isinstance(enable_routing_middleware, bool):
        msg = "enable_routing_middleware must be a boolean"
        raise ImproperConfigurationError(msg)
    if routing_header is not None:
        if (
            not isinstance(routing_header, str)
            or not routing_header.strip()
            or not is_valid_routing_token(routing_header)
        ):
            msg = "routing_header must be None or a non-empty printable ASCII header name"
            raise ImproperConfigurationError(msg)
        try:
            routing_header.encode("ascii")
        except UnicodeEncodeError as exc:
            msg = "routing_header must be None or a non-empty printable ASCII header name"
            raise ImproperConfigurationError(msg) from exc
    if write_pin_cookie is not None and (
        not isinstance(write_pin_cookie, str)
        or not write_pin_cookie.strip()
        or not is_valid_routing_token(write_pin_cookie)
    ):
        msg = "write_pin_cookie must be None or a non-empty printable token"
        raise ImproperConfigurationError(msg)
    normalized_secret = _normalize_secret(routing_secret) if routing_secret is not None else None
    return enable_routing_middleware, routing_header, write_pin_cookie, normalized_secret


def apply_request_routing_hints(
    payload: RoutingContextPayload,
    *,
    bind_key: str | None = None,
    routing_header_value: str | None = None,
    cookie_header: str | None = None,
    cookie_value: str | None = None,
    write_pin_cookie: str | None = None,
    routing_secret: str | bytes | None = None,
    strict_headers: bool = False,
) -> RoutingContextPayload:
    """Apply write-pin cookie and routing header hints onto a routing snapshot."""
    if write_pin_cookie is not None:
        resolved_cookie = cookie_value
        if resolved_cookie is None and cookie_header is not None:
            resolved_cookie = _extract_cookie_value(cookie_header, write_pin_cookie)
        if resolved_cookie:
            if routing_secret is not None:
                verified, signed_causal = verify_write_pin(resolved_cookie, routing_secret, bind_key=bind_key)
                if verified:
                    payload = payload.with_write_pin(bind_key, causal_token=signed_causal)
                else:
                    get_logger("sqlspec.routing").debug("Ignoring unverified write-pin cookie")
            elif resolved_cookie in {"1", "true", "True"}:
                payload = payload.with_write_pin(bind_key, causal_token=None)
            elif is_valid_routing_token(resolved_cookie):
                payload = payload.with_write_pin(bind_key, causal_token=resolved_cookie)
            else:
                get_logger("sqlspec.routing").debug("Ignoring invalid write-pin cookie %r", resolved_cookie)
    cleaned_header = routing_header_value.strip() if routing_header_value is not None else ""
    if cleaned_header:
        if cleaned_header in {"auto", "primary", "replica"}:
            entry = RoutingEntry(
                "primary" if cleaned_header == "primary" else "replica" if cleaned_header == "replica" else "auto"
            )
            payload = payload.with_entry(bind_key, entry)
        elif cleaned_header.startswith("replica:") and is_valid_routing_token(cleaned_header[8:]):
            payload = payload.with_entry(bind_key, RoutingEntry("replica", bind_group=cleaned_header[8:]))
        elif strict_headers:
            msg = f"Invalid routing header {cleaned_header!r}"
            raise RoutingContextError(msg)
        else:
            get_logger("sqlspec.routing").debug("Ignoring invalid routing header %r", cleaned_header)
    return payload


def is_valid_routing_token(value: str) -> bool:
    """Return whether a routing identifier or causal token meets printable length bounds."""
    return _is_valid_routing_token(value)


def extract_asgi_routing_headers(
    scope: Mapping[str, Any], routing_header_bytes: bytes | None, *, include_cookies: bool
) -> tuple[str | None, str]:
    """Extract Latin-1 cookie and routing header values from an ASGI scope."""
    if routing_header_bytes is None and not include_cookies:
        return None, ""
    cookie_parts: list[str] = []
    routing_header_value = ""
    for raw_key, raw_value in scope.get("headers", ()):
        lower_key = raw_key.lower()
        if include_cookies and lower_key == _COOKIE_HEADER_BYTES:
            cookie_parts.append(raw_value.decode("latin-1"))
        elif routing_header_bytes is not None and lower_key == routing_header_bytes:
            routing_header_value = raw_value.decode("latin-1").strip()
    cookie_header = "; ".join(cookie_parts) if cookie_parts else None
    return cookie_header, routing_header_value


def _normalize_secret(secret: Any) -> bytes:
    if isinstance(secret, str):
        encoded = secret.encode("utf-8")
    elif isinstance(secret, bytes):
        encoded = secret
    else:
        msg = "routing_secret must be a non-empty str or bytes"
        raise ImproperConfigurationError(msg)
    if not encoded:
        msg = "routing_secret must be a non-empty str or bytes"
        raise ImproperConfigurationError(msg)
    return encoded


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes | None:
    padding = "=" * ((-len(value)) % 4)
    try:
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except (ValueError, UnicodeEncodeError):
        return None


def _normalize_bind_key(bind_key: str | None) -> str:
    return bind_key or DEFAULT_BIND_KEY


def _is_valid_routing_token(value: str) -> bool:
    return 0 < len(value) <= _MAX_ROUTING_TOKEN_LENGTH and not any(
        ord(ch) < _MIN_PRINTABLE_ASCII or ord(ch) == _ASCII_DEL for ch in value
    )


def _extract_cookie_value(cookie_header: str, cookie_name: str) -> str | None:
    cookie = SimpleCookie()
    try:
        cookie.load(cookie_header)
    except CookieError:
        cookie = SimpleCookie()
    morsel = cookie.get(cookie_name)
    if morsel is not None:
        return morsel.value
    for part in cookie_header.split(";"):
        name, separator, raw_value = part.partition("=")
        if not separator or name.strip() != cookie_name:
            continue
        value = raw_value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) > 1:
            return value[1:-1]
        return value
    return None
