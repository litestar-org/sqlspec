"""Type-preserving, optionally signed pagination cursor tokens."""

import base64
import binascii
import hashlib
import hmac
import json
import math
import re
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlspec.exceptions import InvalidCursorError

__all__ = (
    "CURSOR_TOKEN_MAX_LENGTH",
    "DecodedCursor",
    "decode_cursor",
    "encode_cursor",
    "order_fingerprint",
    "register_cursor_type",
)

CURSOR_TOKEN_MAX_LENGTH = 4096
_DECIMAL_MAX_EXPONENT = 6144
_TIMEDELTA_MAX_DAYS = 999_999_999
_ENTRY_LENGTH = 2


class DecodedCursor:
    """Decoded sort keys and traversal direction."""

    __slots__ = ("backward", "values")

    def __init__(self, values: tuple[Any, ...], backward: bool) -> None:
        self.values = values
        self.backward = backward


def _float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        msg = "Non-finite cursor float"
        raise ValueError(msg)
    return result


def _decimal(value: str) -> Decimal:
    result = Decimal(value)
    if not result.is_finite() or abs(result.adjusted()) > _DECIMAL_MAX_EXPONENT:
        msg = "Out-of-range cursor decimal"
        raise ValueError(msg)
    return result


def _timedelta(value: str) -> timedelta:
    days, seconds, microseconds = (int(part) for part in value.split(":"))
    if abs(days) > _TIMEDELTA_MAX_DAYS:
        msg = "Out-of-range cursor timedelta"
        raise ValueError(msg)
    return timedelta(days=days, seconds=seconds, microseconds=microseconds)


_ENCODERS: dict[type, tuple[str, Callable[[Any], str]]] = {
    float: ("f", repr),
    str: ("s", str),
    bytes: ("y", lambda value: base64.b64encode(value).decode("ascii")),
    Decimal: ("d", str),
    UUID: ("u", str),
    datetime: ("dt", lambda value: value.isoformat()),
    date: ("da", lambda value: value.isoformat()),
    time: ("t", lambda value: value.isoformat()),
    timedelta: ("td", lambda value: f"{value.days}:{value.seconds}:{value.microseconds}"),
}
_DECODERS: dict[str, Callable[[str], Any]] = {
    "f": _float,
    "s": str,
    "y": lambda value: base64.b64decode(value, validate=True),
    "d": _decimal,
    "u": UUID,
    "dt": datetime.fromisoformat,
    "da": date.fromisoformat,
    "t": time.fromisoformat,
    "td": _timedelta,
}


def register_cursor_type(
    value_type: type, tag: str, encoder: Callable[[Any], str], decoder: Callable[[str], Any]
) -> None:
    """Register a cursor value type at application initialization.

    Args:
        value_type: Python type to encode.
        tag: Unique lowercase wire-format type tag.
        encoder: Convert a value to text.
        decoder: Restore a value from text.

    Raises:
        ValueError: The type or tag is already registered or the tag is invalid.
    """
    if (
        not re.fullmatch(r"[a-z][a-z0-9_]{0,15}", tag)
        or tag in {"n", "b", "i"}
        or tag in _DECODERS
        or value_type in _ENCODERS
        or value_type in {type(None), bool, int}
    ):
        msg = "Cursor type or tag is invalid or already registered"
        raise ValueError(msg)
    _ENCODERS[value_type] = (tag, encoder)
    _DECODERS[tag] = decoder


def order_fingerprint(parts: Sequence[tuple[str, str, str | None]]) -> str:
    """Identify the declared ordering.

    Args:
        parts: Column names, sort directions and NULL placements in order.

    Returns:
        A deterministic sixteen-character digest.
    """
    return hashlib.sha256(
        "|".join(f"{field}:{order}:{nulls or ''}" for field, order, nulls in parts).encode("utf-8")
    ).hexdigest()[:16]


def _encode_value(value: Any) -> list[Any]:
    if value is None:
        return ["n", None]
    if type(value) is bool:
        return ["b", value]
    for value_type in type(value).__mro__:
        if value_type is int:
            result = int(value)
            if abs(result) >= 2**127:
                msg = "Out-of-range cursor integer"
                raise ValueError(msg)
            return ["i", result]
        if value_type in _ENCODERS:
            tag, encoder = _ENCODERS[value_type]
            encoded = encoder(value)
            if tag in {"f", "d", "td"}:
                _DECODERS[tag](encoded)
            return [tag, encoded]
    msg = f"Cannot encode cursor value of type {type(value).__name__}; register it with register_cursor_type()"
    raise TypeError(msg)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def encode_cursor(
    values: Sequence[Any], fingerprint: str, *, backward: bool = False, secret: bytes | None = None
) -> str:
    """Encode sort keys into an opaque URL-safe token.

    Args:
        values: Sort-key values in declared order.
        fingerprint: Digest of the declared ordering.
        backward: Whether the token traverses backward.
        secret: Optional signing key.

    Returns:
        The cursor token.

    Raises:
        TypeError: A value type has no registered encoder.
        ValueError: A key or resulting token exceeds supported bounds.
    """
    payload = {"v": 1, "d": "b" if backward else "f", "o": fingerprint, "k": [_encode_value(value) for value in values]}
    body = _b64(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    token = (
        body if secret is None else body + "." + _b64(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
    )
    if len(token) > CURSOR_TOKEN_MAX_LENGTH:
        msg = "Cursor token exceeds maximum length"
        raise ValueError(msg)
    return token


def _decode_value(entry: Any) -> Any:
    if not isinstance(entry, list) or len(entry) != _ENTRY_LENGTH or not isinstance(entry[0], str):
        msg = "Invalid cursor entry"
        raise ValueError(msg)
    tag, value = entry
    if tag == "n" and value is None:
        return None
    if tag == "b" and type(value) is bool:
        return value
    if tag == "i" and type(value) is int and abs(value) < 2**127:
        return value
    if tag in _DECODERS and isinstance(value, str):
        return _DECODERS[tag](value)
    msg = "Invalid cursor value"
    raise ValueError(msg)


def decode_cursor(token: object, fingerprint: str, key_count: int, *, secret: bytes | None = None) -> DecodedCursor:
    """Validate a cursor and recover its sort keys.

    Args:
        token: Client-provided cursor token.
        fingerprint: Expected ordering digest.
        key_count: Expected number of sort keys.
        secret: Optional required signing key.

    Returns:
        Typed sort-key values and traversal direction.

    Raises:
        InvalidCursorError: The token is malformed, mismatched, or incorrectly signed.
    """
    if (
        not isinstance(token, str)
        or not token
        or len(token) > CURSOR_TOKEN_MAX_LENGTH
        or not re.fullmatch(r"[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?", token)
    ):
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    body = token
    if secret is None and "." in token:
        msg = "Invalid pagination cursor: unexpected signature"
        raise InvalidCursorError(msg)
    if secret is not None:
        if "." not in token:
            msg = "Invalid pagination cursor: missing signature"
            raise InvalidCursorError(msg)
        body, signature = token.split(".")
        expected = _b64(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(expected.encode("ascii"), signature.encode("ascii")):
            msg = "Invalid pagination cursor: signature mismatch"
            raise InvalidCursorError(msg)
    try:
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)).decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, ValueError, RecursionError) as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc
    if (
        not isinstance(payload, dict)
        or type(payload.get("v")) is not int
        or payload["v"] != 1
        or payload.get("d") not in ("f", "b")
        or not isinstance(payload.get("o"), str)
        or not isinstance(payload.get("k"), list)
    ):
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    if payload["o"] != fingerprint or len(payload["k"]) != key_count:
        msg = "Invalid pagination cursor: ordering mismatch"
        raise InvalidCursorError(msg)
    try:
        values = tuple(_decode_value(entry) for entry in payload["k"])
    except (ValueError, TypeError, ArithmeticError) as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc
    return DecodedCursor(values, payload["d"] == "b")
