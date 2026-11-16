"""Type-preserving, optionally signed pagination cursor tokens."""

import base64
import binascii
import datetime as dt
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlspec.exceptions import ImproperConfigurationError, InvalidCursorError

__all__ = (
    "CURSOR_TOKEN_MAX_LENGTH",
    "DecodedCursor",
    "decode_cursor",
    "encode_cursor",
    "normalize_cursor_ttl",
    "normalize_pagination_cursor_ttl",
    "order_fingerprint",
    "register_cursor_type",
    "resolve_env_pagination_cursor_secret",
    "resolve_plugin_cursor_settings",
)

CURSOR_TOKEN_MAX_LENGTH = 4096
_DECIMAL_MAX_EXPONENT = 6144
_TIMEDELTA_MAX_DAYS = 999_999_999
_ENTRY_LENGTH = 2
_NONCE_LENGTH = 16
_HASH_BLOCK_SIZE = 32
_SIGNED_PART_COUNT = 2
_ENCRYPTED_PART_COUNT = 3
_ENCRYPTED_PREFIX = "e1"
_ENCRYPTION_KEY_INFO = b"sqlspec-cursor-enc-v1"
_MAC_KEY_INFO = b"sqlspec-cursor-mac-v1"
_B64_PART_PATTERN = re.compile(r"[A-Za-z0-9_-]+")


class DecodedCursor:
    """Decoded sort keys and traversal direction."""

    __slots__ = ("backward", "values")

    def __init__(self, values: tuple[Any, ...], backward: bool) -> None:
        self.values = values
        self.backward = backward


_ENCODERS: dict[type, tuple[str, Callable[[Any], str]]] = {
    float: ("f", repr),
    str: ("s", str),
    bytes: ("y", lambda value: base64.b64encode(value).decode("ascii")),
    Decimal: ("d", str),
    UUID: ("u", str),
    dt.datetime: ("dt", lambda value: value.isoformat()),
    dt.date: ("da", lambda value: value.isoformat()),
    dt.time: ("t", lambda value: value.isoformat()),
    dt.timedelta: ("td", lambda value: f"{value.days}:{value.seconds}:{value.microseconds}"),
}


def normalize_cursor_ttl(ttl: "float | dt.timedelta | None") -> "float | None":
    """Validate and normalize a cursor TTL value into positive finite seconds.

    Args:
        ttl: Lifetime in seconds, timedelta, or None.

    Returns:
        Positive finite seconds, or None when ttl is None.

    Raises:
        TypeError: TTL is an unsupported type.
        ValueError: TTL is non-positive or non-finite.
    """
    if ttl is None:
        return None
    if isinstance(ttl, dt.timedelta):
        seconds = ttl.total_seconds()
    elif isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
        msg = "ttl must be a positive finite duration"
        raise TypeError(msg)
    else:
        seconds = float(ttl)
    if not math.isfinite(seconds) or seconds <= 0:
        msg = "ttl must be positive and finite"
        raise ValueError(msg)
    return seconds


def normalize_pagination_cursor_ttl(value: Any) -> float:
    """Return a cursor TTL in seconds.

    Args:
        value: TTL given as seconds or as a ``datetime.timedelta``.

    Returns:
        The TTL in seconds.

    Raises:
        ImproperConfigurationError: If the value is not an int, float, or ``datetime.timedelta``.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, dt.timedelta)):
        msg = "pagination_cursor_ttl must be an int, float, or datetime.timedelta"
        raise ImproperConfigurationError(msg)
    return value.total_seconds() if isinstance(value, dt.timedelta) else float(value)


def resolve_env_pagination_cursor_secret() -> str | None:
    """Return the cursor secret from ``SQLSPEC_PAGINATION_CURSOR_SECRET`` or ``SECRET_KEY``.

    Returns:
        The first non-empty environment value, otherwise ``None``.
    """
    return os.environ.get("SQLSPEC_PAGINATION_CURSOR_SECRET") or os.environ.get("SECRET_KEY") or None


def resolve_plugin_cursor_settings(
    settings_list: "Sequence[Mapping[str, Any]]",
    *,
    pagination_cursor_secret: str | bytes | None,
    pagination_cursor_ttl: float | dt.timedelta | None,
    pagination_cursor_encrypted: bool | None,
) -> "tuple[str | bytes | None, float | dt.timedelta | None, bool | None]":
    """Merge application-wide cursor settings with per-configuration framework settings.

    Args:
        settings_list: Framework extension settings of every registered configuration.
        pagination_cursor_secret: Secret passed to the plugin, if any.
        pagination_cursor_ttl: Cursor expiration window passed to the plugin, if any.
        pagination_cursor_encrypted: Cursor encryption flag passed to the plugin, if any.

    Returns:
        The resolved secret, TTL, and encryption flag. The secret falls back to the environment.

    Raises:
        ImproperConfigurationError: If configurations disagree on a setting or a TTL has an invalid type.
    """
    resolved_secret: str | bytes | None = pagination_cursor_secret
    for settings in settings_list:
        candidate = settings.get("pagination_cursor_secret")
        if candidate is None:
            continue
        if resolved_secret is None:
            resolved_secret = candidate
        elif resolved_secret != candidate:
            msg = "Conflicting 'pagination_cursor_secret' values across plugin configuration"
            raise ImproperConfigurationError(msg)
    if resolved_secret is None:
        resolved_secret = resolve_env_pagination_cursor_secret()

    resolved_ttl: int | float | dt.timedelta | None = None
    resolved_ttl_seconds: float | None = None
    if pagination_cursor_ttl is not None:
        resolved_ttl = pagination_cursor_ttl
        resolved_ttl_seconds = normalize_pagination_cursor_ttl(pagination_cursor_ttl)
    for settings in settings_list:
        candidate_ttl = settings.get("pagination_cursor_ttl")
        if candidate_ttl is None:
            continue
        candidate_seconds = normalize_pagination_cursor_ttl(candidate_ttl)
        if resolved_ttl_seconds is None:
            resolved_ttl = candidate_ttl
            resolved_ttl_seconds = candidate_seconds
        elif resolved_ttl_seconds != candidate_seconds:
            msg = "Conflicting 'pagination_cursor_ttl' values across plugin configuration"
            raise ImproperConfigurationError(msg)

    resolved_encrypt: bool | None = pagination_cursor_encrypted
    for settings in settings_list:
        candidate_encrypt = settings.get("pagination_cursor_encrypted")
        if candidate_encrypt is None:
            continue
        if resolved_encrypt is None:
            resolved_encrypt = bool(candidate_encrypt)
        elif resolved_encrypt != bool(candidate_encrypt):
            msg = "Conflicting 'pagination_cursor_encrypted' values across plugin configuration"
            raise ImproperConfigurationError(msg)

    return resolved_secret, resolved_ttl, resolved_encrypt


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


def encode_cursor(
    values: Sequence[Any],
    fingerprint: str,
    *,
    backward: bool = False,
    secret: bytes | None = None,
    ttl: float | dt.timedelta | None = None,
    encrypted: bool = False,
) -> str:
    """Encode sort keys into an opaque URL-safe token.

    Args:
        values: Sort-key values in declared order.
        fingerprint: Digest of the declared ordering.
        backward: Whether the token traverses backward.
        secret: Optional signing or encryption key.
        ttl: Optional positive finite expiration duration in seconds or timedelta.
        encrypted: Encrypt the payload using HMAC-SHA256 CTR + Encrypt-then-MAC.

    Returns:
        The cursor token.

    Raises:
        TypeError: A value type has no registered encoder.
        ValueError: Secret, TTL, key, or resulting token fails validation.
    """
    ttl_seconds = normalize_cursor_ttl(ttl)
    if secret is not None and not secret:
        msg = "secret must not be empty"
        raise ValueError(msg)
    if encrypted and not secret:
        msg = "secret must be non-empty when encrypted=True"
        raise ValueError(msg)

    payload: dict[str, Any] = {
        "v": 1,
        "d": "b" if backward else "f",
        "o": fingerprint,
        "k": [_encode_value(value) for value in values],
    }
    if ttl_seconds is not None:
        issued_at = time.time()
        expires_at = issued_at + ttl_seconds
        if not math.isfinite(expires_at):
            msg = "ttl exceeds maximum representable expiration timestamp"
            raise ValueError(msg)
        payload["i"] = issued_at
        payload["e"] = expires_at

    plaintext = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if encrypted:
        assert secret is not None
        encryption_key, mac_key = _derive_encryption_keys(secret)
        nonce = secrets.token_bytes(_NONCE_LENGTH)
        ciphertext = _apply_keystream(encryption_key, nonce, plaintext)
        sealed = _b64(nonce + ciphertext)
        mac_input = f"{_ENCRYPTED_PREFIX}.{sealed}".encode("ascii")
        mac = _b64(hmac.new(mac_key, mac_input, hashlib.sha256).digest())
        token = f"{_ENCRYPTED_PREFIX}.{sealed}.{mac}"
    else:
        body = _b64(plaintext)
        token = (
            body
            if secret is None
            else body + "." + _b64(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
        )
    if len(token) > CURSOR_TOKEN_MAX_LENGTH:
        msg = "Cursor token exceeds maximum length"
        raise ValueError(msg)
    return token


def decode_cursor(
    token: object,
    fingerprint: str,
    key_count: int,
    *,
    secret: bytes | None = None,
    ttl: float | dt.timedelta | None = None,
    encrypted: bool = False,
) -> DecodedCursor:
    """Validate a cursor and recover its sort keys.

    Args:
        token: Client-provided cursor token.
        fingerprint: Expected ordering digest.
        key_count: Expected number of sort keys.
        secret: Optional required signing or encryption key.
        ttl: Optional maximum cursor lifetime enforced against issued-at/expiration claims.
        encrypted: Require encrypted ``e1`` tokens when True and reject them when False.

    Returns:
        Typed sort-key values and traversal direction.

    Raises:
        ValueError: Invalid secret or ttl configuration.
        InvalidCursorError: The token is malformed, expired, mismatched, or invalid.
    """
    ttl_seconds = normalize_cursor_ttl(ttl)
    if secret is not None and not secret:
        msg = "secret must not be empty"
        raise ValueError(msg)
    if encrypted and not secret:
        msg = "secret must be non-empty when encrypted=True"
        raise ValueError(msg)
    if not isinstance(token, str) or not token or len(token) > CURSOR_TOKEN_MAX_LENGTH:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)

    parts = token.split(".")
    if len(parts) == _ENCRYPTED_PART_COUNT:
        raw_bytes = _decode_encrypted_parts(parts, secret=secret, encrypted=encrypted)
    elif len(parts) in {1, _SIGNED_PART_COUNT}:
        raw_bytes = _decode_signed_parts(parts, secret=secret, encrypted=encrypted)
    else:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)

    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
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
    _validate_payload_timestamps(payload, ttl_seconds)
    try:
        values = tuple(_decode_value(entry) for entry in payload["k"])
    except (ValueError, TypeError, ArithmeticError, LookupError) as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc
    return DecodedCursor(values, payload["d"] == "b")


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


def _timedelta(value: str) -> dt.timedelta:
    days, seconds, microseconds = (int(part) for part in value.split(":"))
    if abs(days) > _TIMEDELTA_MAX_DAYS:
        msg = "Out-of-range cursor timedelta"
        raise ValueError(msg)
    return dt.timedelta(days=days, seconds=seconds, microseconds=microseconds)


_DECODERS: dict[str, Callable[[str], Any]] = {
    "f": _float,
    "s": str,
    "y": lambda value: base64.b64decode(value, validate=True),
    "d": _decimal,
    "u": UUID,
    "dt": dt.datetime.fromisoformat,
    "da": dt.date.fromisoformat,
    "t": dt.time.fromisoformat,
    "td": _timedelta,
}


def _derive_encryption_keys(secret: bytes) -> "tuple[bytes, bytes]":
    encryption_key = hmac.new(secret, _ENCRYPTION_KEY_INFO, hashlib.sha256).digest()
    mac_key = hmac.new(secret, _MAC_KEY_INFO, hashlib.sha256).digest()
    return encryption_key, mac_key


def _apply_keystream(encryption_key: bytes, nonce: bytes, data: bytes) -> bytes:
    """XOR data with an HMAC-SHA256 counter keystream; the same call encrypts and decrypts."""
    length = len(data)
    output = bytearray(length)
    offset = 0
    counter = 0
    while offset < length:
        block = hmac.new(encryption_key, nonce + counter.to_bytes(8, "big"), hashlib.sha256).digest()
        chunk_len = min(_HASH_BLOCK_SIZE, length - offset)
        for idx in range(chunk_len):
            output[offset + idx] = data[offset + idx] ^ block[idx]
        offset += chunk_len
        counter += 1
    return bytes(output)


def _decode_signed_parts(parts: "list[str]", *, secret: bytes | None, encrypted: bool) -> bytes:
    for part in parts:
        if _B64_PART_PATTERN.fullmatch(part) is None:
            msg = "Invalid pagination cursor: malformed token"
            raise InvalidCursorError(msg)
    if encrypted:
        msg = "Invalid pagination cursor: encryption required"
        raise InvalidCursorError(msg)
    body = parts[0]
    has_signature = len(parts) == _SIGNED_PART_COUNT
    if secret is None and has_signature:
        msg = "Invalid pagination cursor: unexpected signature"
        raise InvalidCursorError(msg)
    if secret is not None:
        if not has_signature:
            msg = "Invalid pagination cursor: missing signature"
            raise InvalidCursorError(msg)
        expected = _b64(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(expected.encode("ascii"), parts[1].encode("ascii")):
            msg = "Invalid pagination cursor: signature mismatch"
            raise InvalidCursorError(msg)
    try:
        return base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
    except binascii.Error as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc


def _decode_encrypted_parts(parts: "list[str]", *, secret: bytes | None, encrypted: bool) -> bytes:
    if (
        parts[0] != _ENCRYPTED_PREFIX
        or _B64_PART_PATTERN.fullmatch(parts[1]) is None
        or _B64_PART_PATTERN.fullmatch(parts[2]) is None
    ):
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    if secret is None:
        msg = "Invalid pagination cursor: missing secret"
        raise InvalidCursorError(msg)
    if not encrypted:
        msg = "Invalid pagination cursor: unexpected encrypted token"
        raise InvalidCursorError(msg)
    encryption_key, mac_key = _derive_encryption_keys(secret)
    mac_input = f"{_ENCRYPTED_PREFIX}.{parts[1]}".encode("ascii")
    expected_mac = _b64(hmac.new(mac_key, mac_input, hashlib.sha256).digest())
    if not hmac.compare_digest(expected_mac.encode("ascii"), parts[2].encode("ascii")):
        msg = "Invalid pagination cursor: signature mismatch"
        raise InvalidCursorError(msg)
    try:
        sealed = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
    except binascii.Error as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc
    if len(sealed) < _NONCE_LENGTH:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    nonce = sealed[:_NONCE_LENGTH]
    ciphertext = sealed[_NONCE_LENGTH:]
    return _apply_keystream(encryption_key, nonce, ciphertext)


def _parse_timestamp_claim(raw_value: Any) -> float:
    if type(raw_value) is bool or not isinstance(raw_value, (int, float)):
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    try:
        numeric_value = float(raw_value)
    except OverflowError as exc:
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg) from exc
    if not math.isfinite(numeric_value):
        msg = "Invalid pagination cursor: malformed token"
        raise InvalidCursorError(msg)
    return numeric_value


def _validate_payload_timestamps(payload: dict[str, Any], ttl_seconds: "float | None") -> None:
    issued_at: float | None = _parse_timestamp_claim(payload["i"]) if "i" in payload else None
    expires_at: float | None = _parse_timestamp_claim(payload["e"]) if "e" in payload else None
    now = time.time()
    if expires_at is not None and now >= expires_at:
        msg = "Invalid pagination cursor: expired"
        raise InvalidCursorError(msg)
    if ttl_seconds is not None and (issued_at is None or expires_at is None or now >= issued_at + ttl_seconds):
        msg = "Invalid pagination cursor: expired"
        raise InvalidCursorError(msg)


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
