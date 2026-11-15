"""Cursor wire format and validation contracts."""

import base64
import json
import math
import re
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest

from sqlspec.core._cursor import (
    decode_cursor,
    encode_cursor,
    normalize_cursor_ttl,
    order_fingerprint,
    register_cursor_type,
)
from sqlspec.exceptions import InvalidCursorError


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -7,
        2**70,
        1.5,
        "",
        "héllo~|:",
        b"\x00\xff",
        Decimal("1.10"),
        timedelta(days=1, seconds=2, microseconds=3),
        UUID("12345678-1234-5678-1234-567812345678"),
        datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        datetime(2026, 1, 2),
        date(2026, 1, 2),
        time(3, 4, 5, 6),
    ],
)
def test_round_trip(value: Any) -> None:
    token = encode_cursor([value], "order")
    result = decode_cursor(token, "order", 1)
    assert result.values == (value,)
    assert type(result.values[0]) is type(value)
    assert not result.backward
    assert re.fullmatch(r"[A-Za-z0-9_-]+", token)


def test_signed_direction() -> None:
    token = encode_cursor([1, "two", None], "order", backward=True, secret=b"secret")
    result = decode_cursor(token, "order", 3, secret=b"secret")
    assert result.values == (1, "two", None)
    assert result.backward
    assert re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token)
    for changed, secret, reason in [
        (token, b"wrong", "signature mismatch"),
        ("A" + token[1:], b"secret", "signature mismatch"),
        (token, None, "unexpected signature"),
        (token.split(".")[0], b"secret", "missing signature"),
        (token + "é", b"secret", "malformed token"),
    ]:
        with pytest.raises(InvalidCursorError, match=reason):
            decode_cursor(changed, "order", 3, secret=secret)


@pytest.mark.parametrize(
    "entry",
    [
        ["n", 1],
        ["b", 1],
        ["i", True],
        ["i", "5"],
        ["i", 2**200],
        ["f", "nan"],
        ["f", "inf"],
        ["d", "sNaN"],
        ["d", "1E+999999999"],
        ["td", "999999999999:0:0"],
        ["td", "1:2"],
        ["dt", "nope"],
        ["y", "!!!"],
        ["s", 2],
        ["unknown", "x"],
        [1, "x"],
        ["s"],
        None,
    ],
)
def test_invalid_entries(entry: Any) -> None:
    token = (
        base64
        .urlsafe_b64encode(json.dumps({"v": 1, "d": "f", "o": "order", "k": [entry]}).encode())
        .decode()
        .rstrip("=")
    )
    with pytest.raises(InvalidCursorError, match="Invalid pagination cursor: malformed token"):
        decode_cursor(token, "order", 1)


@pytest.mark.parametrize(
    "token",
    [
        None,
        1,
        b"cursor",
        [],
        object(),
        "",
        "!!!",
        "a" * 4097,
        "YWJj!!!!",
        "a",
        "W10",
        "bm90IGpzb24",
        base64.urlsafe_b64encode(b"[" * 1500 + b"]" * 1500).decode().rstrip("="),
    ],
)
def test_invalid_tokens(token: object) -> None:
    with pytest.raises(InvalidCursorError, match="malformed token"):
        decode_cursor(token, "order", 1)


@pytest.mark.parametrize("field,value", [("v", 2), ("v", True), ("d", "x"), ("o", 1), ("k", {})])
def test_invalid_payload(field: str, value: Any) -> None:
    payload = {"v": 1, "d": "f", "o": "order", "k": []}
    payload[field] = value
    token = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    with pytest.raises(InvalidCursorError, match="malformed token"):
        decode_cursor(token, "order", 0)


@pytest.mark.parametrize("value", [2**200, float("nan"), Decimal("NaN"), Decimal("1e9999")])
def test_encode_guards(value: Any) -> None:
    with pytest.raises(ValueError):
        encode_cursor([value], "order")


def test_order_checks() -> None:
    token = encode_cursor([1], "order")
    for fingerprint, count in [("other", 1), ("order", 2)]:
        with pytest.raises(InvalidCursorError, match="ordering mismatch"):
            decode_cursor(token, fingerprint, count)
    parts = [("id", "asc", None), ("name", "desc", "last")]
    fingerprint = order_fingerprint(parts)
    assert len(fingerprint) == 16
    assert fingerprint == order_fingerprint(parts)
    assert fingerprint != order_fingerprint(parts[::-1])
    assert fingerprint != order_fingerprint([("id", "asc", "first"), parts[1]])


def test_registration() -> None:
    register_cursor_type(Fraction, "frac", str, Fraction)
    assert decode_cursor(encode_cursor([Fraction(1, 3)], "o"), "o", 1).values == (Fraction(1, 3),)
    for value_type, tag in [
        (Fraction, "new"),
        (complex, "frac"),
        (complex, "s"),
        (complex, "Bad-Tag"),
        (int, "integer"),
    ]:
        with pytest.raises(ValueError):
            register_cursor_type(value_type, tag, str, str)
    with pytest.raises(TypeError, match="register_cursor_type"):
        encode_cursor([object()], "o")


def test_encode_rejects_oversized_token() -> None:
    with pytest.raises(ValueError, match="maximum length"):
        encode_cursor(["x" * 4096], "o")


def test_subclass_values_use_base_codec() -> None:
    class CustomInt(int):
        pass

    class CustomDateTime(datetime):
        pass

    result = decode_cursor(encode_cursor([CustomInt(7), CustomDateTime(2026, 1, 2)], "o"), "o", 2)
    assert result.values == (7, datetime(2026, 1, 2))
    assert type(result.values[0]) is int
    assert type(result.values[1]) is datetime


@pytest.mark.parametrize(
    ("bad_ttl", "exc_type"),
    [
        (0, ValueError),
        (0.0, ValueError),
        (-1, ValueError),
        (-0.1, ValueError),
        (timedelta(seconds=0), ValueError),
        (timedelta(milliseconds=-1), ValueError),
        (float("nan"), ValueError),
        (float("inf"), ValueError),
        (float("-inf"), ValueError),
        (True, TypeError),
        (False, TypeError),
        ("10", TypeError),
    ],
)
def test_cursor_ttl_validation_rejects_invalid_values(bad_ttl: Any, exc_type: type[Exception]) -> None:
    with pytest.raises(exc_type, match="ttl"):
        normalize_cursor_ttl(bad_ttl)
    with pytest.raises(exc_type, match="ttl"):
        encode_cursor([1], "o", ttl=bad_ttl)
    token = encode_cursor([1], "o")
    with pytest.raises(exc_type, match="ttl"):
        decode_cursor(token, "o", 1, ttl=bad_ttl)


def test_cursor_ttl_none_omits_timestamps_for_determinism() -> None:
    token_a = encode_cursor([42], "o")
    token_b = encode_cursor([42], "o", ttl=None)
    assert token_a == token_b
    payload = json.loads(base64.urlsafe_b64decode(token_a + "=" * (-len(token_a) % 4)))
    assert "i" not in payload
    assert "e" not in payload


@pytest.mark.parametrize("ttl", [60, 60.0, timedelta(seconds=60)])
def test_cursor_ttl_stores_timestamps_and_enforces_expiry(ttl: float | timedelta) -> None:
    with patch("sqlspec.core._cursor.time.time", side_effect=[1000.0, 1059.5, 1059.9, 1060.0]):
        token = encode_cursor([42], "o", ttl=ttl)
        payload = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
        assert payload["i"] == 1000.0
        assert math.isclose(payload["e"], 1060.0)

        assert decode_cursor(token, "o", 1).values == (42,)
        assert decode_cursor(token, "o", 1, ttl=ttl).values == (42,)
        with pytest.raises(InvalidCursorError, match="expired"):
            decode_cursor(token, "o", 1)


def test_decode_rejects_token_without_timestamps_when_ttl_required() -> None:
    token = encode_cursor([42], "o")
    with pytest.raises(InvalidCursorError, match="expired"):
        decode_cursor(token, "o", 1, ttl=60)


@pytest.mark.parametrize(
    "payload",
    [
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": float("nan")},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": float("inf")},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": "bad"},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": True},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": 10**400},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "e": -(10**400)},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "i": float("nan")},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "i": "bad"},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "i": True},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "i": 10**400},
        {"v": 1, "d": "f", "o": "o", "k": [["s", "1"]], "i": -(10**400)},
    ],
)
def test_decode_rejects_malformed_expiration_claim(payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, separators=(",", ":"), allow_nan=True).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    with pytest.raises(InvalidCursorError, match="Invalid pagination cursor: malformed token"):
        decode_cursor(body, "o", 1)


@pytest.mark.parametrize("bad_secret", [None, b""])
def test_encrypt_requires_non_empty_secret(bad_secret: bytes | None) -> None:
    with pytest.raises(ValueError, match="secret"):
        encode_cursor([1], "o", secret=bad_secret, encrypt=True)
    with pytest.raises(ValueError, match="secret"):
        decode_cursor("e1.AAAA.AAAA", "o", 1, secret=bad_secret, encrypt=True)


def test_encrypted_cursor_roundtrip_hides_plaintext_and_uses_fresh_nonce() -> None:
    secret = b"0123456789abcdef0123456789abcdef"
    with patch("sqlspec.core._cursor.time.time", return_value=2000.0):
        t1 = encode_cursor(["alice", 99], "o", secret=secret, ttl=60, encrypt=True)
        t2 = encode_cursor(["alice", 99], "o", secret=secret, ttl=60, encrypt=True)
        assert t1.startswith("e1.")
        assert t2.startswith("e1.")
        assert len(t1.split(".")) == 3
        assert t1 != t2
        raw_ciphertext = base64.urlsafe_b64decode(t1.split(".")[1] + "===")
        assert b"alice" not in raw_ciphertext
        decoded = decode_cursor(t1, "o", 2, secret=secret, ttl=60, encrypt=True)
        assert decoded.values == ("alice", 99)


@pytest.mark.parametrize("tamper", ["ciphertext", "mac", "truncated_nonce", "wrong_secret"])
def test_encrypted_cursor_rejects_tampering(tamper: str) -> None:
    secret = b"secret-key-material"
    token = encode_cursor(["alice"], "o", secret=secret, encrypt=True)
    version, body_b64, mac_b64 = token.split(".")
    if tamper == "ciphertext":
        raw = bytearray(base64.urlsafe_b64decode(body_b64 + "==="))
        raw[-1] ^= 0xFF
        mutated = f"{version}.{base64.urlsafe_b64encode(bytes(raw)).rstrip(b'=').decode('ascii')}.{mac_b64}"
        with pytest.raises(InvalidCursorError):
            decode_cursor(mutated, "o", 1, secret=secret, encrypt=True)
    elif tamper == "mac":
        raw_mac = bytearray(base64.urlsafe_b64decode(mac_b64 + "==="))
        raw_mac[0] ^= 0xFF
        mutated = f"{version}.{body_b64}.{base64.urlsafe_b64encode(bytes(raw_mac)).rstrip(b'=').decode('ascii')}"
        with pytest.raises(InvalidCursorError):
            decode_cursor(mutated, "o", 1, secret=secret, encrypt=True)
    elif tamper == "truncated_nonce":
        short_body = base64.urlsafe_b64encode(b"short").rstrip(b"=").decode("ascii")
        with pytest.raises(InvalidCursorError):
            decode_cursor(f"{version}.{short_body}.{mac_b64}", "o", 1, secret=secret, encrypt=True)
    else:
        with pytest.raises(InvalidCursorError):
            decode_cursor(token, "o", 1, secret=b"different-secret", encrypt=True)


def test_cursor_mode_mismatch_rejected() -> None:
    secret = b"secret-key-material"
    signed = encode_cursor([1], "o", secret=secret, encrypt=False)
    unsigned = encode_cursor([1], "o")
    encrypted = encode_cursor([1], "o", secret=secret, encrypt=True)

    with pytest.raises(InvalidCursorError):
        decode_cursor(signed, "o", 1, secret=secret, encrypt=True)
    with pytest.raises(InvalidCursorError):
        decode_cursor(unsigned, "o", 1, secret=secret, encrypt=True)
    with pytest.raises(InvalidCursorError):
        decode_cursor(encrypted, "o", 1, secret=secret, encrypt=False)
    with pytest.raises(InvalidCursorError):
        decode_cursor(encrypted, "o", 1, secret=None, encrypt=False)


@pytest.mark.parametrize(
    ("tag", "raw_value"),
    [
        ("dt", "not-a-datetime"),
        ("da", "not-a-date"),
        ("t", "not-a-time"),
        ("d", "not-a-decimal"),
        ("f", "nan"),
        ("u", "not-a-uuid"),
        ("y", "!"),
        ("td", "9999999999:0:0"),
    ],
)
def test_decode_conversion_errors_map_to_invalid_cursor_error(tag: str, raw_value: str) -> None:
    payload = {"v": 1, "d": "f", "o": "o", "k": [[tag, raw_value]]}
    body = (
        base64
        .urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        .rstrip(b"=")
        .decode("ascii")
    )
    with pytest.raises(InvalidCursorError):
        decode_cursor(body, "o", 1)
