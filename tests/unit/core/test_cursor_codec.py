"""Cursor wire format and validation contracts."""

import base64
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any
from uuid import UUID

import pytest

from sqlspec.core._cursor import decode_cursor, encode_cursor, order_fingerprint, register_cursor_type
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
        datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc),
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
