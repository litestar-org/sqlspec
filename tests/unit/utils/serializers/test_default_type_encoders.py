"""Tests for ``DEFAULT_TYPE_ENCODERS`` registry and MRO-walking enc_hook.

Mirrors ``advanced-alchemy`` test coverage adapted to sqlspec semantics:
- ``Decimal`` → ``float`` (project precedent), not ``str``.
- Unsupported objects raise ``TypeError`` instead of falling back to ``str``.
"""

from __future__ import annotations

import datetime
import enum
import ipaddress
import json
import uuid
from decimal import Decimal
from pathlib import Path, PurePosixPath

import pytest

from sqlspec.typing import MSGSPEC_INSTALLED, NUMPY_INSTALLED, ORJSON_INSTALLED, PYDANTIC_INSTALLED
from sqlspec.utils.serializers import DEFAULT_TYPE_ENCODERS
from sqlspec.utils.serializers._json import (
    MsgspecSerializer,
    OrjsonSerializer,
    StandardLibSerializer,
    _create_enc_hook,
    encode_json,
)

pytestmark = pytest.mark.xdist_group("utils")


def test_registry_is_public_dict() -> None:
    """DEFAULT_TYPE_ENCODERS is a public, non-empty dict mapping types to callables."""
    assert isinstance(DEFAULT_TYPE_ENCODERS, dict)
    assert DEFAULT_TYPE_ENCODERS
    for key, encoder in DEFAULT_TYPE_ENCODERS.items():
        assert isinstance(key, type)
        assert callable(encoder)


def test_registry_covers_required_types() -> None:
    """Registry covers every type listed in the AC."""
    required = {
        datetime.datetime,
        datetime.date,
        datetime.time,
        datetime.timedelta,
        Decimal,
        uuid.UUID,
        bytes,
        Path,
        ipaddress.IPv4Address,
        ipaddress.IPv4Interface,
        ipaddress.IPv4Network,
        ipaddress.IPv6Address,
        ipaddress.IPv6Interface,
        ipaddress.IPv6Network,
        set,
        frozenset,
        enum.Enum,
    }
    missing = required - set(DEFAULT_TYPE_ENCODERS.keys())
    assert not missing, f"missing required encoders: {missing}"


def test_registry_includes_purepath() -> None:
    """PurePath registration is required for non-OS-bound paths."""
    from pathlib import PurePath

    assert PurePath in DEFAULT_TYPE_ENCODERS


def test_decimal_encodes_to_float_not_str() -> None:
    """sqlspec diverges from AA: Decimal -> float (preserves _normalize_supported_value behaviour)."""
    encoder = DEFAULT_TYPE_ENCODERS[Decimal]
    result = encoder(Decimal("19.99"))
    assert result == pytest.approx(19.99)
    assert isinstance(result, float)


def test_uuid_encodes_to_str() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[uuid.UUID]
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert encoder(value) == str(value)


def test_set_encodes_to_list() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[set]
    assert sorted(encoder({1, 2, 3})) == [1, 2, 3]


def test_frozenset_encodes_to_list() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[frozenset]
    assert sorted(encoder(frozenset({1, 2, 3}))) == [1, 2, 3]


def test_bytes_encodes_to_utf8_string() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[bytes]
    assert encoder(b"hello") == "hello"


def test_ipv4_address_encodes_to_str() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[ipaddress.IPv4Address]
    assert encoder(ipaddress.IPv4Address("192.0.2.1")) == "192.0.2.1"


def test_ipv6_network_encodes_to_str() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[ipaddress.IPv6Network]
    assert encoder(ipaddress.IPv6Network("2001:db8::/32")) == "2001:db8::/32"


def test_path_encodes_to_str() -> None:
    encoder = DEFAULT_TYPE_ENCODERS[Path]
    assert encoder(Path("/tmp/foo")) == str(Path("/tmp/foo"))


def test_enum_encodes_to_value() -> None:
    class Color(enum.Enum):
        RED = "red"

    encoder = DEFAULT_TYPE_ENCODERS[enum.Enum]
    assert encoder(Color.RED) == "red"


@pytest.mark.skipif(not PYDANTIC_INSTALLED, reason="Pydantic not installed")
def test_pydantic_basemodel_registered() -> None:
    from pydantic import BaseModel

    assert BaseModel in DEFAULT_TYPE_ENCODERS


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
def test_numpy_types_registered() -> None:
    import numpy as np

    assert np.ndarray in DEFAULT_TYPE_ENCODERS
    assert np.generic in DEFAULT_TYPE_ENCODERS


def test_create_enc_hook_walks_mro_for_intenum() -> None:
    """IntEnum has no explicit registration; it must resolve via Enum on the MRO."""

    class HttpStatus(enum.IntEnum):
        OK = 200

    enc_hook = _create_enc_hook(DEFAULT_TYPE_ENCODERS)
    assert enc_hook(HttpStatus.OK) == 200


def test_create_enc_hook_resolves_subclass_of_enum() -> None:
    """Custom Enum subclasses should resolve via the Enum encoder."""

    class Currency(enum.Enum):
        USD = "USD"

    enc_hook = _create_enc_hook(DEFAULT_TYPE_ENCODERS)
    assert enc_hook(Currency.USD) == "USD"


def test_create_enc_hook_user_override_wins() -> None:
    """A user-supplied registry entry takes precedence over the default."""
    overrides = {**DEFAULT_TYPE_ENCODERS, datetime.datetime: lambda _: "OVERRIDE"}
    enc_hook = _create_enc_hook(overrides)
    assert enc_hook(datetime.datetime(2026, 1, 1)) == "OVERRIDE"


def test_create_enc_hook_raises_on_unknown_type() -> None:
    """Strict sqlspec semantics: no MRO match → TypeError mentioning 'unsupported'."""
    enc_hook = _create_enc_hook(DEFAULT_TYPE_ENCODERS)
    with pytest.raises(TypeError, match="unsupported"):
        enc_hook(object())


@pytest.mark.skipif(not MSGSPEC_INSTALLED, reason="msgspec not installed")
def test_msgspec_serializer_accepts_type_encoders_override() -> None:
    """Constructor-level override: pass type_encoders to MsgspecSerializer."""

    class Point:
        def __init__(self, x: int, y: int) -> None:
            self.x = x
            self.y = y

    serializer = MsgspecSerializer(type_encoders={Point: lambda p: [p.x, p.y]})
    encoded = serializer.encode({"point": Point(1, 2)})
    assert json.loads(encoded) == {"point": [1, 2]}


@pytest.mark.skipif(not ORJSON_INSTALLED, reason="orjson not installed")
def test_orjson_serializer_accepts_type_encoders_override() -> None:
    class Point:
        def __init__(self, x: int, y: int) -> None:
            self.x = x
            self.y = y

    serializer = OrjsonSerializer(type_encoders={Point: lambda p: {"x": p.x, "y": p.y}})
    encoded = serializer.encode({"point": Point(3, 4)})
    assert json.loads(encoded) == {"point": {"x": 3, "y": 4}}


def test_stdlib_serializer_accepts_type_encoders_override() -> None:
    class Tag:
        def __init__(self, name: str) -> None:
            self.name = name

    serializer = StandardLibSerializer(type_encoders={Tag: lambda t: t.name})
    encoded = serializer.encode({"tag": Tag("vip")})
    assert json.loads(encoded) == {"tag": "vip"}


def test_offset_pagination_round_trip_via_dataclass_branch() -> None:
    """OffsetPagination is a dataclass; encoding goes through the dataclass tail probe."""
    from sqlspec.core._pagination import OffsetPagination

    page: OffsetPagination[int] = OffsetPagination(items=[1, 2, 3], limit=10, offset=0, total=3)
    assert json.loads(encode_json(page)) == {"items": [1, 2, 3], "limit": 10, "offset": 0, "total": 3}


def test_purepath_subclass_resolves_via_mro() -> None:
    """PurePosixPath instance must resolve via PurePath registration on its MRO."""
    enc_hook = _create_enc_hook(DEFAULT_TYPE_ENCODERS)
    assert enc_hook(PurePosixPath("/etc/hosts")) == "/etc/hosts"


def test_behavioural_equivalence_battery() -> None:
    """The registry drives stdlib encoding through the MRO enc_hook.

    Uses ``StandardLibSerializer`` directly so every value flows through the
    registry (msgspec/orjson encode some of these natively before enc_hook is
    consulted).
    """
    serializer = StandardLibSerializer()
    payload = {
        "ts": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        "d": datetime.date(2026, 1, 1),
        "t": datetime.time(12, 30, 0),
        "td": datetime.timedelta(seconds=90),
        "dec": Decimal("1.5"),
        "uid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "raw": b"abc",
        "p": Path("/tmp/x"),
        "ip": ipaddress.IPv4Address("10.0.0.1"),
        "s": {1, 2, 3},
    }

    decoded = json.loads(serializer.encode(payload))
    assert decoded["ts"] == "2026-01-01T00:00:00Z"
    assert decoded["d"] == "2026-01-01"
    assert decoded["t"] == "12:30:00"
    assert decoded["td"] == 90.0
    assert decoded["dec"] == pytest.approx(1.5)
    assert decoded["uid"] == "12345678-1234-5678-1234-567812345678"
    assert decoded["raw"] == "abc"
    assert decoded["p"] == str(Path("/tmp/x"))
    assert decoded["ip"] == "10.0.0.1"
    assert sorted(decoded["s"]) == [1, 2, 3]
