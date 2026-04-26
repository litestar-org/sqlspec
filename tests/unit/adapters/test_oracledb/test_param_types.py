"""Unit tests for Oracle typed parameter wrappers.

Covers the contract for :class:`OracleBlob`, :class:`OracleClob`, and
:class:`OracleJson` — the three slot-based wrapper classes a power-user can
opt into when binding LOB / JSON parameters with explicit type intent.

Per chapter-2/spec.md §3 T1, the wrappers are pure containers: they hold a
``value`` attribute, expose ``__slots__`` to avoid per-instance ``__dict__``
allocation, and do no type validation in ``__init__`` (validation happens at
the T2 routing site).
"""

import pytest


def test_oracle_clob_class_is_importable_from_param_types_module() -> None:
    """``OracleClob`` lives in ``sqlspec.adapters.oracledb._param_types``."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    assert OracleClob.__module__ == "sqlspec.adapters.oracledb._param_types"


def test_oracle_blob_class_is_importable_from_param_types_module() -> None:
    """``OracleBlob`` lives in ``sqlspec.adapters.oracledb._param_types``."""
    from sqlspec.adapters.oracledb._param_types import OracleBlob

    assert OracleBlob.__module__ == "sqlspec.adapters.oracledb._param_types"


def test_oracle_json_class_is_importable_from_param_types_module() -> None:
    """``OracleJson`` lives in ``sqlspec.adapters.oracledb._param_types``."""
    from sqlspec.adapters.oracledb._param_types import OracleJson

    assert OracleJson.__module__ == "sqlspec.adapters.oracledb._param_types"


def test_param_types_module_all_lists_three_classes() -> None:
    """``__all__`` is the alphabetised triplet of public wrappers."""
    import sqlspec.adapters.oracledb._param_types as module

    assert module.__all__ == ("OracleBlob", "OracleClob", "OracleJson")


def test_oracle_clob_uses_slots() -> None:
    """``OracleClob`` defines ``__slots__`` — instances have no ``__dict__``."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    assert OracleClob.__slots__ == ("value",)
    instance = OracleClob("payload")
    assert not hasattr(instance, "__dict__")


def test_oracle_blob_uses_slots() -> None:
    """``OracleBlob`` defines ``__slots__`` — instances have no ``__dict__``."""
    from sqlspec.adapters.oracledb._param_types import OracleBlob

    assert OracleBlob.__slots__ == ("value",)
    instance = OracleBlob(b"payload")
    assert not hasattr(instance, "__dict__")


def test_oracle_json_uses_slots() -> None:
    """``OracleJson`` defines ``__slots__`` — instances have no ``__dict__``."""
    from sqlspec.adapters.oracledb._param_types import OracleJson

    assert OracleJson.__slots__ == ("value",)
    instance = OracleJson({"a": 1})
    assert not hasattr(instance, "__dict__")


def test_oracle_clob_accepts_str_value() -> None:
    """``OracleClob(str)`` round-trips the original string through ``.value``."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    payload = "hello world"
    assert OracleClob(payload).value is payload


def test_oracle_clob_accepts_bytes_value() -> None:
    """``OracleClob(bytes)`` is allowed — T2 decodes utf-8 at the routing site."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    payload = b"hello world"
    assert OracleClob(payload).value is payload


def test_oracle_blob_accepts_bytes_value() -> None:
    """``OracleBlob(bytes)`` round-trips the original bytes through ``.value``."""
    from sqlspec.adapters.oracledb._param_types import OracleBlob

    payload = b"\x00\x01\x02"
    assert OracleBlob(payload).value is payload


def test_oracle_blob_accepts_str_value() -> None:
    """``OracleBlob(str)`` is allowed — T2 encodes utf-8 at the routing site."""
    from sqlspec.adapters.oracledb._param_types import OracleBlob

    payload = "text"
    assert OracleBlob(payload).value is payload


@pytest.mark.parametrize(
    "payload",
    [{"a": 1}, [1, 2, 3], (1, 2, 3), "raw string", 42, None, True],
    ids=["dict", "list", "tuple", "str", "int", "none", "bool"],
)
def test_oracle_json_accepts_any_value(payload: object) -> None:
    """``OracleJson`` accepts arbitrary payloads — type discipline is at T2."""
    from sqlspec.adapters.oracledb._param_types import OracleJson

    assert OracleJson(payload).value is payload


def test_oracle_clob_rejects_extra_attribute_assignment() -> None:
    """Slot enforcement: assigning a non-``value`` attribute raises ``AttributeError``."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    instance = OracleClob("v")
    with pytest.raises(AttributeError):
        instance.extra = "boom"  # type: ignore[attr-defined]


def test_oracle_blob_rejects_extra_attribute_assignment() -> None:
    from sqlspec.adapters.oracledb._param_types import OracleBlob

    instance = OracleBlob(b"v")
    with pytest.raises(AttributeError):
        instance.extra = "boom"  # type: ignore[attr-defined]


def test_oracle_json_rejects_extra_attribute_assignment() -> None:
    from sqlspec.adapters.oracledb._param_types import OracleJson

    instance = OracleJson({"a": 1})
    with pytest.raises(AttributeError):
        instance.extra = "boom"  # type: ignore[attr-defined]


def test_oracle_clob_value_is_mutable_via_assignment() -> None:
    """``.value`` is a normal slot — reassignment works (used by routing for in-place ops)."""
    from sqlspec.adapters.oracledb._param_types import OracleClob

    instance = OracleClob("first")
    instance.value = "second"
    assert instance.value == "second"
