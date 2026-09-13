"""Tests for TypeCoercionCapabilities and configuration integration."""

import copy
import pickle

import pytest

from sqlspec.config import DatabaseConfigBase, DatabaseConfigProtocol
from sqlspec.core.capabilities import TypeCoercionCapabilities


def test_capabilities_construction() -> None:
    """Verify capabilities object initializes with given values."""
    cap = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="millisecond", json_columns_decoded=True, uuid_binding="text"
    )
    assert cap.datetime_binding == "iso_text"
    assert cap.timestamp_precision == "millisecond"
    assert cap.json_columns_decoded is True
    assert cap.uuid_binding == "text"


def test_capabilities_frozen_and_slots() -> None:
    """Verify capabilities is a slots class and immutable."""
    cap = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    assert not hasattr(cap, "__dict__")

    with pytest.raises(AttributeError):
        cap.datetime_binding = "iso_text"  # type: ignore[misc]

    with pytest.raises(AttributeError):
        cap.new_field = "test"  # type: ignore[attr-defined]

    with pytest.raises(AttributeError):
        del cap.datetime_binding  # type: ignore[misc]


def test_capabilities_equality_and_hash() -> None:
    """Verify equality, inequality, and hashing behavior."""
    cap1 = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    cap2 = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    cap3 = TypeCoercionCapabilities(
        datetime_binding="naive_utc",
        timestamp_precision="microsecond",
        json_columns_decoded=False,
        uuid_binding="native",
    )

    assert cap1 == cap2
    assert cap1 != cap3
    assert cap1 != "not_a_capability"
    assert hash(cap1) == hash(cap2)
    assert len({cap1, cap2, cap3}) == 2


def test_capabilities_repr() -> None:
    """Verify repr representation matches expected format."""
    cap = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    expected = (
        "TypeCoercionCapabilities("
        "datetime_binding='native', "
        "timestamp_precision='microsecond', "
        "json_columns_decoded=False, "
        "uuid_binding='native')"
    )
    assert repr(cap) == expected


def test_capabilities_copy_and_pickle() -> None:
    """Verify capabilities object survives copy and pickle roundtrips."""
    cap = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="second", json_columns_decoded=True, uuid_binding="text"
    )
    copied = copy.copy(cap)
    assert copied == cap
    pickled = pickle.loads(pickle.dumps(cap))
    assert pickled == cap


def test_database_config_default_capabilities() -> None:
    """Verify default type coercion capabilities on base config protocol."""
    default_cap = DatabaseConfigProtocol.type_coercion_capabilities
    assert default_cap.datetime_binding == "native"
    assert default_cap.timestamp_precision == "microsecond"
    assert default_cap.json_columns_decoded is False
    assert default_cap.uuid_binding == "native"
    assert DatabaseConfigBase.type_coercion_capabilities == default_cap
