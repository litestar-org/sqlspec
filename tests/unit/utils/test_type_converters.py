"""Tests for nested converter helpers."""

import uuid
from decimal import Decimal

import pytest

from sqlspec._typing import UUID_UTILS_INSTALLED
from sqlspec.utils.type_converters import build_nested_decimal_normalizer, build_uuid_coercions

pytestmark = pytest.mark.xdist_group("utils")


def test_nested_decimal_normalizer_preserves_identity_when_unchanged() -> None:
    """Unchanged nested payloads should keep their existing container identities."""
    normalizer = build_nested_decimal_normalizer(mode="float")
    payload = {"items": [1, {"value": "x"}], "meta": ("a", None)}

    normalized = normalizer(payload)

    assert normalized is payload
    assert normalized["items"] is payload["items"]
    assert normalized["items"][1] is payload["items"][1]
    assert normalized["meta"] is payload["meta"]


def test_nested_decimal_normalizer_copies_only_changed_branch() -> None:
    """Nested normalization should allocate only along branches containing Decimal values."""
    normalizer = build_nested_decimal_normalizer(mode="float")
    payload = {"changed": [1, {"value": Decimal("1.5")}], "unchanged": ("a", {"flag": True})}

    normalized = normalizer(payload)

    assert normalized == {"changed": [1, {"value": 1.5}], "unchanged": ("a", {"flag": True})}
    assert normalized is not payload
    assert normalized["changed"] is not payload["changed"]
    assert normalized["changed"][1] is not payload["changed"][1]
    assert normalized["unchanged"] is payload["unchanged"]


def test_nested_decimal_normalizer_supports_sequence_subclasses() -> None:
    """Sequence subclasses should still resolve through the dispatcher cache."""

    class DecimalList(list):
        pass

    normalizer = build_nested_decimal_normalizer(mode="float")
    payload = {"items": DecimalList([Decimal("1.5"), "x"])}

    normalized = normalizer(payload)

    assert normalized == {"items": [1.5, "x"]}


# --- UUID coercion tests ---


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid_utils not installed")
def test_build_uuid_coercions_default_returns_str() -> None:
    """Default mode converts uuid_utils.UUID to str."""
    import uuid_utils

    coercions = build_uuid_coercions()
    assert uuid_utils.UUID in coercions

    u = uuid_utils.uuid7()
    result = coercions[type(u)](u)
    assert isinstance(result, str)
    assert result == str(u)


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid_utils not installed")
def test_build_uuid_coercions_native_returns_stdlib_uuid() -> None:
    """Native mode converts uuid_utils.UUID to uuid.UUID via .bytes."""
    import uuid_utils

    coercions = build_uuid_coercions(native=True)
    assert uuid_utils.UUID in coercions

    u = uuid_utils.uuid7()
    result = coercions[type(u)](u)
    assert type(result) is uuid.UUID
    assert str(result) == str(u)


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid_utils not installed")
def test_build_uuid_coercions_does_not_include_stdlib_uuid() -> None:
    """Neither mode should include uuid.UUID as a key."""
    assert uuid.UUID not in build_uuid_coercions()
    assert uuid.UUID not in build_uuid_coercions(native=True)


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid_utils not installed")
def test_build_uuid_coercions_preserves_uuid_value() -> None:
    """Round-trip through native coercion preserves the UUID value."""
    import uuid_utils

    coercions = build_uuid_coercions(native=True)
    u = uuid_utils.uuid7()
    stdlib_uuid = coercions[type(u)](u)
    assert stdlib_uuid.bytes == u.bytes
    assert stdlib_uuid.int == u.int
