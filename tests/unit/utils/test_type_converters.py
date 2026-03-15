"""Tests for nested converter helpers."""

from decimal import Decimal

import pytest

from sqlspec.utils.type_converters import build_nested_decimal_normalizer

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
