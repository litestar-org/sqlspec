"""Tests for declared parameter metadata + type registry (Ch1, sqlspec-smgc.1)."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from sqlspec.core.parameters._declared import (
    ParameterDeclaration,
    matches_param_type,
    register_param_type,
    resolve_param_type,
)


def test_declaration_fields() -> None:
    decl = ParameterDeclaration(name="status_cd", type_str="str")
    assert decl.name == "status_cd"
    assert decl.type_str == "str"
    assert decl.description is None


def test_declaration_with_description() -> None:
    decl = ParameterDeclaration("limit", "int", description="Max rows")
    assert decl.description == "Max rows"


def test_optional_declaration_marks_required_false() -> None:
    decl = ParameterDeclaration("status_cd", "str", required=False)
    assert decl.required is False


def test_declaration_equality_and_hash() -> None:
    a = ParameterDeclaration("a", "int")
    b = ParameterDeclaration("a", "int")
    c = ParameterDeclaration("a", "int", description="differs")
    d = ParameterDeclaration("a", "int", required=False)
    assert a == b
    assert a != c
    assert a != d
    assert a != "not-a-declaration"
    assert hash(a) == hash(b)


@pytest.mark.parametrize(
    ("type_str", "expected"),
    [
        ("str", str),
        ("int", int),
        ("float", float),
        ("bool", bool),
        ("bytes", bytes),
        ("date", date),
        ("datetime", datetime),
        ("Decimal", Decimal),
        ("uuid", UUID),
        ("UUID", UUID),
        ("uuid.UUID", UUID),
        ("dict", dict),
        ("dict[str, Any]", dict),
        ("list[int]", list),
        ("list[str]", list),
        ("list", list),
    ],
)
def test_resolve_known_types(type_str: str, expected: type) -> None:
    assert resolve_param_type(type_str) is expected


def test_resolve_is_case_and_whitespace_insensitive() -> None:
    assert resolve_param_type("  LIST[ INT ]  ") is list
    assert resolve_param_type("INT") is int


def test_json_type_uses_serializer_backed_matcher() -> None:
    assert matches_param_type("json", {"ok": ["nested"]})
    assert matches_param_type("jsonb", ["ok"])
    assert not matches_param_type("json", {"bad": object()})
    assert not matches_param_type("json", object())


def test_resolve_unknown_returns_none() -> None:
    assert resolve_param_type("Money") is None
    assert resolve_param_type("frobnicate") is None


def test_register_param_type_adds_and_resolves() -> None:
    assert resolve_param_type("Money") is None
    register_param_type("Money", Decimal)
    try:
        assert resolve_param_type("Money") is Decimal
        assert resolve_param_type("money") is Decimal  # case-insensitive
    finally:
        # keep global registry clean for other tests
        from sqlspec.core.parameters._declared import _TYPE_REGISTRY

        _TYPE_REGISTRY.pop("money", None)


def test_public_exports() -> None:
    from sqlspec import ParameterDeclaration as TopDecl
    from sqlspec import matches_param_type as top_matches
    from sqlspec import register_param_type as top_register

    assert TopDecl is ParameterDeclaration
    assert top_matches is matches_param_type
    assert top_register is register_param_type
