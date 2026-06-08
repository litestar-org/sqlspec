"""Tests for declared parameter metadata + type registry (Ch1, sqlspec-smgc.1)."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from sqlspec.core.parameters._declared import ParameterDeclaration, register_param_type, resolve_param_type


def test_declaration_fields() -> None:
    decl = ParameterDeclaration(name="status_cd", type_str="str")
    assert decl.name == "status_cd"
    assert decl.type_str == "str"
    assert decl.description is None


def test_declaration_with_description() -> None:
    decl = ParameterDeclaration("limit", "int", description="Max rows")
    assert decl.description == "Max rows"


def test_declaration_equality_and_hash() -> None:
    a = ParameterDeclaration("a", "int")
    b = ParameterDeclaration("a", "int")
    c = ParameterDeclaration("a", "int", description="differs")
    assert a == b
    assert a != c
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
    from sqlspec import register_param_type as top_register

    assert TopDecl is ParameterDeclaration
    assert top_register is register_param_type
