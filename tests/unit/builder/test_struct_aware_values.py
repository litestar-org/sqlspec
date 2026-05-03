"""Struct-aware INSERT/UPDATE builder tests (chapter 2 of struct-aware-serialization-builder).

Covers ``Insert.values_from``, ``Insert.values_from_many``, ``Update.set_from`` against
dicts, dataclasses, msgspec Structs (with ``rename=``), Pydantic models (with ``Field(alias=)``),
and attrs classes (with ``field(alias=)``). The contract is that all five schema kinds
produce identical Python-attribute-name keys regardless of any wire-rename meta.
"""

from collections.abc import Callable
from typing import Any

import pytest

from sqlspec import sql
from sqlspec.utils.serializers import reset_serializer_cache

pytestmark = pytest.mark.xdist_group("builder")


@pytest.fixture(autouse=True)
def _reset_serializer_cache() -> "Any":
    reset_serializer_cache()
    yield
    reset_serializer_cache()


def test_insert_values_from_msgspec_struct_with_rename_uses_python_names() -> None:
    """Regression: msgspec Struct with rename="camel" must NOT leak camelCase column names."""
    import msgspec

    class _User(msgspec.Struct, rename="camel"):
        user_id: str
        display_name: str

    obj = _User(user_id="abc-123", display_name="Cody")
    stmt = sql.insert("users").values_from(obj).build()

    assert "INSERT INTO" in stmt.sql
    assert "userId" not in stmt.sql
    assert "displayName" not in stmt.sql

    assert stmt.parameters["user_id"] == "abc-123"
    assert stmt.parameters["display_name"] == "Cody"


def test_insert_values_from_many_dataclasses_emits_multi_row() -> None:
    """values_from_many must produce a multi-row INSERT keyed by Python attribute names."""
    from dataclasses import dataclass

    @dataclass
    class _User:
        user_id: str
        name: str

    users = [_User("u1", "Alice"), _User("u2", "Bob"), _User("u3", "Carol")]
    stmt = sql.insert("users").values_from_many(users).build()

    assert "INSERT INTO" in stmt.sql
    assert stmt.parameters["user_id"] == "u1"
    assert stmt.parameters["user_id_1"] == "u2"
    assert stmt.parameters["user_id_2"] == "u3"
    assert stmt.parameters["name"] == "Alice"
    assert stmt.parameters["name_1"] == "Bob"
    assert stmt.parameters["name_2"] == "Carol"


def test_insert_values_from_many_empty_list_returns_self_unchanged() -> None:
    """Empty input must return the builder unchanged (matches values_from_dicts contract)."""
    builder = sql.insert("users")
    result = builder.values_from_many([])
    assert result is builder


def test_update_set_from_msgspec_struct_with_rename_uses_python_names() -> None:
    """set_from must emit Python attribute names regardless of msgspec rename meta."""
    import msgspec

    class _UserPatch(msgspec.Struct, rename="camel"):
        display_name: str
        email: str

    patch = _UserPatch(display_name="Updated", email="new@example.com")
    stmt = sql.update("users").set_from(patch).where_eq("id", 42).build()

    assert "UPDATE" in stmt.sql
    assert "displayName" not in stmt.sql
    assert stmt.parameters["display_name"] == "Updated"
    assert stmt.parameters["email"] == "new@example.com"
    assert stmt.parameters["id"] == 42


# --- Schema-kind matrix factories ---


def _make_dict() -> "dict[str, str]":
    return {"user_id": "u1", "display_name": "Cody"}


def _make_dataclass() -> "Any":
    from dataclasses import dataclass

    @dataclass
    class _User:
        user_id: str
        display_name: str

    return _User(user_id="u1", display_name="Cody")


def _make_msgspec_struct_with_rename() -> "Any":
    import msgspec

    class _User(msgspec.Struct, rename="camel"):
        user_id: str
        display_name: str

    return _User(user_id="u1", display_name="Cody")


def _make_pydantic_model_with_alias() -> "Any":
    pytest.importorskip("pydantic")
    import pydantic

    class _User(pydantic.BaseModel):
        user_id: str = pydantic.Field(alias="userId")
        display_name: str = pydantic.Field(alias="displayName")

        model_config = pydantic.ConfigDict(populate_by_name=True)

    return _User(userId="u1", displayName="Cody")


def _make_attrs_instance() -> "Any":
    pytest.importorskip("attrs")
    import attrs

    @attrs.define
    class _User:
        user_id: str = attrs.field(alias="userId")
        display_name: str = attrs.field(alias="displayName")

    return _User(userId="u1", displayName="Cody")


SCHEMA_FACTORIES = [
    pytest.param(_make_dict, id="dict"),
    pytest.param(_make_dataclass, id="dataclass"),
    pytest.param(_make_msgspec_struct_with_rename, id="msgspec_rename"),
    pytest.param(_make_pydantic_model_with_alias, id="pydantic_alias"),
    pytest.param(_make_attrs_instance, id="attrs_alias"),
]


@pytest.mark.parametrize("factory", SCHEMA_FACTORIES)
def test_insert_values_from_uses_python_names_for_all_schema_kinds(factory: "Callable[[], Any]") -> None:
    obj = factory()
    stmt = sql.insert("users").values_from(obj, exclude_unset=False).build()

    assert stmt.parameters["user_id"] == "u1"
    assert stmt.parameters["display_name"] == "Cody"
    assert "userId" not in stmt.parameters
    assert "displayName" not in stmt.parameters


@pytest.mark.parametrize("factory", SCHEMA_FACTORIES)
def test_update_set_from_uses_python_names_for_all_schema_kinds(factory: "Callable[[], Any]") -> None:
    obj = factory()
    stmt = sql.update("users").set_from(obj, exclude_unset=False).where_eq("id", 42).build()

    assert stmt.parameters["user_id"] == "u1"
    assert stmt.parameters["display_name"] == "Cody"


@pytest.mark.parametrize("factory", SCHEMA_FACTORIES)
def test_insert_values_from_many_uses_python_names_for_all_schema_kinds(factory: "Callable[[], Any]") -> None:
    obj1 = factory()
    obj2 = factory()
    stmt = sql.insert("users").values_from_many([obj1, obj2], exclude_unset=False).build()

    assert stmt.parameters["user_id"] == "u1"
    assert stmt.parameters["display_name"] == "Cody"
    assert stmt.parameters["user_id_1"] == "u1"
    assert stmt.parameters["display_name_1"] == "Cody"


def test_insert_values_from_msgspec_exclude_unset_filters_unset_fields() -> None:
    """exclude_unset=True must drop UNSET msgspec fields from the resulting INSERT."""
    import msgspec
    from msgspec import UNSET

    class _UserPatch(msgspec.Struct, rename="camel", omit_defaults=False):
        user_id: str = UNSET  # type: ignore[assignment]
        display_name: str = UNSET  # type: ignore[assignment]

    patch = _UserPatch(user_id="u1")
    stmt = sql.insert("users").values_from(patch, exclude_unset=True).build()

    assert stmt.parameters["user_id"] == "u1"
    assert "display_name" not in stmt.parameters
