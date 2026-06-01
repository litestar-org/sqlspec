"""Regression tests for sqlspec.typing portability."""

import typing
from collections.abc import Mapping
from pathlib import Path


def test_typing_module_source_does_not_reference_private_typeddict() -> None:
    """sqlspec.typing must not import or reference typing._TypedDict."""
    assert "_TypedDict" not in Path("sqlspec/typing.py").read_text()


def test_supported_schema_model_includes_mapping() -> None:
    """SupportedSchemaModel should include Mapping[str, Any]."""
    from sqlspec.typing import SupportedSchemaModel

    args = typing.get_args(SupportedSchemaModel)

    assert any(getattr(arg, "__origin__", None) is Mapping for arg in args)


def test_typing_module_has_no_private_typeddict_name() -> None:
    """sqlspec.typing should not expose the private _TypedDict name."""
    import sqlspec.typing as typing_module

    assert not hasattr(typing_module, "_TypedDict")
