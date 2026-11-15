"""SQL result classes and helpers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.core.result._base import (
        ArrowResult,
        DMLResult,
        EmptyResult,
        SQLResult,
        StackResult,
        StatementResult,
        build_arrow_result_from_reader,
        build_arrow_result_from_table,
        create_arrow_result,
        create_sql_result,
    )

__all__ = (
    "ArrowResult",
    "DMLResult",
    "EmptyResult",
    "SQLResult",
    "StackResult",
    "StatementResult",
    "build_arrow_result_from_reader",
    "build_arrow_result_from_table",
    "create_arrow_result",
    "create_sql_result",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "ArrowResult": ("sqlspec.core.result._base", "ArrowResult"),
    "DMLResult": ("sqlspec.core.result._base", "DMLResult"),
    "EmptyResult": ("sqlspec.core.result._base", "EmptyResult"),
    "SQLResult": ("sqlspec.core.result._base", "SQLResult"),
    "StackResult": ("sqlspec.core.result._base", "StackResult"),
    "StatementResult": ("sqlspec.core.result._base", "StatementResult"),
    "build_arrow_result_from_reader": ("sqlspec.core.result._base", "build_arrow_result_from_reader"),
    "build_arrow_result_from_table": ("sqlspec.core.result._base", "build_arrow_result_from_table"),
    "create_arrow_result": ("sqlspec.core.result._base", "create_arrow_result"),
    "create_sql_result": ("sqlspec.core.result._base", "create_sql_result"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
