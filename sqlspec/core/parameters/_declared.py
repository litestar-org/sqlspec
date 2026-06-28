"""Declared parameter metadata for SQL-file ``-- param:`` annotations.

Carries the name, declared type string, and description parsed from
``-- param: <name> <type> [description]`` directives, plus an extensible registry
that resolves declared type strings to Python types for validation. Resolution is a
pure lookup; declared type strings are never evaluated.
"""

from collections.abc import Callable
from datetime import date, datetime, time
from decimal import Decimal
from typing import Final, TypeAlias
from uuid import UUID

from sqlspec.utils.serializers import to_json

__all__ = (
    "ParamTypeMatcher",
    "ParameterDeclaration",
    "matches_param_type",
    "register_param_type",
    "resolve_param_type",
)

ParamTypeMatcher: TypeAlias = type | tuple[type, ...] | Callable[[object], bool]


_JSON_VALUE_TYPES: "Final[tuple[type, ...]]" = (dict, list, str, int, float, bool)


def _is_json_value(value: object) -> bool:
    """Return whether a value can be encoded by SQLSpec's JSON serializer."""
    if not isinstance(value, _JSON_VALUE_TYPES):
        return False
    try:
        to_json(value)
    except (TypeError, ValueError):
        return False
    return True


_TYPE_REGISTRY: "Final[dict[str, ParamTypeMatcher]]" = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "date": date,
    "datetime": datetime,
    "time": time,
    "decimal": Decimal,
    "uuid": UUID,
    "uuid.uuid": UUID,
    "dict": dict,
    "dict[str,any]": dict,
    "dict[str,object]": dict,
    "json": _is_json_value,
    "jsonb": _is_json_value,
    "list[int]": list,
    "list[str]": list,
    "list[float]": list,
    "list[bool]": list,
    "list": list,
    "tuple": tuple,
}


class ParameterDeclaration:
    """A single parameter declared in a SQL file header."""

    __slots__ = ("description", "name", "required", "type_str")

    def __init__(self, name: str, type_str: str, description: "str | None" = None, *, required: bool = True) -> None:
        self.name = name
        self.type_str = type_str
        self.description = description
        self.required = required

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ParameterDeclaration):
            return NotImplemented
        return (
            self.name == other.name
            and self.type_str == other.type_str
            and self.description == other.description
            and self.required == other.required
        )

    def __hash__(self) -> int:
        return hash((self.name, self.type_str, self.description, self.required))

    def __repr__(self) -> str:
        return (
            f"ParameterDeclaration(name={self.name!r}, type_str={self.type_str!r}, "
            f"description={self.description!r}, required={self.required!r})"
        )


def _normalize_type_key(type_str: str) -> str:
    """Normalize a declared type string to its registry lookup key."""
    return "".join(type_str.split()).lower()


def register_param_type(name: str, py_type: ParamTypeMatcher) -> None:
    """Register or override a declared-type-string matcher.

    Args:
        name: The declared type string as written in ``-- param:`` (case-insensitive).
        py_type: The Python type, tuple of types, or predicate used for validation.
    """
    _TYPE_REGISTRY[_normalize_type_key(name)] = py_type


def resolve_param_type(type_str: str) -> "ParamTypeMatcher | None":
    """Resolve a declared type string to a matcher, or ``None`` if unknown.

    Unknown type strings are documentation-only and skipped during validation.
    The declared string is looked up, never evaluated. Parameterized containers
    (``list[int]``) resolve to their origin type (``list``).

    Args:
        type_str: The declared type string from a ``-- param:`` directive.

    Returns:
        The resolved matcher, or ``None`` when not in the registry.
    """
    return _TYPE_REGISTRY.get(_normalize_type_key(type_str))


def matches_param_type(type_str: str, value: object) -> bool:
    """Return whether a value satisfies a declared type string.

    Unknown type strings are documentation-only and always match. ``None`` is
    handled by the driver as SQL ``NULL`` before this helper is called.
    """
    resolved = resolve_param_type(type_str)
    if resolved is None:
        return True
    if isinstance(resolved, tuple):
        return isinstance(value, resolved)
    if isinstance(resolved, type):
        return isinstance(value, resolved)
    return resolved(value)
