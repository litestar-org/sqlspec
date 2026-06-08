"""Declared parameter metadata for SQL-file ``-- param:`` annotations.

Carries the name, declared type string, required flag, and description parsed from
``-- param: <name> <type>[?] [description]`` directives, plus an extensible registry
that resolves declared type strings to Python types for validation. Resolution is a
pure lookup; declared type strings are never evaluated.
"""

from datetime import date, datetime, time
from decimal import Decimal

__all__ = ("ParameterDeclaration", "register_param_type", "resolve_param_type")


class ParameterDeclaration:
    """A single parameter declared in a SQL file header."""

    __slots__ = ("description", "name", "required", "type_str")

    def __init__(
        self, name: str, type_str: str, required: bool = True, description: "str | None" = None
    ) -> None:
        self.name = name
        self.type_str = type_str
        self.required = required
        self.description = description

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ParameterDeclaration):
            return NotImplemented
        return (
            self.name == other.name
            and self.type_str == other.type_str
            and self.required == other.required
            and self.description == other.description
        )

    def __hash__(self) -> int:
        return hash((self.name, self.type_str, self.required, self.description))

    def __repr__(self) -> str:
        return (
            f"ParameterDeclaration(name={self.name!r}, type_str={self.type_str!r}, "
            f"required={self.required!r}, description={self.description!r})"
        )


_TYPE_REGISTRY: "dict[str, type]" = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "date": date,
    "datetime": datetime,
    "time": time,
    "decimal": Decimal,
    "list[int]": list,
    "list[str]": list,
    "list[float]": list,
    "list[bool]": list,
    "list": list,
    "tuple": tuple,
}


def _normalize_type_key(type_str: str) -> str:
    """Normalize a declared type string to its registry lookup key."""
    return "".join(type_str.split()).lower()


def register_param_type(name: str, py_type: type) -> None:
    """Register or override a declared-type-string to Python-type mapping.

    Args:
        name: The declared type string as written in ``-- param:`` (case-insensitive).
        py_type: The Python type used for ``isinstance`` validation.
    """
    _TYPE_REGISTRY[_normalize_type_key(name)] = py_type


def resolve_param_type(type_str: str) -> "type | None":
    """Resolve a declared type string to a Python type, or ``None`` if unknown.

    Unknown type strings are documentation-only and skipped during validation.
    The declared string is looked up, never evaluated. Parameterized containers
    (``list[int]``) resolve to their origin type (``list``).

    Args:
        type_str: The declared type string from a ``-- param:`` directive.

    Returns:
        The resolved Python type, or ``None`` when not in the registry.
    """
    return _TYPE_REGISTRY.get(_normalize_type_key(type_str))
