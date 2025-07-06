"""AST hashing utilities for cache key generation."""

from typing import Any, Optional

from sqlglot import exp


def hash_expression(expr: Optional[exp.Expression], _seen: Optional[set[int]] = None) -> int:
    """Generate deterministic hash from AST structure.

    Args:
        expr: SQLGlot Expression to hash
        _seen: Set of seen object IDs to handle circular references

    Returns:
        Deterministic hash of the AST structure
    """
    if expr is None:
        return hash(None)

    if _seen is None:
        _seen = set()

    expr_id = id(expr)
    if expr_id in _seen:
        return hash(expr_id)

    _seen.add(expr_id)

    # Build hash from type and args
    components: list[Any] = [type(expr).__name__]

    for key, value in sorted(expr.args.items()):
        components.extend((key, _hash_value(value, _seen)))

    return hash(tuple(components))


def _hash_value(value: Any, _seen: set[int]) -> int:
    """Hash different value types consistently.

    Args:
        value: Value to hash (can be Expression, list, dict, or primitive)
        _seen: Set of seen object IDs to handle circular references

    Returns:
        Deterministic hash of the value
    """
    if isinstance(value, exp.Expression):
        return hash_expression(value, _seen)
    if isinstance(value, list):
        return hash(tuple(_hash_value(v, _seen) for v in value))
    if isinstance(value, dict):
        items = sorted((k, _hash_value(v, _seen)) for k, v in value.items())
        return hash(tuple(items))
    if isinstance(value, tuple):
        return hash(tuple(_hash_value(v, _seen) for v in value))
    # Primitives: str, int, bool, None, etc.
    return hash(value)
