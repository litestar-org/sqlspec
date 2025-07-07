"""Statement hashing utilities for cache key generation.

This module provides centralized hashing logic for SQL statements,
including expressions, parameters, filters, and complete SQL objects.
"""

from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.sql import SQL

__all__ = ("hash_expression", "hash_parameters", "hash_sql_statement")


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


def hash_parameters(
    positional_params: Optional[list[Any]] = None,
    named_params: Optional[dict[str, Any]] = None,
    original_parameters: Optional[Any] = None,
) -> int:
    """Generate hash for SQL parameters.

    Args:
        positional_params: List of positional parameters
        named_params: Dictionary of named parameters
        original_parameters: Original parameters (for execute_many)

    Returns:
        Combined hash of all parameters
    """
    param_hash = 0

    # Hash positional parameters
    if positional_params:
        # Handle unhashable types like lists
        hashable_params = []
        for param in positional_params:
            if isinstance(param, (list, dict)):
                # Convert unhashable types to hashable representations
                hashable_params.append(repr(param))
            else:
                hashable_params.append(param)
        param_hash ^= hash(tuple(hashable_params))

    # Hash named parameters
    if named_params:
        # Handle unhashable types in named params
        hashable_items = []
        for key, value in sorted(named_params.items()):
            if isinstance(value, (list, dict)):
                hashable_items.append((key, repr(value)))
            else:
                hashable_items.append((key, value))
        param_hash ^= hash(tuple(hashable_items))

    # Hash original parameters (important for execute_many)
    if original_parameters is not None:
        if isinstance(original_parameters, list):
            # For execute_many, hash the count and first few items to avoid
            # performance issues with large parameter sets
            param_hash ^= hash(("original_count", len(original_parameters)))
            if original_parameters:
                # Hash first 3 items as representatives
                sample_size = min(3, len(original_parameters))
                sample_hash = hash(repr(original_parameters[:sample_size]))
                param_hash ^= hash(("original_sample", sample_hash))
        else:
            param_hash ^= hash(("original", repr(original_parameters)))

    return param_hash


def hash_filters(filters: Optional[list["StatementFilter"]] = None) -> int:
    """Generate hash for statement filters.

    Args:
        filters: List of statement filters

    Returns:
        Hash of the filters
    """
    if not filters:
        return 0

    # Use class names and any hashable attributes
    filter_components = []
    for f in filters:
        # Use class name as primary identifier
        components: list[Any] = [f.__class__.__name__]

        # Add any hashable attributes if available
        if hasattr(f, "__dict__"):
            for key, value in sorted(f.__dict__.items()):
                try:
                    # Try to hash the value
                    hash(value)
                    components.append((key, value))
                except TypeError:  # noqa: PERF203
                    # If not hashable, use repr
                    components.append((key, repr(value)))

        filter_components.append(tuple(components))

    return hash(tuple(filter_components))


def hash_sql_statement(statement: "SQL") -> str:
    """Generate a complete cache key for a SQL statement.

    This centralizes all the complex hashing logic that was previously
    scattered across different parts of the codebase.

    Args:
        statement: SQL statement object

    Returns:
        Cache key string
    """
    from sqlspec.utils.type_guards import is_expression

    # Hash the expression or raw SQL
    if is_expression(statement._statement):
        expr_hash = hash_expression(statement._statement)
    else:
        expr_hash = hash(statement._raw_sql)

    # Hash all parameters
    param_hash = hash_parameters(
        positional_params=statement._positional_params,
        named_params=statement._named_params,
        original_parameters=statement._original_parameters,
    )

    # Hash filters
    filter_hash = hash_filters(statement._filters)

    # Combine with other state
    state_components = [
        expr_hash,
        param_hash,
        filter_hash,
        hash(statement._dialect),  # Use _dialect instead of _config.dialect
        hash(statement._is_many),
        hash(statement._is_script),
    ]

    return f"sql:{hash(tuple(state_components))}"
