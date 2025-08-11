"""Statement hashing utilities for cache key generation.

This module provides centralized hashing logic for SQL statements,
including expressions, parameters, filters, and complete SQL objects.

Also supports fine-grained AST sub-expression caching.
"""

from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.utils.type_guards import is_typed_parameter

if TYPE_CHECKING:
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import SQL

__all__ = (
    "hash_expression",
    "hash_expression_node",
    "hash_optimized_expression",
    "hash_parameters",
    "hash_sql_statement",
)


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
    positional_parameters: Optional[list[Any]] = None,
    named_parameters: Optional[dict[str, Any]] = None,
    original_parameters: Optional[Any] = None,
) -> int:
    """Generate hash for SQL parameters.

    Args:
        positional_parameters: List of positional parameters
        named_parameters: Dictionary of named parameters
        original_parameters: Original parameters (for execute_many)

    Returns:
        Combined hash of all parameters
    """
    param_hash = 0

    # Hash positional parameters
    if positional_parameters:
        from sqlspec.core.parameters import TypedParameter

        hashable_parameters: list[tuple[Any, Any]] = []
        for param in positional_parameters:
            if isinstance(param, TypedParameter):
                if isinstance(param.value, (list, dict)):
                    hashable_parameters.append((repr(param.value), param.original_type))
                else:
                    hashable_parameters.append((param.value, param.original_type))
            elif isinstance(param, (list, dict)):
                # Convert unhashable types to hashable representations
                hashable_parameters.append((repr(param), "unhashable"))
            else:
                # Check if param itself contains unhashable types
                try:
                    hash(param)
                    hashable_parameters.append((param, "primitive"))
                except TypeError:
                    # If unhashable, convert to string representation
                    hashable_parameters.append((repr(param), "unhashable_repr"))

        param_hash ^= hash(tuple(hashable_parameters))

    if named_parameters:
        # Handle unhashable types in named parameters
        hashable_items = []
        for key, value in sorted(named_parameters.items()):
            if is_typed_parameter(value):
                # For TypedParameter, hash its value with type info
                if isinstance(value.value, (list, dict)):
                    hashable_items.append((key, (repr(value.value), value.original_type)))
                else:
                    hashable_items.append((key, (value.value, value.original_type)))
            elif isinstance(value, (list, dict)):
                hashable_items.append((key, (repr(value), "unhashable")))
            else:
                hashable_items.append((key, (value, "primitive")))
        param_hash ^= hash(tuple(hashable_items))

    # Hash original parameters (important for execute_many)
    if original_parameters is not None:
        if isinstance(original_parameters, list):
            # For execute_many, hash the count and first few items to avoid
            param_hash ^= hash(("original_count", len(original_parameters)))
            if original_parameters:
                # Hash first 3 items as representatives
                sample_size = min(3, len(original_parameters))
                sample_hash = hash(repr(original_parameters[:sample_size]))
                param_hash ^= hash(("original_sample", sample_hash))
        else:
            param_hash ^= hash(("original", repr(original_parameters)))

    return param_hash


def _hash_filter_value(value: Any) -> int:
    try:
        return hash(value)
    except TypeError:
        return hash(repr(value))


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
        # Use getattr with default instead of hasattr for mypyc compatibility
        filter_dict = getattr(f, "__dict__", None)
        if filter_dict is not None:
            for key, value in sorted(filter_dict.items()):
                components.append((key, _hash_filter_value(value)))

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
        positional_parameters=statement._positional_parameters,
        named_parameters=statement._named_parameters,
        original_parameters=statement._original_parameters,
    )

    # Hash filters
    filter_hash = hash_filters(statement._filters)

    # Combine with other state
    state_components = [
        expr_hash,
        param_hash,
        filter_hash,
        hash(statement._dialect),
        hash(statement._is_many),
        hash(statement._is_script),
    ]

    return f"sql:{hash(tuple(state_components))}"


def hash_expression_node(node: exp.Expression, include_children: bool = True, dialect: Optional[str] = None) -> str:
    """Generate a cache key for an individual expression node.

    This is used for sub-expression caching where we want to cache
    frequently used SQL fragments like complex JOIN clauses or WHERE conditions.

    Args:
        node: The expression node to hash
        include_children: Whether to include child nodes in the hash
        dialect: SQL dialect for context-aware hashing

    Returns:
        Cache key string for the expression node
    """
    if include_children:
        # Full structural hash including all children
        node_hash = hash_expression(node)
    else:
        # Shallow hash - just the node type and immediate args
        components: list[Any] = [type(node).__name__]
        for key, value in sorted(node.args.items()):
            if not isinstance(value, (list, exp.Expression)):
                # Only include primitive values, not child expressions
                components.extend((key, hash(value)))
        node_hash = hash(tuple(components))

    dialect_part = f":{dialect}" if dialect else ""
    return f"expr{dialect_part}:{node_hash}"


def hash_optimized_expression(
    expr: exp.Expression,
    dialect: str,
    schema: Optional[dict[str, Any]] = None,
    optimizer_settings: Optional[dict[str, Any]] = None,
) -> str:
    """Generate a cache key for optimized expressions.

    This creates a unique key that captures the expression structure,
    dialect, schema context, and optimizer settings to ensure we only
    reuse optimized expressions when all context matches.

    Args:
        expr: The unoptimized expression
        dialect: Target SQL dialect
        schema: Schema information
        optimizer_settings: Additional optimizer configuration

    Returns:
        Cache key string for the optimized expression
    """
    # Base expression hash
    expr_hash = hash_expression(expr)

    # Schema hash - simplified representation
    schema_hash = 0
    if schema:
        # Hash table names and column counts to avoid deep schema hashing
        schema_items = []
        for table_name, table_schema in sorted(schema.items()):
            if isinstance(table_schema, dict):
                schema_items.append((table_name, len(table_schema)))
            else:
                schema_items.append((table_name, hash("unknown")))
        schema_hash = hash(tuple(schema_items))

    # Optimizer settings hash
    settings_hash = 0
    if optimizer_settings:
        settings_items = sorted(optimizer_settings.items())
        settings_hash = hash(tuple(settings_items))

    # Combine all components
    components = (expr_hash, dialect, schema_hash, settings_hash)
    return f"opt:{hash(components)}"
