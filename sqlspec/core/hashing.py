"""Statement hashing utilities for cache key generation.

Provides hashing functions for SQL statements, expressions, parameters,
filters, and AST sub-expressions.
"""

from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.core.parameters import TypedParameter
from sqlspec.utils.type_guards import is_expression, is_typed_parameter

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import SQL

__all__ = (
    "hash_expression",
    "hash_expression_node",
    "hash_optimized_expression",
    "hash_parameters",
    "hash_sql_statement",
)


def hash_expression(expr: "exp.Expression | None", _seen: "set[int] | None" = None) -> int:
    """Generate hash from AST structure.

    Args:
        expr: SQLGlot Expression to hash
        _seen: Set of seen object IDs to handle circular references

    Returns:
        Hash of the AST structure
    """
    if expr is None:
        return hash(None)

    if _seen is None:
        _seen = set()

    expr_id = id(expr)
    if expr_id in _seen:
        return hash(expr_id)

    _seen.add(expr_id)

    components: list[Any] = [type(expr).__name__]

    for key, value in sorted(expr.args.items()):
        components.extend((key, _hash_value(value, _seen)))

    return hash(tuple(components))


def _hash_value(value: Any, _seen: "set[int]") -> int:
    """Hash different value types.

    Args:
        value: Value to hash (can be Expression, list, dict, or primitive)
        _seen: Set of seen object IDs to handle circular references

    Returns:
        Hash of the value
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

    return hash(value)


def hash_parameters(
    positional_parameters: "list[Any] | None" = None,
    named_parameters: "dict[str, Any] | None" = None,
    original_parameters: Any | None = None,
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

    if positional_parameters:
        hashable_parameters: list[tuple[Any, Any]] = []
        for param in positional_parameters:
            if isinstance(param, TypedParameter):
                if isinstance(param.value, (list, dict)):
                    hashable_parameters.append((repr(param.value), param.original_type))
                else:
                    hashable_parameters.append((param.value, param.original_type))
            elif isinstance(param, (list, dict)):
                hashable_parameters.append((repr(param), "unhashable"))
            else:
                try:
                    hash(param)
                    hashable_parameters.append((param, "primitive"))
                except TypeError:
                    hashable_parameters.append((repr(param), "unhashable_repr"))

        param_hash ^= hash(tuple(hashable_parameters))

    if named_parameters:
        hashable_items: list[tuple[str, tuple[Any, Any]]] = []
        for key, value in sorted(named_parameters.items()):
            if is_typed_parameter(value):
                if isinstance(value.value, (list, dict)):
                    hashable_items.append((key, (repr(value.value), value.original_type)))
                else:
                    hashable_items.append((key, (value.value, value.original_type)))
            elif isinstance(value, (list, dict)):
                hashable_items.append((key, (repr(value), "unhashable")))
            else:
                hashable_items.append((key, (value, "primitive")))
        param_hash ^= hash(tuple(hashable_items))

    if original_parameters is not None:
        if isinstance(original_parameters, list):
            param_hash ^= hash(("original_count", len(original_parameters)))
            if original_parameters:
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


def hash_filters(filters: "Sequence[StatementFilter] | None" = None) -> int:
    """Generate hash for statement filters.

    Args:
        filters: List of statement filters

    Returns:
        Hash of the filters
    """
    if not filters:
        return 0

    return hash(tuple(f.get_cache_key() for f in filters))


def hash_sql_statement(statement: "SQL") -> str:
    """Generate cache key for a SQL statement.

    Args:
        statement: SQL statement object

    Returns:
        Cache key string
    """
    stmt_expr = statement.statement_expression
    expr_hash = hash_expression(stmt_expr) if is_expression(stmt_expr) else hash(statement.raw_sql)

    param_hash = hash_parameters(
        positional_parameters=statement.positional_parameters,
        named_parameters=statement.named_parameters,
        original_parameters=statement.original_parameters,
    )

    filter_hash = hash_filters(statement.filters)

    state_components = [
        expr_hash,
        param_hash,
        filter_hash,
        hash(statement.dialect),
        hash(statement.is_many),
        hash(statement.is_script),
    ]

    return f"sql:{hash(tuple(state_components))}"


def hash_expression_node(node: exp.Expression, include_children: bool = True, dialect: str | None = None) -> str:
    """Generate cache key for an expression node.

    Args:
        node: The expression node to hash
        include_children: Whether to include child nodes in the hash
        dialect: SQL dialect for context-aware hashing

    Returns:
        Cache key string for the expression node
    """
    if include_children:
        node_hash = hash_expression(node)
    else:
        components: list[Any] = [type(node).__name__]
        for key, value in sorted(node.args.items()):
            if not isinstance(value, (list, exp.Expression)):
                components.extend((key, hash(value)))
        node_hash = hash(tuple(components))

    dialect_part = f":{dialect}" if dialect else ""
    return f"expr{dialect_part}:{node_hash}"


def hash_optimized_expression(
    expr: exp.Expression,
    dialect: str,
    schema: "dict[str, Any] | None" = None,
    optimizer_settings: "dict[str, Any] | None" = None,
) -> str:
    """Generate cache key for optimized expressions.

    Creates a key that includes expression structure, dialect, schema
    context, and optimizer settings.

    Args:
        expr: The unoptimized expression
        dialect: Target SQL dialect
        schema: Schema information
        optimizer_settings: Additional optimizer configuration

    Returns:
        Cache key string for the optimized expression
    """

    expr_hash = hash_expression(expr)

    schema_hash = 0
    if schema:
        schema_items = []
        for table_name, table_schema in sorted(schema.items()):
            if isinstance(table_schema, dict):
                schema_items.append((table_name, len(table_schema)))
            else:
                schema_items.append((table_name, hash("unknown")))
        schema_hash = hash(tuple(schema_items))

    settings_hash = 0
    if optimizer_settings:
        settings_items = sorted(optimizer_settings.items())
        settings_hash = hash(tuple(settings_items))

    components = (expr_hash, dialect, schema_hash, settings_hash)
    return f"opt:{hash(components)}"
