"""Statement hashing utilities for cache key generation.

Provides hashing functions for SQL statements, expressions, parameters,
filters, and AST sub-expressions.
"""

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from sqlglot import exp

from sqlspec.core._ordering import has_default_nulls, has_explicit_nulls
from sqlspec.core.parameters._types import TypedParameter
from sqlspec.utils.type_guards import is_expression

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import SQL

__all__ = (
    "hash_expression",
    "hash_expression_node",
    "hash_filters",
    "hash_optimized_expression",
    "hash_parameters",
    "hash_sql_statement",
)


_HASH_SIMPLE_TYPES: Final[frozenset[type]] = frozenset({str, int, bool, float, type(None)})


def hash_expression(expr: "exp.Expr | None", _seen: "set[int] | None" = None) -> int:
    """Generate hash from AST structure.

    Args:
        expr: SQLGlot Expression to hash
        _seen: Set of active ancestor object IDs to handle circular references

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

    if isinstance(expr, exp.Expression):
        args = expr.args
        arg_count = len(args)
        if arg_count == 1:
            key, value = next(iter(args.items()))
            components.append(key)
            components.append(
                hash_expression(value, _seen)
                if isinstance(value, exp.Expr)
                else (hash(value) if type(value) in _HASH_SIMPLE_TYPES else _hash_value(value, _seen))
            )
        elif arg_count > 1:
            for key, value in sorted(args.items()):
                components.append(key)
                components.append(
                    hash_expression(value, _seen)
                    if isinstance(value, exp.Expr)
                    else (hash(value) if type(value) in _HASH_SIMPLE_TYPES else _hash_value(value, _seen))
                )
        if has_default_nulls(expr):
            components.append("default_nulls")
        if has_explicit_nulls(expr):
            components.append("explicit_nulls")
    else:
        components.append(hash(expr.sql(copy=False)))

    _seen.remove(expr_id)
    return hash(tuple(components))


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
            if isinstance(value, TypedParameter):
                if isinstance(value.value, (list, dict)):
                    hashable_items.append((key, (repr(value.value), value.original_type)))
                else:
                    hashable_items.append((key, (value.value, value.original_type)))
            elif isinstance(value, (list, dict)):
                hashable_items.append((key, (repr(value), "unhashable")))
            else:
                try:
                    hash(value)
                    hashable_items.append((key, (value, "primitive")))
                except TypeError:
                    hashable_items.append((key, (repr(value), "unhashable_repr")))
        param_hash ^= hash(tuple(hashable_items))

    if original_parameters is not None:
        if isinstance(original_parameters, list):
            param_hash ^= hash(("original_count", len(original_parameters)))
            if original_parameters:
                sample_hash = hash(repr(original_parameters))
                param_hash ^= hash(("original_sample", sample_hash))
        else:
            param_hash ^= hash(("original", repr(original_parameters)))

    return param_hash


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
        hash(statement.statement_config),
    ]

    return f"sql:{hash(tuple(state_components))}"


def hash_expression_node(node: "exp.Expr", include_children: bool = True, dialect: str | None = None) -> str:
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
        if isinstance(node, exp.Expression):
            for key, value in sorted(node.args.items()):
                if not isinstance(value, (list, exp.Expr)):
                    components.extend((key, hash(value)))
        node_hash = hash(tuple(components))

    dialect_part = f":{dialect}" if dialect else ""
    return f"expr{dialect_part}:{node_hash}"


def hash_optimized_expression(
    expr: "exp.Expr",
    dialect: "str | None",
    schema: "dict[str, Any] | None" = None,
    optimizer_settings: "dict[str, Any] | None" = None,
) -> str:
    """Generate cache key for optimized expressions.

    Creates a key that includes expression structure, dialect, schema
    context, and optimizer settings.

    Args:
        expr: The unoptimized expression
        dialect: Optimizer dialect, or None for generic SQL
        schema: Schema information
        optimizer_settings: Additional optimizer configuration

    Returns:
        Cache key string for the optimized expression
    """

    return f"opt:{_expression_cache_fingerprint(expr, dialect=dialect, schema=schema, settings=optimizer_settings)}"


def _expression_cache_fingerprint(
    expr: "exp.Expr",
    *,
    parameter_signature: "tuple[str, ...]" = (),
    dialect: Any = None,
    schema: Any = None,
    settings: Any = None,
) -> str:
    components = (
        _expression_digest(expr),
        parameter_signature,
        str(dialect) if dialect is not None else "default",
        None if schema is None else _freeze_cache_value(schema),
        None if settings is None else _freeze_cache_value(settings),
    )
    return hashlib.blake2b(repr(components).encode(), digest_size=16).hexdigest()


class _ArgKey:
    """Marks an argument name on the digest work stack."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


def _expression_digest(expr: "exp.Expr") -> str:
    """Return a 128-bit digest of an expression tree's full content.

    The walk is iterative, so nesting depth is bounded by memory, not by the interpreter's recursion
    limit. Every node contributes its type, comments, NULL-ordering markers, and each set argument in
    name order. Argument counts preserve node boundaries, and leaf lengths delimit adjacent values.
    Repeat visits contribute a reference to the first visit, which also ends cyclic trees.
    """
    hasher = hashlib.blake2b(digest_size=16)
    update = hasher.update
    ordinals: dict[int, int] = {}
    stack: list[Any] = [expr]
    while stack:
        node = stack.pop()
        if isinstance(node, exp.Expr):
            node_id = id(node)
            first_visit = ordinals.get(node_id)
            if first_visit is not None:
                update(b"R%d;" % first_visit)
                continue
            ordinals[node_id] = len(ordinals)
            update(b"E")
            update(type(node).__name__.encode())
            update(b";")
            if not isinstance(node, exp.Expression):
                _update_digest_text(update, node.sql(copy=False))
                continue
            if has_default_nulls(node):
                update(b"d")
            if has_explicit_nulls(node):
                update(b"x")
            pending = len(stack)
            comments = node.comments
            if comments:
                stack.append(comments)
                stack.append(_ArgKey("comments"))
            args = node.args
            for name in sorted(args, reverse=True):
                value = args[name]
                if value is None:
                    continue
                stack.append(value)
                stack.append(_ArgKey(name))
            update(b"A%d;" % ((len(stack) - pending) // 2))
        elif type(node) is _ArgKey:
            update(b"K")
            update(node.name.encode())
            update(b";")
        elif type(node) is str:
            _update_digest_text(update, node)
        elif type(node) is list or type(node) is tuple:
            update(b"L%d;" % len(node))
            stack.extend(reversed(node))
        elif isinstance(node, dict):
            update(b"M%d;" % len(node))
            for key in sorted(node, key=repr, reverse=True):
                stack.append(node[key])
                stack.append(_ArgKey(repr(key)))
        else:
            update(b"V")
            update(type(node).__name__.encode())
            update(b":")
            _update_digest_text(update, repr(node))
    return hasher.hexdigest()


def _update_digest_text(update: Any, text: str) -> None:
    encoded = text.encode("utf-8", "surrogatepass")
    update(b"S%d:" % len(encoded))
    update(encoded)


def _freeze_cache_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        items = [(key, _freeze_cache_value(item)) for key, item in value.items()]
        return tuple(sorted(items, key=lambda pair: repr(pair[0])))
    if isinstance(value, (list, tuple)):
        return tuple([_freeze_cache_value(item) for item in value])
    if isinstance(value, (set, frozenset)):
        return tuple(sorted([_freeze_cache_value(item) for item in value], key=repr))
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _hash_value(value: Any, _seen: "set[int]") -> int:
    """Hash different value types.

    Args:
        value: Value to hash (can be Expression, list, dict, or primitive)
        _seen: Set of active ancestor object IDs to handle circular references

    Returns:
        Hash of the value
    """
    if isinstance(value, exp.Expr):
        return hash_expression(value, _seen)
    value_type = type(value)
    if value_type in _HASH_SIMPLE_TYPES:
        return hash(value)
    if value_type is list or value_type is tuple:
        return hash(
            tuple([
                hash_expression(item, _seen)
                if isinstance(item, exp.Expr)
                else (hash(item) if type(item) in _HASH_SIMPLE_TYPES else _hash_value(item, _seen))
                for item in value
            ])
        )
    if isinstance(value, dict):
        items = sorted([(k, _hash_value(v, _seen)) for k, v in value.items()])
        return hash(tuple(items))
    if isinstance(value, (list, tuple)):
        return hash(tuple([_hash_value(v, _seen) for v in value]))

    return hash(value)
