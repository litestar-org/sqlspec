"""ORDER BY item construction that defers NULL placement to the database."""

import itertools
from typing import Final, Literal

from sqlglot import Dialect, exp
from sqlglot.generator import Generator
from sqlglot.tokenizer_core import TokenType

__all__ = ("NullsPlacement", "apply_direction", "default_nulls", "has_default_nulls", "ordered")

NullsPlacement = Literal["first", "last"]

_DEFAULT_NULLS_ARG: Final[str] = "sqlspec_default_nulls"


def has_default_nulls(expression: exp.Expr) -> bool:
    """Check whether an ORDER BY item uses database-default NULL placement.

    Args:
        expression: Expression to inspect.

    Returns:
        Whether the item uses database-default NULL placement.
    """
    return isinstance(expression, exp.Ordered) and _DEFAULT_NULLS_ARG in expression.meta


def ordered(expression: exp.Expr, *, desc: "bool | None" = None, nulls: "NullsPlacement | None" = None) -> exp.Ordered:
    """Build an ORDER BY item with optional NULL placement.

    Args:
        expression: Expression to order by.
        desc: Descending, ascending, or omitted direction.
        nulls: Explicit placement, or None for the database default.

    Returns:
        The ordering expression.
    """
    if nulls is not None:
        return exp.Ordered(this=expression, desc=desc, nulls_first=nulls == "first")
    node = exp.Ordered(this=expression, desc=desc, nulls_first=not desc)
    node.meta[_DEFAULT_NULLS_ARG] = True
    return node


def default_nulls(item: exp.Ordered, source: str, dialect: str | None = None) -> exp.Ordered:
    """Mark parsed ordering as database-default unless its source requests placement.

    Args:
        item: Parsed ordering expression to mutate.
        source: Original ORDER BY item text.
        dialect: Dialect used to parse the source text.

    Returns:
        The same ordering expression.
    """
    tokens = Dialect.get_or_raise(dialect).tokenize(source) if "NULLS" in source.upper() else []
    explicit_nulls = False
    depth = 0
    for token, following in itertools.pairwise(tokens):
        if token.token_type in (TokenType.L_PAREN, TokenType.L_BRACKET):
            depth += 1
        elif token.token_type in (TokenType.R_PAREN, TokenType.R_BRACKET):
            depth -= 1
        elif (
            depth == 0
            and token.token_type == TokenType.VAR
            and token.text.upper() == "NULLS"
            and following.token_type in (TokenType.VAR, TokenType.FIRST)
            and following.text.upper() in ("FIRST", "LAST")
        ):
            explicit_nulls = True
            break
    if not explicit_nulls:
        item.set("nulls_first", not item.args.get("desc"))
        item.meta[_DEFAULT_NULLS_ARG] = True
    return item


def apply_direction(item: exp.Expr, desc: bool) -> exp.Expr:
    """Apply direction without nesting ordering expressions.

    Args:
        item: Expression to order. Existing Ordered nodes are mutated in place.
        desc: Whether to add descending order when no direction is present.

    Returns:
        The same Ordered node, or a wrapped expression when direction is added.
    """
    if not isinstance(item, exp.Ordered):
        return ordered(item, desc=True) if desc else item
    if desc and item.args.get("desc") is None:
        item.set("desc", True)
        if has_default_nulls(item):
            item.set("nulls_first", False)
    return item


def _ordered_sql(generator: Generator, expression: exp.Ordered) -> str:
    if not has_default_nulls(expression):
        return generator.ordered_sql(expression)
    desc = expression.args.get("desc")
    this = generator.sql(expression, "this")
    direction = " DESC" if desc else (" ASC" if desc is False else "")
    with_fill = generator.sql(expression, "with_fill")
    return f"{this}{direction}{' ' + with_fill if with_fill else ''}"


def _generator_classes(root: "type[Generator]") -> "list[type[Generator]]":
    classes = [root]
    for subclass in root.__subclasses__():
        classes.extend(_generator_classes(subclass))
    return classes


def _register_ordered_transform() -> None:
    try:
        from sqlglot.generator import _DISPATCH_CACHE  # pyright: ignore[reportPrivateUsage,reportPrivateImportUsage]
    except ImportError:
        dispatch_cache = None
    else:
        dispatch_cache = _DISPATCH_CACHE
    for generator_class in _generator_classes(Generator):
        if exp.Ordered not in generator_class.TRANSFORMS:
            generator_class.TRANSFORMS[exp.Ordered] = _ordered_sql
            if dispatch_cache is not None:
                dispatch_cache.pop(generator_class, None)


_register_ordered_transform()
