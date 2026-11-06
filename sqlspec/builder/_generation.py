"""SQLGlot generator registration helpers."""

from typing import TYPE_CHECKING, cast

from sqlglot import Dialect
from sqlglot.generator import Generator

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("generator_class_for_dialect", "invalidate_generator_dispatch")


def generator_class_for_dialect(dialect: "DialectType | str | None") -> "type[Generator]":
    """Resolve the SQLGlot generator class that renders a dialect.

    Args:
        dialect: Dialect name, class, or instance; ``None`` selects the base generator.

    Returns:
        The dialect's generator class.
    """
    if dialect is None:
        return Generator

    dialect_class: type[Dialect]
    if isinstance(dialect, str):
        dialect_class = type(Dialect.get_or_raise(dialect))
    elif isinstance(dialect, type) and issubclass(dialect, Dialect):
        dialect_class = dialect
    elif isinstance(dialect, Dialect):
        dialect_class = type(dialect)
    else:
        dialect_class = type(Dialect.get_or_raise(str(dialect)))

    return dialect_class.generator_class


def invalidate_generator_dispatch(*generator_classes: "type[object]") -> None:
    """Clear SQLGlot generator dispatch entries after transform registration.

    SQLGlot caches generator dispatch lookups. SQLSpec mutates ``TRANSFORMS``
    on existing generator classes for compiled sqlglot compatibility, so those
    cache entries need to be invalidated after registration.

    Args:
        *generator_classes: SQLGlot generator classes whose dispatch caches should be cleared.
    """
    try:
        from sqlglot.generator import _DISPATCH_CACHE  # pyright: ignore[reportPrivateUsage,reportPrivateImportUsage]
    except ImportError:
        return

    for generator_class in generator_classes:
        _DISPATCH_CACHE.pop(cast("type[Generator]", generator_class), None)
