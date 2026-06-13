"""Compatibility helpers for sqlglot integration."""

from typing import TYPE_CHECKING, cast

__all__ = ("invalidate_generator_dispatch",)

if TYPE_CHECKING:
    from sqlglot.generator import Generator


def invalidate_generator_dispatch(*generator_classes: "type[object]") -> None:
    """Invalidate sqlglot generator dispatch entries for the provided classes.

    Args:
        *generator_classes: SQLGlot generator classes whose dispatch caches should be cleared.
    """
    try:
        from sqlglot.generator import _DISPATCH_CACHE  # pyright: ignore[reportPrivateUsage,reportPrivateImportUsage]
    except ImportError:
        return

    for generator_class in generator_classes:
        _DISPATCH_CACHE.pop(cast("type[Generator]", generator_class), None)
