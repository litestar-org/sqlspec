"""SQLGlot generator registration helpers."""

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from sqlglot.generator import Generator


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
