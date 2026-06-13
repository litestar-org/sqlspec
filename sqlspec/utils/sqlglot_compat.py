"""Compatibility helpers for sqlglot integration."""

__all__ = ("invalidate_generator_dispatch",)


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
        _DISPATCH_CACHE.pop(generator_class, None)
