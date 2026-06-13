"""Tests for sqlglot compatibility helpers."""

import builtins
from typing import Any

from sqlglot import exp
from sqlglot.generator import Generator

from sqlspec.utils.sqlglot_compat import invalidate_generator_dispatch


def test_invalidate_generator_dispatch_refreshes_transform_cache() -> None:
    """Invalidation should make a fresh generator see TRANSFORMS mutations."""
    sentinel = object()
    original_transform = Generator.TRANSFORMS.get(exp.Null, sentinel)

    try:
        assert Generator().sql(exp.Null()) == "NULL"

        Generator.TRANSFORMS[exp.Null] = lambda _generator, _expression: "SQLSPEC_NULL"
        assert Generator().sql(exp.Null()) == "NULL"

        invalidate_generator_dispatch(Generator)

        assert Generator().sql(exp.Null()) == "SQLSPEC_NULL"
    finally:
        if original_transform is sentinel:
            Generator.TRANSFORMS.pop(exp.Null, None)
        else:
            Generator.TRANSFORMS[exp.Null] = original_transform  # type: ignore[assignment]
        invalidate_generator_dispatch(Generator)


def test_invalidate_generator_dispatch_unknown_class_is_noop() -> None:
    """Unknown classes should be accepted as no-op invalidation targets."""

    class UnknownGenerator:
        pass

    invalidate_generator_dispatch(UnknownGenerator)


def test_invalidate_generator_dispatch_missing_cache_is_noop(monkeypatch) -> None:
    """Missing private sqlglot cache imports should degrade silently."""
    original_import = builtins.__import__

    def raising_import(name: str, globals_: Any = None, locals_: Any = None, fromlist: tuple[str, ...] = (), level: int = 0):
        if name == "sqlglot.generator" and "_DISPATCH_CACHE" in fromlist:
            raise ImportError("cache moved")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", raising_import)

    invalidate_generator_dispatch(Generator)
