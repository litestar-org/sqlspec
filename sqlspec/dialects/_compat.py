"""Compatibility layer for sqlglot[c] compiled generators."""

__all__ = ("is_generator_compiled",)


def is_generator_compiled(generator_class: type) -> bool:
    """Check if a sqlglot generator class is mypyc-compiled.

    When sqlglot[c] is installed, generator modules are compiled and
    reject interpreted subclasses. This detection enables conditional
    code paths: clean subclasses for pure Python, TRANSFORMS patching
    for compiled.
    """
    return hasattr(generator_class, "__mypyc_attrs__")
