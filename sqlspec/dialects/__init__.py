"""Custom sqlglot dialects for sqlspec.

Dialects are registered with sqlglot through the ``sqlglot.dialects``
entry-point group declared in ``pyproject.toml``: sqlglot resolves names such
as ``spanner`` or ``pgvector`` lazily on first use, in any environment where
sqlspec is installed. Importing the subpackages directly also registers the
dialects via sqlglot's ``Dialect`` metaclass.

This package intentionally avoids importing the dialect modules eagerly so
``import sqlspec`` does not pay the sqlglot dialect-machinery cost.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.dialects.postgres import ParadeDB, PGVector
    from sqlspec.dialects.spanner import Spangres, Spanner

__all__ = ("PGVector", "ParadeDB", "Spangres", "Spanner")

_DIALECT_MODULES = {
    "PGVector": "sqlspec.dialects.postgres",
    "ParadeDB": "sqlspec.dialects.postgres",
    "Spangres": "sqlspec.dialects.spanner",
    "Spanner": "sqlspec.dialects.spanner",
}


def __getattr__(name: str) -> Any:
    module_name = _DIALECT_MODULES.get(name)
    if module_name is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    import importlib

    return getattr(importlib.import_module(module_name), name)
