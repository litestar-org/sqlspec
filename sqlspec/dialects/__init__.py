"""Centralized sqlglot dialect registration for sqlspec.

All custom sqlglot dialects are registered here via manual ``Dialect.classes``
assignment. Entry-point registration (``sqlglot.dialects`` group in
``pyproject.toml``) provides automatic discovery for external consumers.

Import this module to ensure all sqlspec dialects are available::

    import sqlspec.dialects  # registers spanner, spangres, etc.
"""

from sqlglot.dialects.dialect import Dialect

from sqlspec.dialects.postgres import ParadeDB, PGVector
from sqlspec.dialects.spanner import Spangres, Spanner

Dialect.classes["pgvector"] = PGVector
Dialect.classes["paradedb"] = ParadeDB
Dialect.classes["spanner"] = Spanner
Dialect.classes["spangres"] = Spangres

__all__ = ("PGVector", "ParadeDB", "Spangres", "Spanner")
