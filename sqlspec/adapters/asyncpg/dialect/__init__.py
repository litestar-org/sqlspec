"""Asyncpg dialect submodule."""

from sqlglot.dialects.dialect import Dialect

from sqlspec.adapters.asyncpg.dialect._paradedb import ParadeDB
from sqlspec.adapters.asyncpg.dialect._pgvector import PGVector

# Register dialects with sqlglot
Dialect.classes["pgvector"] = PGVector
Dialect.classes["paradedb"] = ParadeDB

__all__ = ("PGVector", "ParadeDB")
