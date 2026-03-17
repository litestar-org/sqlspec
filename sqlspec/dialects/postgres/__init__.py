"""PostgreSQL extension sqlglot dialects.

Landing zone for PGVector, ParadeDB, and other Postgres extension dialects.
"""

from sqlspec.dialects.postgres._paradedb import ParadeDB
from sqlspec.dialects.postgres._pgvector import PGVector

__all__: tuple[str, ...] = ("PGVector", "ParadeDB")
