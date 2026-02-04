"""PostgreSQL extension sqlglot dialects.

Landing zone for PGVector, ParadeDB, and other Postgres extension dialects.
"""

from sqlspec.dialects.postgres.paradedb import ParadeDB
from sqlspec.dialects.postgres.pgvector import PGVector

__all__: tuple[str, ...] = ("PGVector", "ParadeDB")
