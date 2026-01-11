"""DuckDB ADK store for Google Agent Development Kit.

DuckDB is an OLAP database optimized for analytical queries. This adapter provides
embedded session storage with zero-configuration setup, excellent for development,
testing, and analytical workloads.

Notes:
    For highly concurrent DML operations, consider PostgreSQL or other
    OLTP-optimized databases.
"""

from sqlspec.adapters.duckdb.adk.store import DuckdbADKMemoryStore, DuckdbADKStore

__all__ = ("DuckdbADKMemoryStore", "DuckdbADKStore")
