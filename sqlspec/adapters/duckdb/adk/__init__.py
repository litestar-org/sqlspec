"""DuckDB ADK store for Google Agent Development Kit - DEV/TEST ONLY.

WARNING: DuckDB is an OLAP database optimized for analytical queries,
not OLTP workloads. This adapter is suitable for local development,
testing, and prototyping only.
"""

from sqlspec.adapters.duckdb.adk.store import DuckdbADKStore

__all__ = ("DuckdbADKStore",)
