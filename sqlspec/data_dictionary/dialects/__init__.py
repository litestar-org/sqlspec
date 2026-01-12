"""Dialect configuration registrations."""

from sqlspec.data_dictionary.dialects.bigquery import BIGQUERY_CONFIG
from sqlspec.data_dictionary.dialects.cockroachdb import COCKROACHDB_CONFIG
from sqlspec.data_dictionary.dialects.duckdb import DUCKDB_CONFIG
from sqlspec.data_dictionary.dialects.mysql import MYSQL_CONFIG
from sqlspec.data_dictionary.dialects.oracle import ORACLE_CONFIG
from sqlspec.data_dictionary.dialects.postgres import POSTGRES_CONFIG
from sqlspec.data_dictionary.dialects.spanner import SPANNER_CONFIG
from sqlspec.data_dictionary.dialects.sqlite import SQLITE_CONFIG

__all__ = (
    "BIGQUERY_CONFIG",
    "COCKROACHDB_CONFIG",
    "DUCKDB_CONFIG",
    "MYSQL_CONFIG",
    "ORACLE_CONFIG",
    "POSTGRES_CONFIG",
    "SPANNER_CONFIG",
    "SQLITE_CONFIG",
)
