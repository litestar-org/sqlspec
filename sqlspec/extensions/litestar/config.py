"""Configuration types for Litestar session store extension."""

from typing import Any, Literal

from typing_extensions import NotRequired, TypedDict

__all__ = ("LitestarConfig",)


class LitestarConfig(TypedDict):
    """Configuration options for Litestar session store extension.

    All fields are optional with sensible defaults. Use in extension_config["litestar"]:
    """

    manage_schema: NotRequired[bool]
    """Apply additive target-schema reconciliation. Default: True."""

    create_schema: NotRequired[bool]
    """Create the session table during managed reconciliation. Default: True."""

    run_migrations: NotRequired[bool]
    """Run packaged versioned migrations when an integration supplies a runner. Default: False."""

    session_table: NotRequired[str]
    """Name of the sessions table. Default: 'litestar_session'"""

    in_memory: NotRequired[bool]
    """
    Enable in-memory table storage (Oracle-specific). Default: False.

    When enabled, tables are created with the in-memory attribute for databases that support it.

    This is an Oracle-specific feature that requires:
        - Oracle Database 12.1.0.2 or higher
        - Database In-Memory option license (Enterprise Edition)
        - Sufficient INMEMORY_SIZE configured in the database instance

    Other database adapters ignore this setting.
    """

    shard_count: NotRequired[int]
    """
    Optional hash shard count for session table primary key.

    When set (>1), adapters that support computed shard columns
    will create a generated shard_id using MOD(FARM_FINGERPRINT(session_id), shard_count)
    and include it in the primary key to reduce hotspotting. Ignored by adapters
    that do not support computed shards.
    """

    table_options: NotRequired[str]
    """
    Optional raw OPTIONS/engine-specific table options string.

    Passed verbatim when the adapter supports table-level OPTIONS/clauses. Ignored by adapters that do not
    support table options.
    """

    index_options: NotRequired[str]
    """Optional raw OPTIONS/engine-specific options for the expires_at index.

    Passed verbatim to the index definition for adapters that support index
    OPTIONS/clauses. Ignored by adapters that do not support index options.
    """

    partitioning: NotRequired[dict[str, Any]]
    """Configure adapter-specific session-table partitioning where supported."""

    partition_expiration_days: NotRequired[int]
    """Set BigQuery partition expiration in days."""

    require_partition_filter: NotRequired[bool]
    """Require partition filters for BigQuery session queries."""

    enable_hash_sharded_indexes: NotRequired[bool]
    """Enable CockroachDB hash-sharded session indexes."""

    hash_shard_bucket_count: NotRequired[int]
    """Set the CockroachDB hash-shard bucket count."""

    ttl_expiration_expression: NotRequired[Literal[False, "expires_at"]]
    """Enable CockroachDB row-level TTL using the session ``expires_at`` column."""

    fillfactor: NotRequired[int]
    """Set PostgreSQL-family session-table fillfactor. Default: 80."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """Set the PostgreSQL-family autovacuum vacuum scale factor."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """Set the PostgreSQL-family autovacuum analyze scale factor."""

    pragma_profile: NotRequired[bool]
    """Apply the SQLite extension-store PRAGMA profile. Default: False."""

    pragma_overrides: NotRequired[dict[str, str | int | bool]]
    """Apply validated SQLite PRAGMA overrides after the optional profile."""
