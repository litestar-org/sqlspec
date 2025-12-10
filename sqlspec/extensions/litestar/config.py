"""Configuration types for Litestar session store extension."""

from typing_extensions import NotRequired, TypedDict

__all__ = ("LitestarConfig",)


class LitestarConfig(TypedDict):
    """Configuration options for Litestar session store extension.

    All fields are optional with sensible defaults. Use in extension_config["litestar"]:

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig

        config = OracleAsyncConfig(
            connection_config={"dsn": "oracle://localhost/XEPDB1"},
            extension_config={
                "litestar": {
                    "session_table": "my_sessions",
                    "in_memory": True
                }
            }
        )

    Notes:
        This TypedDict provides type safety for extension config but is not required.
        You can use plain dicts as well.
    """

    session_table: NotRequired[str]
    """Name of the sessions table. Default: 'litestar_session'

    Examples:
        "app_sessions"
        "user_sessions"
        "tenant_acme_sessions"
    """

    in_memory: NotRequired[bool]
    """Enable in-memory table storage (Oracle-specific). Default: False.

    When enabled, tables are created with the in-memory attribute for databases that support it.

    This is an Oracle-specific feature that requires:
        - Oracle Database 12.1.0.2 or higher
        - Database In-Memory option license (Enterprise Edition)
        - Sufficient INMEMORY_SIZE configured in the database instance

    Other database adapters ignore this setting.

    Examples:
        Oracle with in-memory enabled:
            config = OracleAsyncConfig(
                connection_config={"dsn": "oracle://..."},
                extension_config={
                    "litestar": {
                        "in_memory": True
                    }
                }
            )

    Notes:
        - Tables created with INMEMORY PRIORITY HIGH clause
        - Ignored by unsupported adapters
    """

    shard_count: NotRequired[int]
    """Optional hash shard count for session table primary key.

    When set (>1), adapters that support computed shard columns (e.g., Spanner)
    will create a generated shard_id using MOD(FARM_FINGERPRINT(session_id), shard_count)
    and include it in the primary key to reduce hotspotting. Ignored by adapters
    that do not support computed shards.
    """

    table_options: NotRequired[str]
    """Optional raw OPTIONS/engine-specific table options string.

    Passed verbatim when the adapter supports table-level OPTIONS/clauses
    (e.g., Spanner columnar/tiered storage). Ignored by adapters that do not
    support table options.
    """

    index_options: NotRequired[str]
    """Optional raw OPTIONS/engine-specific options for the expires_at index.

    Passed verbatim to the index definition for adapters that support index
    OPTIONS/clauses. Ignored by adapters that do not support index options.
    """
