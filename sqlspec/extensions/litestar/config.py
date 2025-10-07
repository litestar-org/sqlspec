"""Configuration types for Litestar session store extension."""

from typing_extensions import NotRequired, TypedDict

__all__ = ("LitestarConfig",)


class LitestarConfig(TypedDict, total=False):
    """Configuration options for Litestar session store extension.

    All fields are optional with sensible defaults. Use in extension_config["litestar"]:

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig

        config = OracleAsyncConfig(
            pool_config={"dsn": "oracle://localhost/XEPDB1"},
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

    When enabled, tables are created with the INMEMORY clause for Oracle Database,
    which stores table data in columnar format in memory for faster query performance.

    This is an Oracle-specific feature that requires:
        - Oracle Database 12.1.0.2 or higher
        - Database In-Memory option license (Enterprise Edition)
        - Sufficient INMEMORY_SIZE configured in the database instance

    Other database adapters ignore this setting.

    Examples:
        Oracle with in-memory enabled:
            config = OracleAsyncConfig(
                pool_config={"dsn": "oracle://..."},
                extension_config={
                    "litestar": {
                        "in_memory": True
                    }
                }
            )

    Notes:
        - Improves query performance for session lookups (10-100x faster)
        - Tables created with INMEMORY clause
        - Requires Oracle Database In-Memory option license
        - Ignored by non-Oracle adapters
    """
