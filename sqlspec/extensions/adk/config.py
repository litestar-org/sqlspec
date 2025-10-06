"""Configuration types for ADK session store extension."""

from typing_extensions import NotRequired, TypedDict

__all__ = ("ADKConfig",)


class ADKConfig(TypedDict, total=False):
    """Configuration options for ADK session store extension.

    All fields are optional with sensible defaults. Use in extension_config["adk"]:

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            pool_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "user_fk_column": "tenant_id INTEGER REFERENCES tenants(id)"
                }
            }
        )

    Notes:
        This TypedDict provides type safety for extension config but is not required.
        You can use plain dicts as well.
    """

    session_table: NotRequired[str]
    """Name of the sessions table. Default: 'adk_sessions'

    Examples:
        "agent_sessions"
        "my_app_sessions"
        "tenant_acme_sessions"
    """

    events_table: NotRequired[str]
    """Name of the events table. Default: 'adk_events'

    Examples:
        "agent_events"
        "my_app_events"
        "tenant_acme_events"
    """

    user_fk_column: NotRequired[str]
    """Optional foreign key column definition to link sessions to your user table.

    Format: "column_name TYPE [NOT NULL] REFERENCES table(column) [options...]"

    The entire definition is passed through to DDL verbatim. We only parse
    the column name (first word) for use in INSERT/SELECT statements.

    Supports:
        - Foreign key constraints: REFERENCES table(column)
        - Nullable or NOT NULL
        - CASCADE options: ON DELETE CASCADE, ON UPDATE CASCADE
        - Dialect-specific options (DEFERRABLE, ENABLE VALIDATE, etc.)
        - Plain columns without FK (just extra column storage)

    Examples:
        PostgreSQL with UUID FK:
            "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"

        MySQL with BIGINT FK:
            "user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT"

        Oracle with NUMBER FK:
            "user_id NUMBER(10) REFERENCES users(id) ENABLE VALIDATE"

        SQLite with INTEGER FK:
            "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"

        Nullable FK (optional relationship):
            "workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL"

        No FK (just extra column):
            "organization_name VARCHAR(128) NOT NULL"

        Deferred constraint (PostgreSQL):
            "user_id UUID REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED"

    Notes:
        - Column name (first word) is extracted for INSERT/SELECT queries
        - Rest of definition is passed through to CREATE TABLE DDL
        - Database validates the DDL syntax (fail-fast on errors)
        - Works with all database dialects (PostgreSQL, MySQL, SQLite, Oracle, etc.)
    """
