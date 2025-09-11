"""Create Litestar session table migration with dialect-specific optimizations."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext


def up(context: "Optional[MigrationContext]" = None) -> "list[str]":
    """Create the litestar sessions table with dialect-specific column types.

    This table supports session management with optimized data types:
    - PostgreSQL: Uses JSONB for efficient JSON storage and TIMESTAMP WITH TIME ZONE
    - MySQL/MariaDB: Uses native JSON type and DATETIME
    - Oracle: Uses JSON column type (stored as RAW internally)
    - SQLite/Others: Uses TEXT for JSON data

    The table name can be customized via the extension configuration.

    Args:
        context: Migration context containing dialect information and extension config.

    Returns:
        List of SQL statements to execute for upgrade.
    """
    dialect = context.dialect if context else None

    # Get the table name from extension config, default to 'litestar_sessions'
    table_name = "litestar_sessions"
    if context and context.extension_config:
        table_name = context.extension_config.get("session_table", "litestar_sessions")

    # Determine appropriate data types based on dialect
    if dialect in {"postgres", "postgresql"}:
        data_type = "JSONB"
        timestamp_type = "TIMESTAMP WITH TIME ZONE"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect in {"mysql", "mariadb"}:
        data_type = "JSON"
        timestamp_type = "DATETIME"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect == "oracle":
        data_type = "CLOB"
        timestamp_type = "TIMESTAMP"
        created_at_default = ""  # We'll handle default separately in Oracle
    elif dialect == "sqlite":
        data_type = "TEXT"
        timestamp_type = "DATETIME"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect == "duckdb":
        data_type = "VARCHAR"  # DuckDB prefers VARCHAR for JSON storage
        timestamp_type = "TIMESTAMP"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    else:
        # Generic fallback
        data_type = "TEXT"
        timestamp_type = "TIMESTAMP"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"

    if dialect == "oracle":
        # Oracle has different syntax for CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                    session_id VARCHAR2(255) PRIMARY KEY,
                    data {data_type} NOT NULL,
                    expires_at {timestamp_type} NOT NULL,
                    created_at {timestamp_type} DEFAULT SYSTIMESTAMP NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Table already exists
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{table_name}_expires_at ON {table_name}(expires_at)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Index already exists
                        RAISE;
                    END IF;
            END;
            """,
        ]

    if dialect in {"mysql", "mariadb"}:
        # MySQL versions < 8.0 don't support CREATE INDEX IF NOT EXISTS
        # For older MySQL versions, the migration system will ignore duplicate index errors (1061)
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                session_id VARCHAR(255) PRIMARY KEY,
                data {data_type} NOT NULL,
                expires_at {timestamp_type} NOT NULL,
                created_at {timestamp_type} NOT NULL {created_at_default}
            )
            """,
            f"""
            CREATE INDEX idx_{table_name}_expires_at
            ON {table_name}(expires_at)
            """,
        ]

    # Determine session_id column type based on dialect
    session_id_type = "TEXT" if dialect in {"postgres", "postgresql"} else "VARCHAR(255)"

    return [
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            session_id {session_id_type} PRIMARY KEY,
            data {data_type} NOT NULL,
            expires_at {timestamp_type} NOT NULL,
            created_at {timestamp_type} NOT NULL {created_at_default}
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_expires_at
        ON {table_name}(expires_at)
        """,
    ]


def down(context: "Optional[MigrationContext]" = None) -> "list[str]":
    """Drop the litestar sessions table and its indexes.

    Args:
        context: Migration context containing extension configuration.

    Returns:
        List of SQL statements to execute for downgrade.
    """
    dialect = context.dialect if context else None
    # Get the table name from extension config, default to 'litestar_sessions'
    table_name = "litestar_sessions"
    if context and context.extension_config:
        table_name = context.extension_config.get("session_table", "litestar_sessions")

    if dialect == "oracle":
        # Oracle has different syntax for DROP IF EXISTS
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{table_name}_expires_at';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN -- Object does not exist
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN -- Table does not exist
                        RAISE;
                    END IF;
            END;
            """,
        ]

    if dialect in {"mysql", "mariadb"}:
        # MySQL DROP INDEX syntax without IF EXISTS for older versions
        # The migration system will ignore "index doesn't exist" errors (1091)
        return [f"DROP INDEX idx_{table_name}_expires_at ON {table_name}", f"DROP TABLE IF EXISTS {table_name}"]

    return [f"DROP INDEX IF EXISTS idx_{table_name}_expires_at", f"DROP TABLE IF EXISTS {table_name}"]
