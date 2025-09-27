"""Create Litestar session table migration with dialect-specific optimizations."""

from typing import TYPE_CHECKING, Optional

from sqlspec.utils.logging import get_logger

logger = get_logger("migrations.litestar.session")

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext


async def up(context: "Optional[MigrationContext]" = None) -> "list[str]":
    """Create the litestar sessions table with dialect-specific column types.

    This table supports session management with optimized data types:
    - PostgreSQL: Uses JSONB for efficient JSON storage and TIMESTAMP WITH TIME ZONE
    - MySQL/MariaDB: Uses native JSON type and DATETIME
    - DuckDB: Uses native JSON type for optimal analytical performance
    - Oracle: Version-specific JSON storage:
      * Oracle 21c+ with compatible>=20: Native JSON type
      * Oracle 19c+ (Autonomous): BLOB with OSON format
      * Oracle 12c+: BLOB with JSON validation
      * Older versions: BLOB fallback
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

    data_type = None
    timestamp_type = None
    if context and context.driver:
        try:
            # Try to get optimal types if data dictionary is available
            dd = context.driver.data_dictionary
            if hasattr(dd, "get_optimal_type"):
                # Check if it's an async method
                import inspect

                if inspect.iscoroutinefunction(dd.get_optimal_type):
                    json_result = await dd.get_optimal_type(context.driver, "json")  # type: ignore[arg-type]
                    timestamp_result = await dd.get_optimal_type(context.driver, "timestamp")  # type: ignore[arg-type]
                else:
                    json_result = dd.get_optimal_type(context.driver, "json")  # type: ignore[arg-type]
                    timestamp_result = dd.get_optimal_type(context.driver, "timestamp")  # type: ignore[arg-type]

                data_type = str(json_result) if json_result else None
                timestamp_type = str(timestamp_result) if timestamp_result else None
                logger.info("Detected types - JSON: %s, Timestamp: %s", data_type, timestamp_type)
        except Exception as e:
            logger.warning("Failed to detect optimal types: %s", e)
            data_type = None
            timestamp_type = None

    # Set defaults based on dialect if data dictionary failed
    if dialect in {"postgres", "postgresql"}:
        data_type = data_type or "JSONB"
        timestamp_type = timestamp_type or "TIMESTAMP WITH TIME ZONE"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect in {"mysql", "mariadb"}:
        data_type = data_type or "JSON"
        timestamp_type = timestamp_type or "DATETIME"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect == "oracle":
        data_type = data_type or "BLOB"
        timestamp_type = timestamp_type or "TIMESTAMP"
        created_at_default = ""  # We'll handle default separately in Oracle
    elif dialect == "sqlite":
        data_type = data_type or "TEXT"
        timestamp_type = timestamp_type or "DATETIME"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    elif dialect == "duckdb":
        data_type = data_type or "JSON"
        timestamp_type = timestamp_type or "TIMESTAMP"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"
    else:
        # Generic fallback
        data_type = data_type or "TEXT"
        timestamp_type = timestamp_type or "TIMESTAMP"
        created_at_default = "DEFAULT CURRENT_TIMESTAMP"

    if dialect == "oracle":
        # Oracle has different syntax for CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS
        # Handle JSON constraints for BLOB columns
        if "CHECK" in data_type:
            # Extract the constraint part (e.g., "CHECK (data IS JSON FORMAT OSON)")
            # and separate the base type (BLOB) from the constraint
            base_type = data_type.split()[0]  # "BLOB"
            constraint_part = data_type[len(base_type) :].strip()  # "CHECK (data IS JSON FORMAT OSON)"
            data_column_def = f"data {base_type} NOT NULL {constraint_part}"
        else:
            # For JSON type or CLOB, no additional constraint needed
            data_column_def = f"data {data_type} NOT NULL"

        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                    session_id VARCHAR2(255) PRIMARY KEY,
                    {data_column_def},
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

    # Use optimal text type for session_id
    if context and context.driver:
        try:
            dd = context.driver.data_dictionary
            text_result = dd.get_optimal_type(context.driver, "text")  # type: ignore[arg-type]
            session_id_type = str(text_result) if text_result else "VARCHAR(255)"
        except Exception:
            session_id_type = "VARCHAR(255)"
    else:
        session_id_type = "VARCHAR(255)"

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


async def down(context: "Optional[MigrationContext]" = None) -> "list[str]":
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
