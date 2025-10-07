"""DuckDB ADK Store with User FK Column Example.

This example demonstrates how to use the owner_id_column parameter
in DuckDB ADK store for multi-tenant session management.
"""

from pathlib import Path

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.adk import DuckdbADKStore

__all__ = ("main",)


def main() -> None:
    """Demonstrate owner ID column support in DuckDB ADK store."""
    db_path = Path("multi_tenant_sessions.ddb")

    try:
        config = DuckDBConfig(pool_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tenants (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                INSERT INTO tenants (id, name) VALUES
                    (1, 'Acme Corp'),
                    (2, 'Initech')
                ON CONFLICT DO NOTHING
            """)
            conn.commit()

        store = DuckdbADKStore(
            config,
            session_table="adk_sessions",
            events_table="adk_events",
            owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)",
        )
        store.create_tables()

        print(f"User FK column name: {store.owner_id_column_name}")
        print(f"User FK column DDL: {store.owner_id_column_ddl}")
        print()

        session1 = store.create_session(
            session_id="session-acme-001",
            app_name="analytics-app",
            user_id="user-alice",
            state={"workspace": "dashboard", "theme": "dark"},
            owner_id=1,
        )
        print(f"Created session for Acme Corp: {session1['id']}")

        session2 = store.create_session(
            session_id="session-initech-001",
            app_name="analytics-app",
            user_id="user-bob",
            state={"workspace": "reports", "theme": "light"},
            owner_id=2,
        )
        print(f"Created session for Initech: {session2['id']}")

        with config.provide_connection() as conn:
            cursor = conn.execute("""
                SELECT s.id, s.user_id, t.name as tenant_name, s.state
                FROM adk_sessions s
                JOIN tenants t ON s.tenant_id = t.id
                ORDER BY t.name
            """)
            rows = cursor.fetchall()

            print("\nSessions with tenant info:")
            for row in rows:
                print(f"  {row[0]} - User: {row[1]}, Tenant: {row[2]}")

        with config.provide_connection() as conn:
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM adk_sessions WHERE tenant_id = ?
            """,
                (1,),
            )
            count = cursor.fetchone()[0]
            print(f"\nSessions for Acme Corp (tenant_id=1): {count}")

        print("\nTrying to create session with invalid tenant_id...")
        try:
            store.create_session(
                session_id="session-invalid", app_name="analytics-app", user_id="user-charlie", state={}, owner_id=999
            )
        except Exception as e:
            print(f"Foreign key constraint violation (expected): {type(e).__name__}")

        print("\n✓ User FK column example completed successfully!")

    finally:
        if db_path.exists():
            db_path.unlink()
            print(f"\nCleaned up: {db_path}")


if __name__ == "__main__":
    main()
