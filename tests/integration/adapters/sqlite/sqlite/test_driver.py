"""SQLite-only integration behavior not shared with the async adapter."""

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.sqlite import SqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


def test_asset_maintenance_alert_complex_query(sqlite_session: SqliteDriver) -> None:
    """Test complex CTE query with INSERT, ON CONFLICT, RETURNING, and LEFT JOIN.

    This tests the specific asset_maintenance_alert query pattern with:
    - WITH clause (CTE)
    - INSERT INTO with SELECT subquery
    - ON CONFLICT ON CONSTRAINT with DO NOTHING
    - RETURNING clause
    - LEFT JOIN with to_jsonb function
    - Named parameters (:date_start, :date_end)
    """

    sqlite_session.execute_script("""
        CREATE TABLE alert_definition (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE asset_maintenance (
            id INTEGER PRIMARY KEY,
            responsible_id INTEGER NOT NULL,
            planned_date_start DATE,
            cancelled BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        );

        CREATE TABLE alert_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_maintenance_id INTEGER NOT NULL,
            alert_definition_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_alert UNIQUE (user_id, asset_maintenance_id, alert_definition_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (asset_maintenance_id) REFERENCES asset_maintenance(id),
            FOREIGN KEY (alert_definition_id) REFERENCES alert_definition(id)
        );
    """)

    sqlite_session.execute("INSERT INTO alert_definition (id, name) VALUES (?, ?)", (1, "maintenances_today"))

    sqlite_session.execute_many(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        [
            (1, "John Doe", "john@example.com"),
            (2, "Jane Smith", "jane@example.com"),
            (3, "Bob Wilson", "bob@example.com"),
        ],
    )

    sqlite_session.execute_many(
        "INSERT INTO asset_maintenance (id, responsible_id, planned_date_start, cancelled) VALUES (?, ?, ?, ?)",
        [
            (1, 1, "2024-01-15", False),
            (2, 2, "2024-01-16", False),
            (3, 3, "2024-01-17", False),
            (4, 1, "2024-01-18", True),
            (5, 2, "2024-01-10", False),
            (6, 3, "2024-01-20", False),
        ],
    )

    insert_result = sqlite_session.execute(
        """
        INSERT INTO alert_users (user_id, asset_maintenance_id, alert_definition_id)
        SELECT responsible_id, id, (SELECT id FROM alert_definition WHERE name = 'maintenances_today')
        FROM asset_maintenance
        WHERE planned_date_start IS NOT NULL
        AND planned_date_start BETWEEN :date_start AND :date_end
        AND cancelled = 0
        ON CONFLICT(user_id, asset_maintenance_id, alert_definition_id) DO NOTHING
    """,
        {"date_start": "2024-01-15", "date_end": "2024-01-17"},
    )

    sqlite_session.connection.commit()

    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 3

    select_result = sqlite_session.execute("""
        SELECT
            au.*,
            u.id as user_id_from_join,
            u.name as user_name,
            u.email as user_email
        FROM alert_users au
        LEFT JOIN users u ON u.id = au.user_id
        WHERE au.created_at >= datetime('now', '-1 minute')
        ORDER BY au.id
    """)

    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    for row in select_result.get_data():
        assert row["user_id"] in [1, 2, 3]
        assert row["asset_maintenance_id"] in [1, 2, 3]
        assert row["alert_definition_id"] == 1
        assert row["user_name"] in ["John Doe", "Jane Smith", "Bob Wilson"]
        assert "@example.com" in row["user_email"]

    insert_result2 = sqlite_session.execute(
        """
        INSERT INTO alert_users (user_id, asset_maintenance_id, alert_definition_id)
        SELECT responsible_id, id, (SELECT id FROM alert_definition WHERE name = 'maintenances_today')
        FROM asset_maintenance
        WHERE planned_date_start IS NOT NULL
        AND planned_date_start BETWEEN :date_start AND :date_end
        AND cancelled = 0
        ON CONFLICT(user_id, asset_maintenance_id, alert_definition_id) DO NOTHING
    """,
        {"date_start": "2024-01-15", "date_end": "2024-01-17"},
    )

    assert insert_result2.rows_affected == 0

    count_result = sqlite_session.execute("SELECT COUNT(*) as count FROM alert_users")
    assert count_result.data is not None
    assert count_result.get_data()[0]["count"] == 3
