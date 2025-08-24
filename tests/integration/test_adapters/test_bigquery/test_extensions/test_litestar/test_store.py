"""Integration tests for BigQuery session store with migration support."""

import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
def bigquery_config(bigquery_service: "BigQueryService", table_schema_prefix: str) -> BigQueryConfig:
    """Create BigQuery configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": table_schema_prefix,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Include Litestar migrations
            },
        )
        yield config


@pytest.fixture
def store(bigquery_config: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store instance with migrations applied."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(bigquery_config)
    commands.init(bigquery_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Use the migrated table structure
    return SQLSpecSessionStore(
        config=bigquery_config,
        table_name="litestar_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


def test_bigquery_store_table_creation(
    store: SQLSpecSessionStore, bigquery_config: BigQueryConfig, table_schema_prefix: str
) -> None:
    """Test that store table is created via migrations."""
    with bigquery_config.provide_session() as driver:
        # Verify table exists (created by migrations) using BigQuery's information schema
        result = driver.execute(f"""
            SELECT table_name
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name = 'litestar_sessions'
        """)
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "litestar_sessions"

        # Verify table structure
        result = driver.execute(f"""
            SELECT column_name, data_type
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'litestar_sessions'
        """)
        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify BigQuery-specific data types
        assert columns["session_id"] == "STRING"
        assert columns["data"] == "JSON"
        assert columns["expires_at"] == "TIMESTAMP"
        assert columns["created_at"] == "TIMESTAMP"


def test_bigquery_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the store."""
    key = "test-key"
    value = {
        "user_id": 123,
        "data": ["item1", "item2"],
        "nested": {"key": "value"},
        "bigquery_features": {"json_support": True, "analytics": True},
    }

    # Create
    store.set(key, value, expires_in=3600)

    # Read
    retrieved = store.get(key)
    assert retrieved == value

    # Update
    updated_value = {"user_id": 456, "new_field": "new_value", "bigquery_ml": {"model": "clustering", "accuracy": 0.85}}
    store.set(key, updated_value, expires_in=3600)

    retrieved = store.get(key)
    assert retrieved == updated_value

    # Delete
    store.delete(key)
    result = store.get(key)
    assert result is None


def test_bigquery_store_expiration(store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned."""
    key = "expiring-key"
    value = {"data": "will expire", "bigquery_info": {"serverless": True}}

    # Set with very short expiration
    store.set(key, value, expires_in=1)

    # Should be retrievable immediately
    result = store.get(key)
    assert result == value

    # Wait for expiration
    time.sleep(2)

    # Should return None after expiration
    result = store.get(key)
    assert result is None


def test_bigquery_store_complex_json_data(store: SQLSpecSessionStore) -> None:
    """Test BigQuery's JSON handling capabilities with complex data structures."""
    key = "complex-json-key"
    complex_value = {
        "analytics_config": {
            "project": "test-project-123",
            "dataset": "analytics_data",
            "tables": [
                {"name": "events", "partitioned": True, "clustered": ["user_id", "event_type"]},
                {"name": "users", "partitioned": False, "clustered": ["registration_date"]},
            ],
            "queries": {
                "daily_active_users": {
                    "sql": "SELECT COUNT(DISTINCT user_id) FROM events WHERE DATE(_PARTITIONTIME) = CURRENT_DATE()",
                    "schedule": "0 8 * * *",
                    "destination": {"table": "dau_metrics", "write_disposition": "WRITE_TRUNCATE"},
                },
                "conversion_funnel": {
                    "sql": "WITH funnel AS (SELECT user_id, event_type FROM events) SELECT * FROM funnel",
                    "schedule": "0 9 * * *",
                    "destination": {"table": "funnel_metrics", "write_disposition": "WRITE_APPEND"},
                },
            },
        },
        "ml_models": [
            {
                "name": "churn_prediction",
                "type": "logistic_regression",
                "features": ["days_since_last_login", "total_sessions", "avg_session_duration"],
                "target": "churned_30_days",
                "hyperparameters": {"l1_reg": 0.01, "l2_reg": 0.001, "max_iterations": 100},
                "performance": {"auc": 0.87, "precision": 0.82, "recall": 0.79, "f1": 0.805},
            },
            {
                "name": "lifetime_value",
                "type": "linear_regression",
                "features": ["subscription_tier", "months_active", "feature_usage_score"],
                "target": "total_revenue",
                "hyperparameters": {"learning_rate": 0.001, "batch_size": 1000},
                "performance": {"rmse": 45.67, "mae": 32.14, "r_squared": 0.73},
            },
        ],
        "streaming_config": {
            "dataflow_jobs": [
                {
                    "name": "realtime_events",
                    "source": "pubsub:projects/test/topics/events",
                    "sink": "bigquery:test.analytics.events",
                    "window_size": "1 minute",
                    "transforms": ["validate", "enrich", "deduplicate"],
                }
            ],
            "datastream_connections": [
                {
                    "name": "postgres_replica",
                    "source_type": "postgresql",
                    "destination": "test.raw.postgres_replica",
                    "sync_frequency": "5 minutes",
                }
            ],
        },
    }

    # Store complex JSON data
    store.set(key, complex_value, expires_in=3600)

    # Retrieve and verify
    retrieved = store.get(key)
    assert retrieved == complex_value

    # Verify specific nested structures
    assert retrieved["analytics_config"]["project"] == "test-project-123"
    assert len(retrieved["analytics_config"]["tables"]) == 2
    assert len(retrieved["analytics_config"]["queries"]) == 2
    assert len(retrieved["ml_models"]) == 2
    assert retrieved["ml_models"][0]["performance"]["auc"] == 0.87
    assert retrieved["streaming_config"]["dataflow_jobs"][0]["window_size"] == "1 minute"


def test_bigquery_store_multiple_sessions(store: SQLSpecSessionStore) -> None:
    """Test handling multiple sessions simultaneously."""
    sessions = {}

    # Create multiple sessions with different data
    for i in range(10):
        key = f"session-{i}"
        value = {
            "user_id": 1000 + i,
            "session_data": f"data for session {i}",
            "bigquery_job_id": f"job_{i:03d}",
            "analytics": {"queries_run": i * 5, "bytes_processed": i * 1024 * 1024, "slot_hours": i * 0.1},
            "preferences": {
                "theme": "dark" if i % 2 == 0 else "light",
                "region": f"us-central{i % 3 + 1}",
                "auto_save": True,
            },
        }
        sessions[key] = value
        store.set(key, value, expires_in=3600)

    # Verify all sessions can be retrieved correctly
    for key, expected_value in sessions.items():
        retrieved = store.get(key)
        assert retrieved == expected_value

    # Clean up by deleting all sessions
    for key in sessions:
        store.delete(key)
        assert store.get(key) is None


def test_bigquery_store_cleanup_expired_sessions(store: SQLSpecSessionStore) -> None:
    """Test cleanup of expired sessions."""
    # Create sessions with different expiration times
    short_lived_keys = []
    long_lived_keys = []

    for i in range(5):
        short_key = f"short-{i}"
        long_key = f"long-{i}"

        short_value = {"data": f"short lived {i}", "expires": "soon"}
        long_value = {"data": f"long lived {i}", "expires": "later"}

        store.set(short_key, short_value, expires_in=1)  # 1 second
        store.set(long_key, long_value, expires_in=3600)  # 1 hour

        short_lived_keys.append(short_key)
        long_lived_keys.append(long_key)

    # Verify all sessions exist initially
    for key in short_lived_keys + long_lived_keys:
        assert store.get(key) is not None

    # Wait for short-lived sessions to expire
    time.sleep(2)

    # Cleanup expired sessions
    store.delete_expired()

    # Verify short-lived sessions are gone, long-lived remain
    for key in short_lived_keys:
        assert store.get(key) is None

    for key in long_lived_keys:
        assert store.get(key) is not None

    # Clean up remaining sessions
    for key in long_lived_keys:
        store.delete(key)


def test_bigquery_store_large_session_data(store: SQLSpecSessionStore) -> None:
    """Test BigQuery's ability to handle reasonably large session data."""
    key = "large-session"

    # Create a large but reasonable dataset for BigQuery
    large_value = {
        "user_profile": {
            "basic_info": {f"field_{i}": f"value_{i}" for i in range(100)},
            "preferences": {f"pref_{i}": i % 2 == 0 for i in range(50)},
            "history": [
                {
                    "timestamp": f"2024-01-{(i % 28) + 1:02d}T{(i % 24):02d}:00:00Z",
                    "action": f"action_{i}",
                    "details": {"page": f"/page/{i}", "duration": i * 100, "interactions": i % 10},
                }
                for i in range(200)  # 200 history entries
            ],
        },
        "analytics_data": {
            "events": [
                {
                    "event_id": f"evt_{i:06d}",
                    "event_type": ["click", "view", "scroll", "hover"][i % 4],
                    "properties": {f"prop_{j}": j * i for j in range(15)},
                    "timestamp": f"2024-01-01T{(i % 24):02d}:{(i % 60):02d}:00Z",
                }
                for i in range(150)  # 150 events
            ],
            "segments": {
                f"segment_{i}": {
                    "name": f"Segment {i}",
                    "description": f"User segment {i} " * 10,  # Some repetitive text
                    "criteria": {
                        "age_range": [20 + i, 30 + i],
                        "activity_score": i * 10,
                        "features": [f"feature_{j}" for j in range(10)],
                    },
                    "stats": {"size": i * 1000, "conversion_rate": i * 0.01, "avg_lifetime_value": i * 100},
                }
                for i in range(25)  # 25 segments
            },
        },
        "bigquery_metadata": {
            "dataset_id": "analytics_data",
            "table_schemas": {
                f"table_{i}": {
                    "columns": [
                        {"name": f"col_{j}", "type": ["STRING", "INTEGER", "FLOAT", "BOOLEAN"][j % 4]}
                        for j in range(20)
                    ],
                    "partitioning": {"field": "created_at", "type": "DAY"},
                    "clustering": [f"col_{j}" for j in range(0, 4)],
                }
                for i in range(10)  # 10 table schemas
            },
        },
    }

    # Store large data
    store.set(key, large_value, expires_in=3600)

    # Retrieve and verify
    retrieved = store.get(key)
    assert retrieved == large_value

    # Verify specific parts of the large data
    assert len(retrieved["user_profile"]["basic_info"]) == 100
    assert len(retrieved["user_profile"]["history"]) == 200
    assert len(retrieved["analytics_data"]["events"]) == 150
    assert len(retrieved["analytics_data"]["segments"]) == 25
    assert len(retrieved["bigquery_metadata"]["table_schemas"]) == 10

    # Clean up
    store.delete(key)
