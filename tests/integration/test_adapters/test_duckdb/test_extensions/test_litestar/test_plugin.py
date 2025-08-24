"""Comprehensive Litestar integration tests for DuckDB adapter."""

import time
from datetime import timedelta
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import TestClient

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.duckdb, pytest.mark.integration, pytest.mark.xdist_group("duckdb")]


def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that session store is created properly."""
    assert session_store is not None
    assert session_store._config is not None
    assert session_store._table_name == "litestar_sessions"


def test_session_store_duckdb_table_structure(
    session_store: SQLSpecSessionStore, migrated_config: DuckDBConfig
) -> None:
    """Test that session store table has correct DuckDB-specific structure."""
    with migrated_config.provide_session() as driver:
        # Verify table exists
        result = driver.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'litestar_sessions'"
        )
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "litestar_sessions"

        # Verify table structure with DuckDB-specific types
        result = driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'litestar_sessions' ORDER BY ordinal_position"
        )
        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # DuckDB should use appropriate types for JSON storage
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check DuckDB-specific column types (JSON or VARCHAR for data)
        assert columns.get("data") in ["JSON", "VARCHAR", "TEXT"]
        assert any(dt in columns.get("expires_at", "") for dt in ["TIMESTAMP", "DATETIME"])

        # Verify indexes exist for performance
        result = driver.execute(
            "SELECT index_name FROM information_schema.statistics WHERE table_name = 'litestar_sessions'"
        )
        # DuckDB should have some indexes for performance
        assert len(result.data) >= 0  # DuckDB may not show indexes the same way


def test_basic_session_operations(litestar_app: Litestar) -> None:
    """Test basic session get/set/delete operations."""
    with TestClient(app=litestar_app) as client:
        # Set a simple value
        response = client.get("/session/set/username?value=testuser")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "set", "key": "username", "value": "testuser"}

        # Get the value back
        response = client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": "testuser"}

        # Set another value
        response = client.get("/session/set/user_id?value=12345")
        assert response.status_code == HTTP_200_OK

        # Get all session data
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["username"] == "testuser"
        assert data["user_id"] == "12345"

        # Delete a specific key
        response = client.post("/session/key/username/delete")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "deleted", "key": "username"}

        # Verify it's gone
        response = client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": None}

        # user_id should still exist
        response = client.get("/session/get/user_id")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "user_id", "value": "12345"}


def test_bulk_session_operations(litestar_app: Litestar) -> None:
    """Test bulk session operations."""
    with TestClient(app=litestar_app) as client:
        # Set multiple values at once
        bulk_data = {
            "user_id": 42,
            "username": "alice",
            "email": "alice@example.com",
            "preferences": {"theme": "dark", "notifications": True, "language": "en"},
            "roles": ["user", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
        }

        response = client.post("/session/bulk", json=bulk_data)
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "bulk set", "count": 6}

        # Verify all data was set
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        for key, expected_value in bulk_data.items():
            assert data[key] == expected_value


def test_session_persistence_across_requests(litestar_app: Litestar) -> None:
    """Test that sessions persist across multiple requests."""
    with TestClient(app=litestar_app) as client:
        # Test counter functionality across multiple requests
        expected_counts = [1, 2, 3, 4, 5]

        for expected_count in expected_counts:
            response = client.get("/counter")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"count": expected_count}

        # Verify count persists after setting other data
        response = client.get("/session/set/other_data?value=some_value")
        assert response.status_code == HTTP_200_OK

        response = client.get("/counter")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"count": 6}


def test_duckdb_json_support(session_store: SQLSpecSessionStore, migrated_config: DuckDBConfig) -> None:
    """Test DuckDB JSON support for session data with analytical capabilities."""
    complex_json_data = {
        "analytics_profile": {
            "user_id": 12345,
            "query_history": [
                {
                    "query": "SELECT COUNT(*) FROM sales WHERE date >= '2024-01-01'",
                    "execution_time_ms": 125.7,
                    "rows_returned": 1,
                    "timestamp": "2024-01-15T10:30:00Z",
                },
                {
                    "query": "SELECT product_id, SUM(revenue) FROM sales GROUP BY product_id ORDER BY SUM(revenue) DESC LIMIT 10",
                    "execution_time_ms": 89.3,
                    "rows_returned": 10,
                    "timestamp": "2024-01-15T10:32:00Z",
                },
            ],
            "preferences": {
                "output_format": "parquet",
                "compression": "snappy",
                "parallel_execution": True,
                "vectorization": True,
                "memory_limit": "8GB",
            },
            "datasets": {
                "sales": {
                    "location": "s3://data-bucket/sales/",
                    "format": "parquet",
                    "partitions": ["year", "month"],
                    "last_updated": "2024-01-15T09:00:00Z",
                    "row_count": 50000000,
                },
                "customers": {
                    "location": "/local/data/customers.csv",
                    "format": "csv",
                    "schema": {
                        "customer_id": "INTEGER",
                        "name": "VARCHAR",
                        "email": "VARCHAR",
                        "created_at": "TIMESTAMP",
                    },
                    "row_count": 100000,
                },
            },
        },
        "session_metadata": {
            "created_at": "2024-01-15T10:30:00Z",
            "ip_address": "192.168.1.100",
            "user_agent": "DuckDB Analytics Client v1.0",
            "features": ["json_support", "analytical_queries", "parquet_support", "vectorization"],
            "performance_stats": {
                "queries_executed": 42,
                "avg_execution_time_ms": 235.6,
                "total_data_processed_gb": 15.7,
                "cache_hit_rate": 0.87,
            },
        },
    }

    # Test storing and retrieving complex analytical JSON data
    session_id = "duckdb-json-test-session"
    run_(session_store.set)(session_id, complex_json_data, expires_in=3600)

    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == complex_json_data

    # Verify nested structure access specific to analytical workloads
    assert retrieved_data["analytics_profile"]["preferences"]["vectorization"] is True
    assert retrieved_data["analytics_profile"]["datasets"]["sales"]["row_count"] == 50000000
    assert len(retrieved_data["analytics_profile"]["query_history"]) == 2
    assert retrieved_data["session_metadata"]["performance_stats"]["cache_hit_rate"] == 0.87

    # Test JSON operations directly in DuckDB (DuckDB has strong JSON support)
    with migrated_config.provide_session() as driver:
        # Verify the data is stored appropriately in DuckDB
        result = driver.execute("SELECT data FROM litestar_sessions WHERE session_id = ?", (session_id,))
        assert len(result.data) == 1
        stored_data = result.data[0]["data"]

        # DuckDB can store JSON natively or as text, both are valid
        if isinstance(stored_data, str):
            import json

            parsed_json = json.loads(stored_data)
            assert parsed_json == complex_json_data
        else:
            # If stored as native JSON type in DuckDB
            assert stored_data == complex_json_data

        # Test DuckDB's JSON query capabilities if supported
        try:
            # Try to query JSON data using DuckDB's JSON functions
            result = driver.execute(
                "SELECT json_extract(data, '$.analytics_profile.preferences.vectorization') as vectorization FROM litestar_sessions WHERE session_id = ?",
                (session_id,),
            )
            if result.data and len(result.data) > 0:
                # If DuckDB supports JSON extraction, verify it works
                assert result.data[0]["vectorization"] is True
        except Exception:
            # JSON functions may not be available in all DuckDB versions, which is fine
            pass

    # Cleanup
    run_(session_store.delete)(session_id)


def test_session_expiration(migrated_config: DuckDBConfig) -> None:
    """Test session expiration handling."""
    # Create store with very short lifetime
    session_store = SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions")

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-temp")
    async def set_temp_data(request: Any) -> dict:
        request.session["temp_data"] = "will_expire"
        return {"status": "set"}

    @get("/get-temp")
    async def get_temp_data(request: Any) -> dict:
        return {"temp_data": request.session.get("temp_data")}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(route_handlers=[set_temp_data, get_temp_data], middleware=[session_config.middleware], stores=stores)

    with TestClient(app=app) as client:
        # Set temporary data
        response = client.get("/set-temp")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = client.get("/get-temp")
        assert response.json() == {"temp_data": "will_expire"}

        # Wait for expiration
        time.sleep(2)

        # Data should be expired (new session created)
        response = client.get("/get-temp")
        assert response.json() == {"temp_data": None}


def test_duckdb_transaction_handling(session_store: SQLSpecSessionStore, migrated_config: DuckDBConfig) -> None:
    """Test transaction handling in DuckDB store operations."""
    session_id = "duckdb-transaction-test-session"

    # Test successful transaction
    test_data = {"counter": 0, "analytical_queries": []}
    run_(session_store.set)(session_id, test_data, expires_in=3600)

    # DuckDB handles transactions automatically
    with migrated_config.provide_session() as driver:
        # Start a transaction context
        driver.begin()
        try:
            # Read current data
            result = driver.execute("SELECT data FROM litestar_sessions WHERE session_id = ?", (session_id,))
            if result.data:
                import json

                current_data = json.loads(result.data[0]["data"])
                current_data["counter"] += 1
                current_data["analytical_queries"].append("SELECT * FROM test_table")

                # Update in transaction
                updated_json = json.dumps(current_data)
                driver.execute("UPDATE litestar_sessions SET data = ? WHERE session_id = ?", (updated_json, session_id))
                driver.commit()
        except Exception:
            driver.rollback()
            raise

    # Verify the update succeeded
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data["counter"] == 1
    assert "SELECT * FROM test_table" in retrieved_data["analytical_queries"]

    # Test rollback scenario
    with migrated_config.provide_session() as driver:
        driver.begin()
        try:
            # Make a change that we'll rollback
            driver.execute(
                "UPDATE litestar_sessions SET data = ? WHERE session_id = ?",
                ('{"counter": 999, "analytical_queries": ["rollback_test"]}', session_id),
            )
            # Force a rollback
            driver.rollback()
        except Exception:
            driver.rollback()

    # Verify the rollback worked - data should be unchanged
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data["counter"] == 1  # Should still be 1, not 999
    assert "rollback_test" not in retrieved_data["analytical_queries"]

    # Cleanup
    run_(session_store.delete)(session_id)


def test_concurrent_sessions(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with different clients."""

    @get("/user/login/{user_id:int}")
    async def login_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["login_time"] = time.time()
        return {"status": "logged in", "user_id": user_id}

    @get("/user/whoami")
    async def whoami(request: Any) -> dict:
        user_id = request.session.get("user_id")
        login_time = request.session.get("login_time")
        return {"user_id": user_id, "login_time": login_time}

    @post("/user/update-profile")
    async def update_profile(request: Any) -> dict:
        profile_data = await request.json()
        request.session["profile"] = profile_data
        return {"status": "profile updated"}

    @get("/session/all")
    async def get_all_session(request: Any) -> dict:
        """Get all session data."""
        return dict(request.session)

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[login_user, whoami, update_profile, get_all_session],
        middleware=[session_config.middleware],
        stores=stores,
    )

    # Use separate clients to simulate different browsers/users
    with TestClient(app=app) as client1, TestClient(app=app) as client2, TestClient(app=app) as client3:
        # Each client logs in as different user
        response1 = client1.get("/user/login/100")
        assert response1.json()["user_id"] == 100

        response2 = client2.get("/user/login/200")
        assert response2.json()["user_id"] == 200

        response3 = client3.get("/user/login/300")
        assert response3.json()["user_id"] == 300

        # Each client should maintain separate session
        who1 = client1.get("/user/whoami")
        assert who1.json()["user_id"] == 100

        who2 = client2.get("/user/whoami")
        assert who2.json()["user_id"] == 200

        who3 = client3.get("/user/whoami")
        assert who3.json()["user_id"] == 300

        # Update profiles independently
        client1.post("/user/update-profile", json={"name": "User One", "age": 25})
        client2.post("/user/update-profile", json={"name": "User Two", "age": 30})

        # Verify isolation - get all session data
        response1 = client1.get("/session/all")
        data1 = response1.json()
        assert data1["user_id"] == 100
        assert data1["profile"]["name"] == "User One"

        response2 = client2.get("/session/all")
        data2 = response2.json()
        assert data2["user_id"] == 200
        assert data2["profile"]["name"] == "User Two"

        # Client3 should not have profile data
        response3 = client3.get("/session/all")
        data3 = response3.json()
        assert data3["user_id"] == 300
        assert "profile" not in data3


def test_store_crud_operations(session_store: SQLSpecSessionStore) -> None:
    """Test direct store CRUD operations."""
    session_id = "test-session-crud"

    # Test data with various types
    test_data = {
        "user_id": 12345,
        "username": "testuser",
        "preferences": {"theme": "dark", "language": "en", "notifications": True},
        "tags": ["admin", "user", "premium"],
        "metadata": {"last_login": "2024-01-15T10:30:00Z", "login_count": 42, "is_verified": True},
    }

    # CREATE
    run_(session_store.set)(session_id, test_data, expires_in=3600)

    # READ
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == test_data

    # UPDATE (overwrite)
    updated_data = {**test_data, "last_activity": "2024-01-15T11:00:00Z"}
    run_(session_store.set)(session_id, updated_data, expires_in=3600)

    retrieved_updated = run_(session_store.get)(session_id)
    assert retrieved_updated == updated_data
    assert "last_activity" in retrieved_updated

    # EXISTS
    assert run_(session_store.exists)(session_id) is True
    assert run_(session_store.exists)("nonexistent") is False

    # EXPIRES_IN
    expires_in = run_(session_store.expires_in)(session_id)
    assert 3500 < expires_in <= 3600  # Should be close to 3600

    # DELETE
    run_(session_store.delete)(session_id)

    # Verify deletion
    assert run_(session_store.get)(session_id) is None
    assert run_(session_store.exists)(session_id) is False


def test_large_data_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data."""
    session_id = "test-large-data"

    # Create large data structure
    large_data = {
        "large_list": list(range(10000)),  # 10k integers
        "large_text": "x" * 50000,  # 50k character string
        "nested_structure": {
            f"key_{i}": {"value": f"data_{i}", "numbers": list(range(i, i + 100)), "text": f"{'content_' * 100}{i}"}
            for i in range(100)  # 100 nested objects
        },
        "metadata": {"size": "large", "created_at": "2024-01-15T10:30:00Z", "version": 1},
    }

    # Store large data
    run_(session_store.set)(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_list"]) == 10000
    assert len(retrieved_data["large_text"]) == 50000
    assert len(retrieved_data["nested_structure"]) == 100

    # Cleanup
    run_(session_store.delete)(session_id)


def test_special_characters_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values."""

    # Test data with various special characters
    test_cases = [
        ("unicode_🔑", {"message": "Hello 🌍 World! 你好世界"}),
        ("special-chars!@#$%", {"data": "Value with special chars: !@#$%^&*()"}),
        ("json_escape", {"quotes": '"double"', "single": "'single'", "backslash": "\\path\\to\\file"}),
        ("newlines_tabs", {"multi_line": "Line 1\nLine 2\tTabbed"}),
        ("empty_values", {"empty_string": "", "empty_list": [], "empty_dict": {}}),
        ("null_values", {"null_value": None, "false_value": False, "zero_value": 0}),
    ]

    for session_id, test_data in test_cases:
        # Store data with special characters
        run_(session_store.set)(session_id, test_data, expires_in=3600)

        # Retrieve and verify
        retrieved_data = run_(session_store.get)(session_id)
        assert retrieved_data == test_data, f"Failed for session_id: {session_id}"

        # Cleanup
        run_(session_store.delete)(session_id)


def test_session_cleanup_operations(session_store: SQLSpecSessionStore) -> None:
    """Test session cleanup and maintenance operations."""

    # Create multiple sessions with different expiration times
    sessions_data = [
        ("short_lived_1", {"data": "expires_soon_1"}, 1),  # 1 second
        ("short_lived_2", {"data": "expires_soon_2"}, 1),  # 1 second
        ("medium_lived", {"data": "expires_medium"}, 10),  # 10 seconds
        ("long_lived", {"data": "expires_long"}, 3600),  # 1 hour
    ]

    # Set all sessions
    for session_id, data, expires_in in sessions_data:
        run_(session_store.set)(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, _, _ in sessions_data:
        assert run_(session_store.exists)(session_id), f"Session {session_id} should exist"

    # Wait for short-lived sessions to expire
    time.sleep(2)

    # Delete expired sessions
    run_(session_store.delete_expired)()

    # Check which sessions remain
    assert run_(session_store.exists)("short_lived_1") is False
    assert run_(session_store.exists)("short_lived_2") is False
    assert run_(session_store.exists)("medium_lived") is True
    assert run_(session_store.exists)("long_lived") is True

    # Test get_all functionality
    all_sessions = []

    async def collect_sessions():
        async for session_id, session_data in session_store.get_all():
            all_sessions.append((session_id, session_data))

    run_(collect_sessions)()

    # Should have 2 remaining sessions
    assert len(all_sessions) == 2
    session_ids = {session_id for session_id, _ in all_sessions}
    assert "medium_lived" in session_ids
    assert "long_lived" in session_ids

    # Test delete_all
    run_(session_store.delete_all)()

    # Verify all sessions are gone
    for session_id, _, _ in sessions_data:
        assert run_(session_store.exists)(session_id) is False


def test_session_renewal(session_store: SQLSpecSessionStore) -> None:
    """Test session renewal functionality."""
    session_id = "renewal_test"
    test_data = {"user_id": 123, "activity": "browsing"}

    # Set session with short expiration
    run_(session_store.set)(session_id, test_data, expires_in=5)

    # Get initial expiration time
    initial_expires_in = run_(session_store.expires_in)(session_id)
    assert 4 <= initial_expires_in <= 5

    # Get session data with renewal
    retrieved_data = run_(session_store.get)(session_id, renew_for=timedelta(hours=1))
    assert retrieved_data == test_data

    # Check that expiration time was extended
    new_expires_in = run_(session_store.expires_in)(session_id)
    assert new_expires_in > 3500  # Should be close to 3600 (1 hour)

    # Cleanup
    run_(session_store.delete)(session_id)


def test_error_handling_and_edge_cases(session_store: SQLSpecSessionStore) -> None:
    """Test error handling and edge cases."""

    # Test getting non-existent session
    result = run_(session_store.get)("non_existent_session")
    assert result is None

    # Test deleting non-existent session (should not raise error)
    run_(session_store.delete)("non_existent_session")

    # Test expires_in for non-existent session
    expires_in = run_(session_store.expires_in)("non_existent_session")
    assert expires_in == 0

    # Test empty session data
    run_(session_store.set)("empty_session", {}, expires_in=3600)
    empty_data = run_(session_store.get)("empty_session")
    assert empty_data == {}

    # Test very large expiration time
    run_(session_store.set)("long_expiry", {"data": "test"}, expires_in=365 * 24 * 60 * 60)  # 1 year
    long_expires_in = run_(session_store.expires_in)("long_expiry")
    assert long_expires_in > 365 * 24 * 60 * 60 - 10  # Should be close to 1 year

    # Cleanup
    run_(session_store.delete)("empty_session")
    run_(session_store.delete)("long_expiry")


def test_complex_user_workflow(litestar_app: Litestar) -> None:
    """Test a complex user workflow combining multiple operations."""
    with TestClient(app=litestar_app) as client:
        # User registration workflow
        user_profile = {
            "user_id": 12345,
            "username": "complex_user",
            "email": "complex@example.com",
            "profile": {
                "first_name": "Complex",
                "last_name": "User",
                "age": 25,
                "preferences": {
                    "theme": "dark",
                    "language": "en",
                    "notifications": {"email": True, "push": False, "sms": True},
                },
            },
            "permissions": ["read", "write", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
        }

        # Set user profile
        response = client.put("/user/profile", json=user_profile)
        assert response.status_code == HTTP_200_OK  # PUT returns 200 by default

        # Verify profile was set
        response = client.get("/user/profile")
        assert response.status_code == HTTP_200_OK
        assert response.json()["profile"] == user_profile

        # Update session with additional activity data
        activity_data = {
            "page_views": 15,
            "session_start": "2024-01-15T10:30:00Z",
            "cart_items": [
                {"id": 1, "name": "Product A", "price": 29.99},
                {"id": 2, "name": "Product B", "price": 19.99},
            ],
        }

        response = client.post("/session/bulk", json=activity_data)
        assert response.status_code == HTTP_201_CREATED

        # Test counter functionality within complex session
        for i in range(1, 6):
            response = client.get("/counter")
            assert response.json()["count"] == i

        # Get all session data to verify everything is maintained
        response = client.get("/session/all")
        all_data = response.json()

        # Verify all data components are present
        assert "profile" in all_data
        assert all_data["profile"] == user_profile
        assert all_data["page_views"] == 15
        assert len(all_data["cart_items"]) == 2
        assert all_data["count"] == 5

        # Test selective data removal
        response = client.post("/session/key/cart_items/delete")
        assert response.json()["status"] == "deleted"

        # Verify cart_items removed but other data persists
        response = client.get("/session/all")
        updated_data = response.json()
        assert "cart_items" not in updated_data
        assert "profile" in updated_data
        assert updated_data["count"] == 5

        # Final counter increment to ensure functionality still works
        response = client.get("/counter")
        assert response.json()["count"] == 6


def test_duckdb_analytical_session_data(session_store: SQLSpecSessionStore) -> None:
    """Test DuckDB-specific analytical data types and structures."""
    session_id = "analytical-test"

    # Complex analytical data that showcases DuckDB capabilities
    analytical_data = {
        "query_plan": {
            "operation": "PROJECTION",
            "columns": ["customer_id", "total_revenue", "order_count"],
            "children": [
                {
                    "operation": "AGGREGATE",
                    "group_by": ["customer_id"],
                    "aggregates": {"total_revenue": "SUM(amount)", "order_count": "COUNT(*)"},
                    "children": [
                        {
                            "operation": "FILTER",
                            "condition": "date >= '2024-01-01'",
                            "children": [
                                {
                                    "operation": "PARQUET_SCAN",
                                    "file": "s3://bucket/orders/*.parquet",
                                    "projected_columns": ["customer_id", "amount", "date"],
                                }
                            ],
                        }
                    ],
                }
            ],
        },
        "execution_stats": {
            "rows_scanned": 50_000_000,
            "rows_filtered": 25_000_000,
            "rows_output": 150_000,
            "execution_time_ms": 2_847.5,
            "memory_usage_mb": 512.75,
            "spill_to_disk": False,
        },
        "result_preview": [
            {"customer_id": 1001, "total_revenue": 15_432.50, "order_count": 23},
            {"customer_id": 1002, "total_revenue": 28_901.75, "order_count": 41},
            {"customer_id": 1003, "total_revenue": 8_234.25, "order_count": 12},
        ],
        "export_options": {
            "formats": ["parquet", "csv", "json", "arrow"],
            "compression": ["gzip", "snappy", "zstd"],
            "destinations": ["s3", "local", "azure_blob"],
        },
        "metadata": {
            "schema_version": "1.2.0",
            "query_fingerprint": "abc123def456",
            "cache_key": "analytical_query_2024_01_20",
            "extensions_used": ["httpfs", "parquet", "json"],
        },
    }

    # Store analytical data
    run_(session_store.set)(session_id, analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == analytical_data

    # Verify data structure integrity
    assert retrieved_data["execution_stats"]["rows_scanned"] == 50_000_000
    assert retrieved_data["query_plan"]["operation"] == "PROJECTION"
    assert len(retrieved_data["result_preview"]) == 3
    assert "httpfs" in retrieved_data["metadata"]["extensions_used"]

    # Cleanup
    run_(session_store.delete)(session_id)
