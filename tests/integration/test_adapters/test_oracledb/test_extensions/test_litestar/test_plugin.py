"""Comprehensive Litestar integration tests for OracleDB adapter."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore

pytestmark = [pytest.mark.oracledb, pytest.mark.oracle, pytest.mark.integration, pytest.mark.xdist_group("oracle")]


@pytest.fixture
async def oracle_session_store_async(oracle_async_config: OracleAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store instance for Oracle."""
    store = SQLSpecSessionStore(
        config=oracle_async_config,
        table_name="test_litestar_sessions_async",
        session_id_column="session_id",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists
    async with oracle_async_config.provide_session() as driver:
        await store._ensure_table_exists(driver)
    return store


@pytest.fixture
def oracle_session_store_sync(oracle_sync_config: OracleSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store instance for Oracle."""
    store = SQLSpecSessionStore(
        config=oracle_sync_config,
        table_name="test_litestar_sessions_sync",
        session_id_column="session_id",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists (using async context for setup)

    async def setup_table():
        async with oracle_sync_config.provide_session() as driver:
            await store._ensure_table_exists(driver)

    # Run setup in async context
    import asyncio

    asyncio.run(setup_table())
    return store


@pytest.fixture
async def oracle_session_backend_async(oracle_async_config: OracleAsyncConfig) -> SQLSpecSessionBackend:
    """Create an async session backend instance for Oracle."""
    backend = SQLSpecSessionBackend(
        config=oracle_async_config, table_name="test_litestar_backend_async", session_lifetime=3600
    )
    # Ensure table exists
    async with oracle_async_config.provide_session() as driver:
        await backend.store._ensure_table_exists(driver)
    return backend


@pytest.fixture
def oracle_session_backend_sync(oracle_sync_config: OracleSyncConfig) -> SQLSpecSessionBackend:
    """Create a sync session backend instance for Oracle."""
    backend = SQLSpecSessionBackend(
        config=oracle_sync_config, table_name="test_litestar_backend_sync", session_lifetime=3600
    )
    # Ensure table exists (using async context for setup)

    async def setup_table():
        async with oracle_sync_config.provide_session() as driver:
            await backend.store._ensure_table_exists(driver)

    # Run setup in async context
    import asyncio

    asyncio.run(setup_table())
    return backend


async def test_oracle_async_session_store_basic_operations(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test basic session store operations with Oracle async driver."""
    session_id = f"oracle-async-test-{uuid4()}"
    session_data = {
        "user_id": 12345,
        "username": "oracle_async_user",
        "preferences": {"theme": "dark", "language": "en", "timezone": "America/New_York"},
        "roles": ["user", "admin"],
        "oracle_features": {"plsql_enabled": True, "vectordb_enabled": True, "json_support": True},
    }

    # Set session data
    await oracle_session_store_async.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await oracle_session_store_async.get(session_id)
    assert retrieved_data == session_data

    # Update session data with Oracle-specific information
    updated_data = {
        **session_data,
        "last_login": "2024-01-01T12:00:00Z",
        "oracle_metadata": {"sid": "ORCL", "instance_name": "oracle_instance", "container": "PDB1"},
    }
    await oracle_session_store_async.set(session_id, updated_data, expires_in=3600)

    # Verify update
    retrieved_data = await oracle_session_store_async.get(session_id)
    assert retrieved_data == updated_data
    assert retrieved_data["oracle_metadata"]["sid"] == "ORCL"

    # Delete session
    await oracle_session_store_async.delete(session_id)

    # Verify deletion
    result = await oracle_session_store_async.get(session_id, None)
    assert result is None


def test_oracle_sync_session_store_basic_operations(oracle_session_store_sync: SQLSpecSessionStore) -> None:
    """Test basic session store operations with Oracle sync driver."""
    import asyncio

    async def run_sync_test():
        session_id = f"oracle-sync-test-{uuid4()}"
        session_data = {
            "user_id": 54321,
            "username": "oracle_sync_user",
            "preferences": {"theme": "light", "language": "en"},
            "database_info": {"dialect": "oracle", "version": "23ai", "features": ["plsql", "json", "vector"]},
        }

        # Set session data
        await oracle_session_store_sync.set(session_id, session_data, expires_in=3600)

        # Get session data
        retrieved_data = await oracle_session_store_sync.get(session_id)
        assert retrieved_data == session_data

        # Delete session
        await oracle_session_store_sync.delete(session_id)

        # Verify deletion
        result = await oracle_session_store_sync.get(session_id, None)
        assert result is None

    asyncio.run(run_sync_test())


async def test_oracle_json_data_support(
    oracle_session_store_async: SQLSpecSessionStore, oracle_async_config: OracleAsyncConfig
) -> None:
    """Test Oracle JSON data type support for complex session data."""
    session_id = f"oracle-json-test-{uuid4()}"

    # Complex nested data that utilizes Oracle's JSON capabilities
    complex_data = {
        "user_profile": {
            "personal": {
                "name": "Oracle User",
                "age": 35,
                "location": {"city": "Redwood City", "state": "CA", "coordinates": {"lat": 37.4845, "lng": -122.2285}},
            },
            "enterprise_features": {
                "analytics": {"enabled": True, "level": "advanced"},
                "machine_learning": {"models": ["regression", "classification"], "enabled": True},
                "blockchain": {"tables": ["audit_log", "transactions"], "enabled": False},
            },
        },
        "oracle_specific": {
            "plsql_packages": ["DBMS_SCHEDULER", "DBMS_STATS", "DBMS_VECTOR"],
            "advanced_features": {"autonomous": True, "exadata": False, "multitenant": True, "inmemory": True},
        },
        "large_dataset": [{"id": i, "value": f"oracle_data_{i}"} for i in range(500)],
    }

    # Store complex data
    await oracle_session_store_async.set(session_id, complex_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await oracle_session_store_async.get(session_id)
    assert retrieved_data == complex_data
    assert retrieved_data["oracle_specific"]["advanced_features"]["autonomous"] is True
    assert len(retrieved_data["large_dataset"]) == 500

    # Verify data is properly stored in Oracle database
    async with oracle_async_config.provide_session() as driver:
        result = await driver.execute(
            f"SELECT session_data FROM {oracle_session_store_async._table_name} WHERE session_id = :1", (session_id,)
        )
        assert len(result.data) == 1
        stored_data = result.data[0]["SESSION_DATA"]
        assert isinstance(stored_data, (dict, str))  # Could be parsed or string depending on driver


async def test_oracle_async_session_backend_litestar_integration(
    oracle_session_backend_async: SQLSpecSessionBackend,
) -> None:
    """Test SQLSpecSessionBackend integration with Litestar application using Oracle async."""

    @get("/set-oracle-session")
    async def set_oracle_session(request: Any) -> dict:
        request.session["user_id"] = 99999
        request.session["username"] = "oracle_litestar_user"
        request.session["roles"] = ["dba", "developer"]
        request.session["oracle_config"] = {
            "instance": "ORCL",
            "service_name": "oracle23ai",
            "features_enabled": ["vector_search", "json_relational_duality", "graph_analytics"],
        }
        request.session["plsql_capabilities"] = {
            "procedures": True,
            "functions": True,
            "packages": True,
            "triggers": True,
        }
        return {"status": "oracle session set"}

    @get("/get-oracle-session")
    async def get_oracle_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "roles": request.session.get("roles"),
            "oracle_config": request.session.get("oracle_config"),
            "plsql_capabilities": request.session.get("plsql_capabilities"),
        }

    @post("/update-oracle-preferences")
    async def update_oracle_preferences(request: Any) -> dict:
        oracle_prefs = request.session.get("oracle_preferences", {})
        oracle_prefs.update({
            "optimizer_mode": "ALL_ROWS",
            "nls_language": "AMERICAN",
            "nls_territory": "AMERICA",
            "parallel_degree": 4,
        })
        request.session["oracle_preferences"] = oracle_prefs
        return {"status": "oracle preferences updated"}

    @post("/clear-oracle-session")
    async def clear_oracle_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "oracle session cleared"}

    session_config = ServerSideSessionConfig(
        backend=oracle_session_backend_async, key="oracle-async-test-session", max_age=3600
    )

    app = Litestar(
        route_handlers=[set_oracle_session, get_oracle_session, update_oracle_preferences, clear_oracle_session],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Set Oracle-specific session
        response = await client.get("/set-oracle-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "oracle session set"}

        # Get Oracle session data
        response = await client.get("/get-oracle-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 99999
        assert data["username"] == "oracle_litestar_user"
        assert data["roles"] == ["dba", "developer"]
        assert data["oracle_config"]["instance"] == "ORCL"
        assert "vector_search" in data["oracle_config"]["features_enabled"]
        assert data["plsql_capabilities"]["procedures"] is True

        # Update Oracle preferences
        response = await client.post("/update-oracle-preferences")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "oracle preferences updated"}

        # Verify Oracle preferences were added
        response = await client.get("/get-oracle-session")
        data = response.json()
        assert "oracle_preferences" in data
        oracle_prefs = data["oracle_preferences"]
        assert oracle_prefs["optimizer_mode"] == "ALL_ROWS"
        assert oracle_prefs["parallel_degree"] == 4

        # Clear session
        response = await client.post("/clear-oracle-session")
        assert response.status_code == HTTP_200_OK

        # Verify session is cleared
        response = await client.get("/get-oracle-session")
        data = response.json()
        assert all(value is None for value in data.values())


async def test_oracle_session_persistence_with_plsql_metadata(
    oracle_session_backend_async: SQLSpecSessionBackend,
) -> None:
    """Test session persistence with Oracle PL/SQL execution metadata."""

    @get("/plsql-counter")
    async def plsql_counter_endpoint(request: Any) -> dict:
        # Simulate PL/SQL execution tracking
        executions = request.session.get("plsql_executions", [])
        block_count = request.session.get("block_count", 0)

        block_count += 1
        execution_info = {
            "block_id": f"BLOCK_{block_count}",
            "timestamp": f"2024-01-01T12:{block_count:02d}:00Z",
            "procedure": f"test_procedure_{block_count}",
            "status": "SUCCESS",
            "execution_time_ms": block_count * 10,
        }
        executions.append(execution_info)

        request.session["block_count"] = block_count
        request.session["plsql_executions"] = executions
        request.session["last_plsql_block"] = execution_info

        return {"block_count": block_count, "executions": executions, "last_execution": execution_info}

    session_config = ServerSideSessionConfig(
        backend=oracle_session_backend_async, key="oracle-plsql-persistence-test", max_age=3600
    )

    app = Litestar(route_handlers=[plsql_counter_endpoint], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # First PL/SQL execution
        response = await client.get("/plsql-counter")
        data = response.json()
        assert data["block_count"] == 1
        assert len(data["executions"]) == 1
        assert data["last_execution"]["block_id"] == "BLOCK_1"
        assert data["last_execution"]["procedure"] == "test_procedure_1"

        # Second PL/SQL execution
        response = await client.get("/plsql-counter")
        data = response.json()
        assert data["block_count"] == 2
        assert len(data["executions"]) == 2
        assert data["last_execution"]["block_id"] == "BLOCK_2"

        # Third PL/SQL execution
        response = await client.get("/plsql-counter")
        data = response.json()
        assert data["block_count"] == 3
        assert len(data["executions"]) == 3
        assert data["executions"][0]["block_id"] == "BLOCK_1"
        assert data["executions"][2]["execution_time_ms"] == 30


async def test_oracle_session_expiration(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test session expiration functionality with Oracle."""
    session_id = f"oracle-expiration-test-{uuid4()}"
    session_data = {
        "user_id": 777,
        "oracle_test": "expiration",
        "database_features": ["autonomous", "exadata", "cloud"],
    }

    # Set session with very short expiration
    await oracle_session_store_async.set(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = await oracle_session_store_async.get(session_id)
    assert result == session_data

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await oracle_session_store_async.get(session_id, None)
    assert result is None


async def test_oracle_concurrent_session_operations(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test concurrent session operations with Oracle async driver."""

    async def create_oracle_session(session_num: int) -> None:
        """Create a session with Oracle-specific data."""
        session_id = f"oracle-concurrent-{session_num}"
        session_data = {
            "session_number": session_num,
            "oracle_sid": f"ORCL{session_num}",
            "database_role": "PRIMARY" if session_num % 2 == 0 else "STANDBY",
            "features": {
                "json_enabled": True,
                "vector_search": session_num % 3 == 0,
                "graph_analytics": session_num % 5 == 0,
            },
            "timestamp": f"2024-01-01T12:{session_num:02d}:00Z",
        }
        await oracle_session_store_async.set(session_id, session_data, expires_in=3600)

    async def read_oracle_session(session_num: int) -> "dict[str, Any] | None":
        """Read an Oracle session by number."""
        session_id = f"oracle-concurrent-{session_num}"
        return await oracle_session_store_async.get(session_id, None)

    # Create multiple Oracle sessions concurrently
    create_tasks = [create_oracle_session(i) for i in range(15)]
    await asyncio.gather(*create_tasks)

    # Read all sessions concurrently
    read_tasks = [read_oracle_session(i) for i in range(15)]
    results = await asyncio.gather(*read_tasks)

    # Verify all sessions were created and can be read
    assert len(results) == 15
    for i, result in enumerate(results):
        assert result is not None
        assert result["session_number"] == i
        assert result["oracle_sid"] == f"ORCL{i}"
        assert result["database_role"] in ["PRIMARY", "STANDBY"]
        assert result["features"]["json_enabled"] is True


async def test_oracle_large_session_data_with_clob(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test handling of large session data with Oracle CLOB support."""
    session_id = f"oracle-large-data-{uuid4()}"

    # Create large session data that would benefit from CLOB storage
    large_oracle_data = {
        "user_id": 88888,
        "oracle_metadata": {
            "instance_details": {"sga_size": "2GB", "pga_size": "1GB", "shared_pool": "512MB", "buffer_cache": "1GB"},
            "tablespace_info": [
                {
                    "name": f"TABLESPACE_{i}",
                    "size_mb": 1000 + i * 100,
                    "used_mb": 500 + i * 50,
                    "datafiles": [f"datafile_{i}_{j}.dbf" for j in range(5)],
                }
                for i in range(50)
            ],
        },
        "large_plsql_log": "x" * 100000,  # 100KB of text for CLOB testing
        "query_history": [
            {
                "query_id": f"QRY_{i}",
                "sql_text": f"SELECT * FROM large_table_{i} WHERE condition = :param{i}" * 20,
                "execution_plan": f"execution_plan_data_for_query_{i}" * 50,
                "statistics": {"logical_reads": 1000 + i, "physical_reads": 100 + i, "elapsed_time": 0.1 + i * 0.01},
            }
            for i in range(200)
        ],
        "vector_embeddings": {
            f"embedding_{i}": [float(j) for j in range(768)]
            for i in range(10)  # 10 embeddings with 768 dimensions each
        },
    }

    # Store large Oracle data
    await oracle_session_store_async.set(session_id, large_oracle_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await oracle_session_store_async.get(session_id)
    assert retrieved_data == large_oracle_data
    assert len(retrieved_data["large_plsql_log"]) == 100000
    assert len(retrieved_data["oracle_metadata"]["tablespace_info"]) == 50
    assert len(retrieved_data["query_history"]) == 200
    assert len(retrieved_data["vector_embeddings"]) == 10
    assert len(retrieved_data["vector_embeddings"]["embedding_0"]) == 768


async def test_oracle_session_cleanup_operations(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test session cleanup and maintenance operations with Oracle."""

    # Create sessions with different expiration times and Oracle-specific data
    oracle_sessions_data = [
        (
            f"oracle-short-{i}",
            {"data": f"oracle_short_{i}", "instance": f"ORCL_SHORT_{i}", "features": ["basic", "json"]},
            1,
        )
        for i in range(3)  # Will expire quickly
    ] + [
        (
            f"oracle-long-{i}",
            {"data": f"oracle_long_{i}", "instance": f"ORCL_LONG_{i}", "features": ["advanced", "vector", "analytics"]},
            3600,
        )
        for i in range(3)  # Won't expire
    ]

    # Set all Oracle sessions
    for session_id, data, expires_in in oracle_sessions_data:
        await oracle_session_store_async.set(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, expected_data, _ in oracle_sessions_data:
        result = await oracle_session_store_async.get(session_id)
        assert result == expected_data

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await oracle_session_store_async.delete_expired()

    # Verify short sessions are gone and long sessions remain
    for session_id, expected_data, expires_in in oracle_sessions_data:
        result = await oracle_session_store_async.get(session_id, None)
        if expires_in == 1:  # Short expiration
            assert result is None
        else:  # Long expiration
            assert result == expected_data
            assert "advanced" in result["features"]


async def test_oracle_transaction_handling_in_sessions(
    oracle_session_store_async: SQLSpecSessionStore, oracle_async_config: OracleAsyncConfig
) -> None:
    """Test transaction handling in Oracle session operations."""
    session_id = f"oracle-transaction-test-{uuid4()}"

    # Test that session operations work within Oracle transactions
    async with oracle_async_config.provide_session() as driver:
        async with driver.begin_transaction():
            # Set session data within transaction
            oracle_session_data = {
                "test": "oracle_transaction",
                "oracle_features": {"acid_compliance": True, "read_consistency": True, "flashback": True},
                "transaction_info": {"isolation_level": "READ_COMMITTED", "autocommit": False},
            }
            await oracle_session_store_async.set(session_id, oracle_session_data, expires_in=3600)

            # Verify data is accessible within same transaction
            result = await oracle_session_store_async.get(session_id)
            assert result == oracle_session_data

            # Update data within transaction
            updated_data = {**oracle_session_data, "status": "updated_in_transaction"}
            await oracle_session_store_async.set(session_id, updated_data, expires_in=3600)

        # Verify data persists after transaction commit
        result = await oracle_session_store_async.get(session_id)
        assert result == updated_data
        assert result["status"] == "updated_in_transaction"
        assert result["oracle_features"]["acid_compliance"] is True


async def test_oracle_session_backend_error_handling(oracle_session_backend_async: SQLSpecSessionBackend) -> None:
    """Test error handling in Oracle session backend operations."""

    @get("/oracle-error-test")
    async def oracle_error_test_endpoint(request: Any) -> dict:
        try:
            # Set Oracle-specific session data
            request.session["oracle_instance"] = "ORCL_ERROR_TEST"
            request.session["valid_key"] = "oracle_valid_value"
            request.session["plsql_block"] = {
                "procedure_name": "test_procedure",
                "parameters": {"p1": "value1", "p2": "value2"},
                "execution_status": "SUCCESS",
            }
            return {
                "status": "oracle_success",
                "value": request.session.get("valid_key"),
                "oracle_instance": request.session.get("oracle_instance"),
            }
        except Exception as e:
            return {"status": "oracle_error", "message": str(e)}

    session_config = ServerSideSessionConfig(
        backend=oracle_session_backend_async, key="oracle-error-test-session", max_age=3600
    )

    app = Litestar(route_handlers=[oracle_error_test_endpoint], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        response = await client.get("/oracle-error-test")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["status"] == "oracle_success"
        assert data["value"] == "oracle_valid_value"
        assert data["oracle_instance"] == "ORCL_ERROR_TEST"


async def test_multiple_oracle_apps_with_separate_backends(oracle_async_config: OracleAsyncConfig) -> None:
    """Test multiple Litestar applications with separate Oracle session backends."""

    # Create separate Oracle backends for different applications
    oracle_backend1 = SQLSpecSessionBackend(
        config=oracle_async_config, table_name="oracle_app1_sessions", session_lifetime=3600
    )

    oracle_backend2 = SQLSpecSessionBackend(
        config=oracle_async_config, table_name="oracle_app2_sessions", session_lifetime=3600
    )

    # Ensure tables exist
    async with oracle_async_config.provide_session() as driver:
        await oracle_backend1.store._ensure_table_exists(driver)
        await oracle_backend2.store._ensure_table_exists(driver)

    @get("/oracle-app1-data")
    async def oracle_app1_endpoint(request: Any) -> dict:
        request.session["app"] = "oracle_app1"
        request.session["oracle_config"] = {
            "instance": "ORCL_APP1",
            "service_name": "app1_service",
            "features": ["json", "vector"],
        }
        request.session["data"] = "oracle_app1_data"
        return {
            "app": "oracle_app1",
            "data": request.session["data"],
            "oracle_instance": request.session["oracle_config"]["instance"],
        }

    @get("/oracle-app2-data")
    async def oracle_app2_endpoint(request: Any) -> dict:
        request.session["app"] = "oracle_app2"
        request.session["oracle_config"] = {
            "instance": "ORCL_APP2",
            "service_name": "app2_service",
            "features": ["analytics", "ml"],
        }
        request.session["data"] = "oracle_app2_data"
        return {
            "app": "oracle_app2",
            "data": request.session["data"],
            "oracle_instance": request.session["oracle_config"]["instance"],
        }

    # Create separate Oracle apps
    oracle_app1 = Litestar(
        route_handlers=[oracle_app1_endpoint],
        middleware=[ServerSideSessionConfig(backend=oracle_backend1, key="oracle_app1").middleware],
    )

    oracle_app2 = Litestar(
        route_handlers=[oracle_app2_endpoint],
        middleware=[ServerSideSessionConfig(backend=oracle_backend2, key="oracle_app2").middleware],
    )

    # Test both Oracle apps concurrently
    async with AsyncTestClient(app=oracle_app1) as client1, AsyncTestClient(app=oracle_app2) as client2:
        # Make requests to both apps
        response1 = await client1.get("/oracle-app1-data")
        response2 = await client2.get("/oracle-app2-data")

        # Verify responses
        assert response1.status_code == HTTP_200_OK
        data1 = response1.json()
        assert data1["app"] == "oracle_app1"
        assert data1["data"] == "oracle_app1_data"
        assert data1["oracle_instance"] == "ORCL_APP1"

        assert response2.status_code == HTTP_200_OK
        data2 = response2.json()
        assert data2["app"] == "oracle_app2"
        assert data2["data"] == "oracle_app2_data"
        assert data2["oracle_instance"] == "ORCL_APP2"

        # Verify session data is isolated between Oracle apps
        response1_second = await client1.get("/oracle-app1-data")
        response2_second = await client2.get("/oracle-app2-data")

        assert response1_second.json()["data"] == "oracle_app1_data"
        assert response2_second.json()["data"] == "oracle_app2_data"
        assert response1_second.json()["oracle_instance"] == "ORCL_APP1"
        assert response2_second.json()["oracle_instance"] == "ORCL_APP2"


async def test_oracle_enterprise_features_in_sessions(oracle_session_store_async: SQLSpecSessionStore) -> None:
    """Test Oracle enterprise features integration in session data."""
    session_id = f"oracle-enterprise-{uuid4()}"

    # Enterprise-level Oracle configuration in session
    enterprise_session_data = {
        "user_id": 11111,
        "enterprise_config": {
            "rac_enabled": True,
            "data_guard_config": {
                "primary_db": "ORCL_PRIMARY",
                "standby_dbs": ["ORCL_STANDBY1", "ORCL_STANDBY2"],
                "protection_mode": "MAXIMUM_PERFORMANCE",
            },
            "exadata_features": {"smart_scan": True, "storage_indexes": True, "hybrid_columnar_compression": True},
            "autonomous_features": {
                "auto_scaling": True,
                "auto_backup": True,
                "auto_patching": True,
                "threat_detection": True,
            },
        },
        "vector_config": {
            "vector_memory_size": "1G",
            "vector_format": "FLOAT32",
            "similarity_functions": ["COSINE", "EUCLIDEAN", "DOT"],
        },
        "json_relational_duality": {
            "collections": ["users", "orders", "products"],
            "views_enabled": True,
            "rest_apis_enabled": True,
        },
        "machine_learning": {
            "algorithms": ["regression", "classification", "clustering", "anomaly_detection"],
            "models_deployed": 15,
            "auto_ml_enabled": True,
        },
    }

    # Store enterprise session data
    await oracle_session_store_async.set(
        session_id, enterprise_session_data, expires_in=7200
    )  # Longer session for enterprise

    # Retrieve and verify all enterprise features
    retrieved_data = await oracle_session_store_async.get(session_id)
    assert retrieved_data == enterprise_session_data

    # Verify specific enterprise features
    assert retrieved_data["enterprise_config"]["rac_enabled"] is True
    assert len(retrieved_data["enterprise_config"]["data_guard_config"]["standby_dbs"]) == 2
    assert retrieved_data["enterprise_config"]["exadata_features"]["smart_scan"] is True
    assert retrieved_data["vector_config"]["vector_memory_size"] == "1G"
    assert "COSINE" in retrieved_data["vector_config"]["similarity_functions"]
    assert retrieved_data["json_relational_duality"]["views_enabled"] is True
    assert retrieved_data["machine_learning"]["models_deployed"] == 15

    # Update enterprise configuration
    updated_enterprise_data = {
        **enterprise_session_data,
        "enterprise_config": {
            **enterprise_session_data["enterprise_config"],
            "autonomous_features": {
                **enterprise_session_data["enterprise_config"]["autonomous_features"],
                "auto_indexing": True,
                "auto_partitioning": True,
            },
        },
        "performance_monitoring": {
            "awr_enabled": True,
            "addm_enabled": True,
            "sql_tuning_advisor": True,
            "real_time_sql_monitoring": True,
        },
    }

    await oracle_session_store_async.set(session_id, updated_enterprise_data, expires_in=7200)

    # Verify enterprise updates
    final_data = await oracle_session_store_async.get(session_id)
    assert final_data["enterprise_config"]["autonomous_features"]["auto_indexing"] is True
    assert final_data["performance_monitoring"]["awr_enabled"] is True
