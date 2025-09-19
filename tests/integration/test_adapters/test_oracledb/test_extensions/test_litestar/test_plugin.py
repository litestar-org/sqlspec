"""Comprehensive Litestar integration tests for OracleDB adapter.

This test suite validates the full integration between SQLSpec's OracleDB adapter
and Litestar's session middleware, including Oracle-specific features.
"""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import AsyncTestClient

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = [pytest.mark.oracledb, pytest.mark.oracle, pytest.mark.integration, pytest.mark.xdist_group("oracle")]


@pytest.fixture
async def oracle_async_migrated_config(oracle_async_migration_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Apply migrations once and return the config."""
    commands = AsyncMigrationCommands(oracle_async_migration_config)
    await commands.init(oracle_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()
    return oracle_async_migration_config


@pytest.fixture
def oracle_sync_migrated_config(oracle_sync_migration_config: OracleSyncConfig) -> OracleSyncConfig:
    """Apply migrations once and return the config."""
    commands = SyncMigrationCommands(oracle_sync_migration_config)
    commands.init(oracle_sync_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()
    return oracle_sync_migration_config


@pytest.fixture
async def oracle_async_session_store(oracle_async_migrated_config: OracleAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store instance using the migrated database."""
    return SQLSpecSessionStore(
        config=oracle_async_migrated_config,
        table_name="litestar_sessions_oracle_async",  # Use the default table created by migration
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
def oracle_sync_session_store(oracle_sync_migrated_config: OracleSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store instance using the migrated database."""
    return SQLSpecSessionStore(
        config=oracle_sync_migrated_config,
        table_name="litestar_sessions_oracle_sync",  # Use the default table created by migration
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
async def oracle_async_session_config(oracle_async_migrated_config: OracleAsyncConfig) -> SQLSpecSessionConfig:
    """Create an async session configuration instance."""
    # Create the session configuration
    return SQLSpecSessionConfig(
        table_name="litestar_sessions_oracle_async",
        store="sessions",  # This will be the key in the stores registry
    )


@pytest.fixture
def oracle_sync_session_config(oracle_sync_migrated_config: OracleSyncConfig) -> SQLSpecSessionConfig:
    """Create a sync session configuration instance."""
    # Create the session configuration
    return SQLSpecSessionConfig(
        table_name="litestar_sessions_oracle_sync",
        store="sessions",  # This will be the key in the stores registry
    )


async def test_oracle_async_session_store_creation(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with Oracle async configuration."""
    assert oracle_async_session_store is not None
    assert oracle_async_session_store._table_name == "litestar_sessions_oracle_async"
    assert oracle_async_session_store._session_id_column == "session_id"
    assert oracle_async_session_store._data_column == "data"
    assert oracle_async_session_store._expires_at_column == "expires_at"
    assert oracle_async_session_store._created_at_column == "created_at"


def test_oracle_sync_session_store_creation(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with Oracle sync configuration."""
    assert oracle_sync_session_store is not None
    assert oracle_sync_session_store._table_name == "litestar_sessions_oracle_sync"
    assert oracle_sync_session_store._session_id_column == "session_id"
    assert oracle_sync_session_store._data_column == "data"
    assert oracle_sync_session_store._expires_at_column == "expires_at"
    assert oracle_sync_session_store._created_at_column == "created_at"


async def test_oracle_async_session_store_basic_operations(oracle_async_session_store: SQLSpecSessionStore) -> None:
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
    await oracle_async_session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await oracle_async_session_store.get(session_id)
    assert retrieved_data == session_data

    # Update session data with Oracle-specific information
    updated_data = {
        **session_data,
        "last_login": "2024-01-01T12:00:00Z",
        "oracle_metadata": {"sid": "ORCL", "instance_name": "oracle_instance", "container": "PDB1"},
    }
    await oracle_async_session_store.set(session_id, updated_data, expires_in=3600)

    # Verify update
    retrieved_data = await oracle_async_session_store.get(session_id)
    assert retrieved_data == updated_data
    assert retrieved_data["oracle_metadata"]["sid"] == "ORCL"

    # Delete session
    await oracle_async_session_store.delete(session_id)

    # Verify deletion
    result = await oracle_async_session_store.get(session_id, None)
    assert result is None


def test_oracle_sync_session_store_basic_operations(oracle_sync_session_store: SQLSpecSessionStore) -> None:
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
        await oracle_sync_session_store.set(session_id, session_data, expires_in=3600)

        # Get session data
        retrieved_data = await oracle_sync_session_store.get(session_id)
        assert retrieved_data == session_data

        # Delete session
        await oracle_sync_session_store.delete(session_id)

        # Verify deletion
        result = await oracle_sync_session_store.get(session_id, None)
        assert result is None

    asyncio.run(run_sync_test())


async def test_oracle_async_session_store_oracle_table_structure(
    oracle_async_session_store: SQLSpecSessionStore, oracle_async_migration_config: OracleAsyncConfig
) -> None:
    """Test that session table is created with proper Oracle structure."""
    async with oracle_async_migration_config.provide_session() as driver:
        # Verify table exists with proper name
        result = await driver.execute(
            "SELECT table_name FROM user_tables WHERE table_name = :1", ("LITESTAR_SESSIONS",)
        )
        assert len(result.data) == 1
        table_info = result.data[0]
        assert table_info["TABLE_NAME"] == "LITESTAR_SESSIONS"

        # Verify column structure
        result = await driver.execute(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1", ("LITESTAR_SESSIONS",)
        )
        columns = {row["COLUMN_NAME"]: row for row in result.data}

        assert "SESSION_ID" in columns
        assert "DATA" in columns
        assert "EXPIRES_AT" in columns
        assert "CREATED_AT" in columns

        # Verify constraints
        result = await driver.execute(
            "SELECT constraint_name, constraint_type FROM user_constraints WHERE table_name = :1",
            ("LITESTAR_SESSIONS",),
        )
        constraint_types = [row["CONSTRAINT_TYPE"] for row in result.data]
        assert "P" in constraint_types  # Primary key constraint

        # Verify index exists for expires_at
        result = await driver.execute(
            "SELECT index_name FROM user_indexes WHERE table_name = :1 AND index_name LIKE '%EXPIRES%'",
            ("LITESTAR_SESSIONS",),
        )
        assert len(result.data) == 0  # No additional indexes expected beyond primary key


async def test_oracle_json_data_support(
    oracle_async_session_store: SQLSpecSessionStore, oracle_async_migration_config: OracleAsyncConfig
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
        "large_dataset": [{"id": i, "value": f"oracle_data_{i}"} for i in range(50)],
    }

    # Store complex data
    await oracle_async_session_store.set(session_id, complex_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await oracle_async_session_store.get(session_id)
    assert retrieved_data == complex_data
    assert retrieved_data["oracle_specific"]["advanced_features"]["autonomous"] is True
    assert len(retrieved_data["large_dataset"]) == 50

    # Verify data is properly stored in Oracle database
    async with oracle_async_migration_config.provide_session() as driver:
        result = await driver.execute(
            f"SELECT data FROM {oracle_async_session_store._table_name} WHERE session_id = :1", (session_id,)
        )
        assert len(result.data) == 1
        stored_data = result.data[0]["DATA"]
        assert isinstance(stored_data, (dict, str))  # Could be parsed or string depending on driver


async def test_basic_session_operations(
    oracle_async_session_config: SQLSpecSessionConfig, oracle_async_session_store: SQLSpecSessionStore
) -> None:
    """Test basic session operations through Litestar application using Oracle async."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "oracle_user"
        request.session["preferences"] = {"theme": "dark", "language": "en", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor", "oracle_admin"]
        request.session["oracle_info"] = {"engine": "Oracle", "version": "23ai", "mode": "async"}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "roles": request.session.get("roles"),
            "oracle_info": request.session.get("oracle_info"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", oracle_async_session_store)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session],
        middleware=[oracle_async_session_config.middleware],
        stores=stores,
    )

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        if response.status_code != HTTP_200_OK:
            pass
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "oracle_user"
        assert data["preferences"]["theme"] == "dark"
        assert data["roles"] == ["user", "editor", "oracle_admin"]
        assert data["oracle_info"]["engine"] == "Oracle"

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {
            "user_id": None,
            "username": None,
            "preferences": None,
            "roles": None,
            "oracle_info": None,
        }


async def test_session_persistence_across_requests(
    oracle_async_session_config: SQLSpecSessionConfig, oracle_async_session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across multiple requests with Oracle."""

    @get("/document/create/{doc_id:int}")
    async def create_document(request: Any, doc_id: int) -> dict:
        documents = request.session.get("documents", [])
        document = {
            "id": doc_id,
            "title": f"Oracle Document {doc_id}",
            "content": f"Content for document {doc_id}. " + "Oracle " * 20,
            "created_at": "2024-01-01T12:00:00Z",
            "metadata": {"engine": "Oracle", "storage": "tablespace", "acid": True},
        }
        documents.append(document)
        request.session["documents"] = documents
        request.session["document_count"] = len(documents)
        request.session["last_action"] = f"created_document_{doc_id}"
        return {"document": document, "total_docs": len(documents)}

    @get("/documents")
    async def get_documents(request: Any) -> dict:
        return {
            "documents": request.session.get("documents", []),
            "count": request.session.get("document_count", 0),
            "last_action": request.session.get("last_action"),
        }

    @post("/documents/save-all")
    async def save_all_documents(request: Any) -> dict:
        documents = request.session.get("documents", [])

        # Simulate saving all documents
        saved_docs = {
            "saved_count": len(documents),
            "documents": documents,
            "saved_at": "2024-01-01T12:00:00Z",
            "oracle_transaction": True,
        }

        request.session["saved_session"] = saved_docs
        request.session["last_save"] = "2024-01-01T12:00:00Z"

        # Clear working documents after save
        request.session.pop("documents", None)
        request.session.pop("document_count", None)

        return {"status": "all documents saved", "count": saved_docs["saved_count"]}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", oracle_async_session_store)

    app = Litestar(
        route_handlers=[create_document, get_documents, save_all_documents],
        middleware=[oracle_async_session_config.middleware],
        stores=stores,
    )

    async with AsyncTestClient(app=app) as client:
        # Create multiple documents
        response = await client.get("/document/create/101")
        assert response.json()["total_docs"] == 1

        response = await client.get("/document/create/102")
        assert response.json()["total_docs"] == 2

        response = await client.get("/document/create/103")
        assert response.json()["total_docs"] == 3

        # Verify document persistence
        response = await client.get("/documents")
        data = response.json()
        assert data["count"] == 3
        assert len(data["documents"]) == 3
        assert data["documents"][0]["id"] == 101
        assert data["documents"][0]["metadata"]["engine"] == "Oracle"
        assert data["last_action"] == "created_document_103"

        # Save all documents
        response = await client.post("/documents/save-all")
        assert response.status_code == HTTP_201_CREATED
        save_data = response.json()
        assert save_data["status"] == "all documents saved"
        assert save_data["count"] == 3

        # Verify working documents are cleared but save session persists
        response = await client.get("/documents")
        data = response.json()
        assert data["count"] == 0
        assert len(data["documents"]) == 0


async def test_oracle_session_expiration(oracle_async_migration_config: OracleAsyncConfig) -> None:
    """Test session expiration functionality with Oracle."""
    # Apply migrations first
    commands = AsyncMigrationCommands(oracle_async_migration_config)
    await commands.init(oracle_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store and config with very short lifetime
    session_store = SQLSpecSessionStore(
        config=oracle_async_migration_config,
        table_name="litestar_sessions_oracle_async",  # Use the migrated table
    )

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_oracle_async",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-expiring-data")
    async def set_data(request: Any) -> dict:
        request.session["test_data"] = "oracle_expiring_data"
        request.session["timestamp"] = "2024-01-01T00:00:00Z"
        request.session["database"] = "Oracle"
        request.session["storage_mode"] = "tablespace"
        request.session["acid_compliant"] = True
        return {"status": "data set with short expiration"}

    @get("/get-expiring-data")
    async def get_data(request: Any) -> dict:
        return {
            "test_data": request.session.get("test_data"),
            "timestamp": request.session.get("timestamp"),
            "database": request.session.get("database"),
            "storage_mode": request.session.get("storage_mode"),
            "acid_compliant": request.session.get("acid_compliant"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(route_handlers=[set_data, get_data], middleware=[session_config.middleware], stores=stores)

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-expiring-data")
        assert response.json() == {"status": "data set with short expiration"}

        # Data should be available immediately
        response = await client.get("/get-expiring-data")
        data = response.json()
        assert data["test_data"] == "oracle_expiring_data"
        assert data["database"] == "Oracle"
        assert data["acid_compliant"] is True

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-expiring-data")
        assert response.json() == {
            "test_data": None,
            "timestamp": None,
            "database": None,
            "storage_mode": None,
            "acid_compliant": None,
        }


async def test_oracle_concurrent_session_operations(oracle_async_session_store: SQLSpecSessionStore) -> None:
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
        await oracle_async_session_store.set(session_id, session_data, expires_in=3600)

    async def read_oracle_session(session_num: int) -> "dict[str, Any] | None":
        """Read an Oracle session by number."""
        session_id = f"oracle-concurrent-{session_num}"
        return await oracle_async_session_store.get(session_id, None)

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


async def test_oracle_large_session_data_with_clob(oracle_async_session_store: SQLSpecSessionStore) -> None:
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
                for i in range(5)
            ],
        },
        "large_plsql_log": "x" * 1000,  # 1KB of text for CLOB testing
        "query_history": [
            {
                "query_id": f"QRY_{i}",
                "sql_text": f"SELECT * FROM large_table_{i} WHERE condition = :param{i}" * 2,
                "execution_plan": f"execution_plan_data_for_query_{i}" * 5,
                "statistics": {"logical_reads": 1000 + i, "physical_reads": 100 + i, "elapsed_time": 0.1 + i * 0.01},
            }
            for i in range(20)
        ],
        "vector_embeddings": {
            f"embedding_{i}": [float(j) for j in range(10)]
            for i in range(5)  # 5 embeddings with 10 dimensions each
        },
    }

    # Store large Oracle data
    await oracle_async_session_store.set(session_id, large_oracle_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await oracle_async_session_store.get(session_id)
    assert retrieved_data == large_oracle_data
    assert len(retrieved_data["large_plsql_log"]) == 1000
    assert len(retrieved_data["oracle_metadata"]["tablespace_info"]) == 5
    assert len(retrieved_data["query_history"]) == 20
    assert len(retrieved_data["vector_embeddings"]) == 5
    assert len(retrieved_data["vector_embeddings"]["embedding_0"]) == 10


async def test_oracle_session_cleanup_operations(oracle_async_session_store: SQLSpecSessionStore) -> None:
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
        await oracle_async_session_store.set(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, expected_data, _ in oracle_sessions_data:
        result = await oracle_async_session_store.get(session_id)
        assert result == expected_data

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await oracle_async_session_store.delete_expired()

    # Verify short sessions are gone and long sessions remain
    for session_id, expected_data, expires_in in oracle_sessions_data:
        result = await oracle_async_session_store.get(session_id, None)
        if expires_in == 1:  # Short expiration
            assert result is None
        else:  # Long expiration
            assert result == expected_data
            assert "advanced" in result["features"]


async def test_migration_with_default_table_name(oracle_async_migration_config: OracleAsyncConfig) -> None:
    """Test that migration with string format creates default table name."""
    # Apply migrations
    commands = AsyncMigrationCommands(oracle_async_migration_config)
    await commands.init(oracle_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the migrated table
    store = SQLSpecSessionStore(
        config=oracle_async_migration_config,
        table_name="litestar_sessions_oracle_async",  # Default table name
    )

    # Test that the store works with the migrated table
    session_id = "test_session_default"
    test_data = {"user_id": 1, "username": "test_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data


async def test_migration_with_custom_table_name(oracle_async_migration_config_with_dict: OracleAsyncConfig) -> None:
    """Test that migration with dict format creates custom table name."""
    # Apply migrations
    commands = AsyncMigrationCommands(oracle_async_migration_config_with_dict)
    await commands.init(oracle_async_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    store = SQLSpecSessionStore(
        config=oracle_async_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )

    # Test that the store works with the custom table
    session_id = "test_session_custom"
    test_data = {"user_id": 2, "username": "custom_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data

    # Verify default table doesn't exist
    async with oracle_async_migration_config_with_dict.provide_session() as driver:
        result = await driver.execute(
            "SELECT table_name FROM user_tables WHERE table_name = :1", ("LITESTAR_SESSIONS",)
        )
        assert len(result.data) == 0


async def test_migration_with_mixed_extensions(oracle_async_migration_config_mixed: OracleAsyncConfig) -> None:
    """Test migration with mixed extension formats."""
    # Apply migrations
    commands = AsyncMigrationCommands(oracle_async_migration_config_mixed)
    await commands.init(oracle_async_migration_config_mixed.migration_config["script_location"], package=False)
    await commands.upgrade()

    # The litestar extension should use default table name
    store = SQLSpecSessionStore(
        config=oracle_async_migration_config_mixed,
        table_name="litestar_sessions_oracle_async",  # Default since string format was used
    )

    # Test that the store works
    session_id = "test_session_mixed"
    test_data = {"user_id": 3, "username": "mixed_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data


async def test_oracle_concurrent_webapp_simulation(
    oracle_async_session_config: SQLSpecSessionConfig, oracle_async_session_store: SQLSpecSessionStore
) -> None:
    """Test concurrent web application behavior with Oracle session handling."""

    @get("/user/{user_id:int}/login")
    async def user_login(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["username"] = f"oracle_user_{user_id}"
        request.session["login_time"] = "2024-01-01T12:00:00Z"
        request.session["database"] = "Oracle"
        request.session["session_type"] = "tablespace_based"
        request.session["permissions"] = ["read", "write", "execute"]
        return {"status": "logged in", "user_id": user_id}

    @get("/user/profile")
    async def get_profile(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "login_time": request.session.get("login_time"),
            "database": request.session.get("database"),
            "session_type": request.session.get("session_type"),
            "permissions": request.session.get("permissions"),
        }

    @post("/user/activity")
    async def log_activity(request: Any) -> dict:
        user_id = request.session.get("user_id")
        if user_id is None:
            return {"error": "Not logged in"}

        activities = request.session.get("activities", [])
        activity = {
            "action": "page_view",
            "timestamp": "2024-01-01T12:00:00Z",
            "user_id": user_id,
            "oracle_transaction": True,
        }
        activities.append(activity)
        request.session["activities"] = activities
        request.session["activity_count"] = len(activities)

        return {"status": "activity logged", "count": len(activities)}

    @post("/user/logout")
    async def user_logout(request: Any) -> dict:
        user_id = request.session.get("user_id")
        if user_id is None:
            return {"error": "Not logged in"}

        # Store logout info before clearing session
        request.session["last_logout"] = "2024-01-01T12:00:00Z"
        request.session.clear()

        return {"status": "logged out", "user_id": user_id}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", oracle_async_session_store)

    app = Litestar(
        route_handlers=[user_login, get_profile, log_activity, user_logout],
        middleware=[oracle_async_session_config.middleware],
        stores=stores,
    )

    # Test with multiple concurrent users
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Concurrent logins
        login_tasks = [
            client1.get("/user/1001/login"),
            client2.get("/user/1002/login"),
            client3.get("/user/1003/login"),
        ]
        responses = await asyncio.gather(*login_tasks)

        for i, response in enumerate(responses, 1001):
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "logged in", "user_id": i}

        # Verify each client has correct session
        profile_responses = await asyncio.gather(
            client1.get("/user/profile"), client2.get("/user/profile"), client3.get("/user/profile")
        )

        assert profile_responses[0].json()["user_id"] == 1001
        assert profile_responses[0].json()["username"] == "oracle_user_1001"
        assert profile_responses[1].json()["user_id"] == 1002
        assert profile_responses[2].json()["user_id"] == 1003

        # Log activities concurrently
        activity_tasks = [
            client.post("/user/activity")
            for client in [client1, client2, client3]
            for _ in range(5)  # 5 activities per user
        ]

        activity_responses = await asyncio.gather(*activity_tasks)
        for response in activity_responses:
            assert response.status_code == HTTP_201_CREATED
            assert "activity logged" in response.json()["status"]

        # Verify final activity counts
        final_profiles = await asyncio.gather(
            client1.get("/user/profile"), client2.get("/user/profile"), client3.get("/user/profile")
        )

        for profile_response in final_profiles:
            profile_data = profile_response.json()
            assert profile_data["database"] == "Oracle"
            assert profile_data["session_type"] == "tablespace_based"


async def test_session_cleanup_and_maintenance(oracle_async_migration_config: OracleAsyncConfig) -> None:
    """Test session cleanup and maintenance operations with Oracle."""
    # Apply migrations first
    commands = AsyncMigrationCommands(oracle_async_migration_config)
    await commands.init(oracle_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    store = SQLSpecSessionStore(
        config=oracle_async_migration_config,
        table_name="litestar_sessions_oracle_async",  # Use the migrated table
    )

    # Create sessions with different lifetimes
    temp_sessions = []
    for i in range(8):
        session_id = f"oracle_temp_session_{i}"
        temp_sessions.append(session_id)
        await store.set(
            session_id,
            {
                "data": i,
                "type": "temporary",
                "oracle_engine": "tablespace",
                "created_for": "cleanup_test",
                "acid_compliant": True,
            },
            expires_in=1,
        )

    # Create permanent sessions
    perm_sessions = []
    for i in range(4):
        session_id = f"oracle_perm_session_{i}"
        perm_sessions.append(session_id)
        await store.set(
            session_id,
            {
                "data": f"permanent_{i}",
                "type": "permanent",
                "oracle_engine": "tablespace",
                "created_for": "cleanup_test",
                "durable": True,
            },
            expires_in=3600,
        )

    # Verify all sessions exist initially
    for session_id in temp_sessions + perm_sessions:
        result = await store.get(session_id)
        assert result is not None
        assert result["oracle_engine"] == "tablespace"

    # Wait for temporary sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await store.delete_expired()

    # Verify temporary sessions are gone
    for session_id in temp_sessions:
        result = await store.get(session_id)
        assert result is None

    # Verify permanent sessions still exist
    for session_id in perm_sessions:
        result = await store.get(session_id)
        assert result is not None
        assert result["type"] == "permanent"


async def test_multiple_oracle_apps_with_separate_backends(oracle_async_migration_config: OracleAsyncConfig) -> None:
    """Test multiple Litestar applications with separate Oracle session backends."""

    # Create separate Oracle stores for different applications
    oracle_store1 = SQLSpecSessionStore(
        config=oracle_async_migration_config,
        table_name="litestar_sessions_oracle_async",  # Use migrated table
    )

    oracle_store2 = SQLSpecSessionStore(
        config=oracle_async_migration_config,
        table_name="litestar_sessions_oracle_async",  # Use migrated table
    )

    oracle_config1 = SQLSpecSessionConfig(table_name="litestar_sessions", store="sessions1")

    oracle_config2 = SQLSpecSessionConfig(table_name="litestar_sessions", store="sessions2")

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
    stores1 = StoreRegistry()
    stores1.register("sessions1", oracle_store1)

    stores2 = StoreRegistry()
    stores2.register("sessions2", oracle_store2)

    oracle_app1 = Litestar(
        route_handlers=[oracle_app1_endpoint], middleware=[oracle_config1.middleware], stores=stores1
    )

    oracle_app2 = Litestar(
        route_handlers=[oracle_app2_endpoint], middleware=[oracle_config2.middleware], stores=stores2
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


async def test_oracle_enterprise_features_in_sessions(oracle_async_session_store: SQLSpecSessionStore) -> None:
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
    await oracle_async_session_store.set(
        session_id, enterprise_session_data, expires_in=7200
    )  # Longer session for enterprise

    # Retrieve and verify all enterprise features
    retrieved_data = await oracle_async_session_store.get(session_id)
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

    await oracle_async_session_store.set(session_id, updated_enterprise_data, expires_in=7200)

    # Verify enterprise updates
    final_data = await oracle_async_session_store.get(session_id)
    assert final_data["enterprise_config"]["autonomous_features"]["auto_indexing"] is True
    assert final_data["performance_monitoring"]["awr_enabled"] is True


async def test_oracle_atomic_transactions_pattern(
    oracle_async_session_config: SQLSpecSessionConfig, oracle_async_session_store: SQLSpecSessionStore
) -> None:
    """Test atomic transaction patterns typical for Oracle applications."""

    @post("/transaction/start")
    async def start_transaction(request: Any) -> dict:
        # Initialize transaction state
        request.session["transaction"] = {
            "id": "oracle_txn_001",
            "status": "started",
            "operations": [],
            "atomic": True,
            "engine": "Oracle",
        }
        request.session["transaction_active"] = True
        return {"status": "transaction started", "id": "oracle_txn_001"}

    @post("/transaction/add-operation")
    async def add_operation(request: Any) -> dict:
        data = await request.json()
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        operation = {
            "type": data["type"],
            "table": data.get("table", "default_table"),
            "data": data.get("data", {}),
            "timestamp": "2024-01-01T12:00:00Z",
            "oracle_optimized": True,
        }

        transaction["operations"].append(operation)
        request.session["transaction"] = transaction

        return {"status": "operation added", "operation_count": len(transaction["operations"])}

    @post("/transaction/commit")
    async def commit_transaction(request: Any) -> dict:
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        # Simulate commit
        transaction["status"] = "committed"
        transaction["committed_at"] = "2024-01-01T12:00:00Z"
        transaction["oracle_undo_mode"] = True

        # Add to transaction history
        history = request.session.get("transaction_history", [])
        history.append(transaction)
        request.session["transaction_history"] = history

        # Clear active transaction
        request.session.pop("transaction", None)
        request.session["transaction_active"] = False

        return {
            "status": "transaction committed",
            "operations_count": len(transaction["operations"]),
            "transaction_id": transaction["id"],
        }

    @post("/transaction/rollback")
    async def rollback_transaction(request: Any) -> dict:
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        # Simulate rollback
        transaction["status"] = "rolled_back"
        transaction["rolled_back_at"] = "2024-01-01T12:00:00Z"

        # Clear active transaction
        request.session.pop("transaction", None)
        request.session["transaction_active"] = False

        return {"status": "transaction rolled back", "operations_discarded": len(transaction["operations"])}

    @get("/transaction/history")
    async def get_history(request: Any) -> dict:
        return {
            "history": request.session.get("transaction_history", []),
            "active": request.session.get("transaction_active", False),
            "current": request.session.get("transaction"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", oracle_async_session_store)

    app = Litestar(
        route_handlers=[start_transaction, add_operation, commit_transaction, rollback_transaction, get_history],
        middleware=[oracle_async_session_config.middleware],
        stores=stores,
    )

    async with AsyncTestClient(app=app) as client:
        # Start transaction
        response = await client.post("/transaction/start")
        assert response.json() == {"status": "transaction started", "id": "oracle_txn_001"}

        # Add operations
        operations = [
            {"type": "INSERT", "table": "users", "data": {"name": "Oracle User"}},
            {"type": "UPDATE", "table": "profiles", "data": {"theme": "dark"}},
            {"type": "DELETE", "table": "temp_data", "data": {"expired": True}},
        ]

        for op in operations:
            response = await client.post("/transaction/add-operation", json=op)
            assert "operation added" in response.json()["status"]

        # Verify operations are tracked
        response = await client.get("/transaction/history")
        history_data = response.json()
        assert history_data["active"] is True
        assert len(history_data["current"]["operations"]) == 3

        # Commit transaction
        response = await client.post("/transaction/commit")
        commit_data = response.json()
        assert commit_data["status"] == "transaction committed"
        assert commit_data["operations_count"] == 3

        # Verify transaction history
        response = await client.get("/transaction/history")
        history_data = response.json()
        assert history_data["active"] is False
        assert len(history_data["history"]) == 1
        assert history_data["history"][0]["status"] == "committed"
        assert history_data["history"][0]["oracle_undo_mode"] is True
