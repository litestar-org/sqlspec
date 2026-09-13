"""SQL Server key-range locking for concurrent ADK memory deduplication."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Barrier
from uuid import uuid4

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKMemoryStore
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymssql.adk import PymssqlADKMemoryStore
from sqlspec.extensions.adk import StoredMemory
from tests.integration.fixtures.mssql import _mssql_connection_config, _mssql_python_connection_config

pytestmark = [pytest.mark.mssql, pytest.mark.xdist_group("mssql")]


@pytest.mark.parametrize("adapter", ["mssql_python", "pymssql"])
@pytest.mark.parametrize("autocommit", [False, True])
def test_concurrent_memory_inserts_deduplicate_event_ids(
    mssql_service: MSSQLService, adapter: str, autocommit: bool
) -> None:
    table = f"memory_dedup_{uuid4().hex[:8]}"
    settings = {"memory_table": table, "owner_id_column": "owner_id VARCHAR(64)"}
    config: MssqlPythonConfig | PymssqlConfig
    store: MssqlPythonADKMemoryStore | PymssqlADKMemoryStore
    if adapter == "mssql_python":
        config = MssqlPythonConfig(
            connection_config=_mssql_python_connection_config(mssql_service, autocommit=autocommit),
            extension_config={"adk": settings},
        )
        store = MssqlPythonADKMemoryStore(config)
    else:
        config = PymssqlConfig(
            connection_config={**_mssql_connection_config(mssql_service), "autocommit": autocommit},
            extension_config={"adk": settings},
        )
        store = PymssqlADKMemoryStore(config)

    barrier = Barrier(2)
    event_id = uuid4().hex

    def insert() -> int:
        entry = StoredMemory(
            id=uuid4().hex,
            session_id="session",
            app_name="app",
            user_id="user",
            scope="user",
            event_id=event_id,
            author="user",
            timestamp=datetime(2026, 1, 1),
            content_json={"text": "deduplicated"},
            content_text="deduplicated",
            metadata_json=None,
            inserted_at=datetime(2026, 1, 1),
            embedding=None,
        )
        barrier.wait(timeout=10)
        return store.insert_memory_entries([entry], owner_id="owner")

    try:
        store.create_tables()
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(insert) for _ in range(2)]
            assert sorted(future.result(timeout=30) for future in futures) == [0, 1]
        entries = store.search_entries("deduplicated", "app", "user")
        assert len(entries) == 1
        assert entries[0]["event_id"] == event_id
        with config.provide_session() as driver:
            assert driver.select_value(f"SELECT owner_id FROM [{table}]") == "owner"
    finally:
        try:
            store.drop_tables()
        finally:
            config.close_pool()
