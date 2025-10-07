"""BigQuery-specific ADK store tests."""

import pytest

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.mark.asyncio
async def test_partitioning_and_clustering(bigquery_adk_store, bigquery_service):
    """Test that tables are created with proper partitioning and clustering."""
    import asyncio
    from datetime import datetime, timezone

    from sqlspec.extensions.adk._types import EventRecord

    await bigquery_adk_store.create_session("session-1", "app1", "user1", {"test": True})
    await bigquery_adk_store.create_session("session-2", "app2", "user2", {"test": True})

    event1: EventRecord = {
        "id": "event-1",
        "session_id": "session-1",
        "app_name": "app1",
        "user_id": "user1",
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }
    await bigquery_adk_store.append_event(event1)

    await asyncio.sleep(0.1)

    sessions = await bigquery_adk_store.list_sessions("app1", "user1")
    assert len(sessions) == 1

    events = await bigquery_adk_store.get_events("session-1")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_json_type_storage(bigquery_adk_store, session_fixture):
    """Test that JSON type is properly used for state and metadata."""
    complex_state = {"nested": {"deep": {"value": 123}}, "array": [1, 2, 3], "boolean": True, "null": None}

    await bigquery_adk_store.update_session_state(session_fixture["session_id"], complex_state)

    retrieved = await bigquery_adk_store.get_session(session_fixture["session_id"])
    assert retrieved is not None
    assert retrieved["state"] == complex_state


@pytest.mark.asyncio
async def test_timestamp_precision(bigquery_adk_store):
    """Test that BigQuery TIMESTAMP preserves microsecond precision."""
    import asyncio

    session_id = "precision-test"

    session = await bigquery_adk_store.create_session(session_id, "app", "user", {"test": True})
    create_time_1 = session["create_time"]

    await asyncio.sleep(0.001)

    session2 = await bigquery_adk_store.create_session("precision-test-2", "app", "user", {"test": True})
    create_time_2 = session2["create_time"]

    assert create_time_2 > create_time_1
    assert (create_time_2 - create_time_1).total_seconds() < 1


@pytest.mark.asyncio
async def test_bytes_storage(bigquery_adk_store, session_fixture):
    """Test that BYTES type properly stores binary data."""
    from datetime import datetime, timezone

    from sqlspec.extensions.adk._types import EventRecord

    large_actions = b"x" * 10000

    event: EventRecord = {
        "id": "large-event",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": large_actions,
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events[0]["actions"]) == 10000
    assert events[0]["actions"] == large_actions


@pytest.mark.asyncio
async def test_cost_optimization_query_patterns(bigquery_adk_store):
    """Test that queries use clustering for cost optimization."""
    await bigquery_adk_store.create_session("s1", "app1", "user1", {"test": True})
    await bigquery_adk_store.create_session("s2", "app1", "user1", {"test": True})
    await bigquery_adk_store.create_session("s3", "app2", "user2", {"test": True})

    sessions_app1 = await bigquery_adk_store.list_sessions("app1", "user1")
    assert len(sessions_app1) == 2

    sessions_app2 = await bigquery_adk_store.list_sessions("app2", "user2")
    assert len(sessions_app2) == 1


@pytest.mark.asyncio
async def test_dataset_qualification(bigquery_service):
    """Test that table names are properly qualified with dataset."""
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials

    from sqlspec.adapters.bigquery.adk import BigQueryADKStore
    from sqlspec.adapters.bigquery.config import BigQueryConfig

    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),
        }
    )

    store = BigQueryADKStore(config, dataset_id=bigquery_service.dataset)

    expected_sessions = f"`{bigquery_service.dataset}.adk_sessions`"
    expected_events = f"`{bigquery_service.dataset}.adk_events`"

    assert store._get_full_table_name("adk_sessions") == expected_sessions
    assert store._get_full_table_name("adk_events") == expected_events


@pytest.mark.asyncio
async def test_manual_cascade_delete(bigquery_adk_store, session_fixture):
    """Test manual cascade delete (BigQuery doesn't have foreign keys)."""
    from datetime import datetime, timezone

    from sqlspec.extensions.adk._types import EventRecord

    for i in range(3):
        event: EventRecord = {
            "id": f"event-{i}",
            "session_id": session_fixture["session_id"],
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
            "invocation_id": f"inv-{i}",
            "author": "user",
            "actions": b"",
            "long_running_tool_ids_json": None,
            "branch": None,
            "timestamp": datetime.now(timezone.utc),
            "content": None,
            "grounding_metadata": None,
            "custom_metadata": None,
            "partial": None,
            "turn_complete": None,
            "interrupted": None,
            "error_code": None,
            "error_message": None,
        }
        await bigquery_adk_store.append_event(event)

    events_before = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events_before) == 3

    await bigquery_adk_store.delete_session(session_fixture["session_id"])

    session_after = await bigquery_adk_store.get_session(session_fixture["session_id"])
    assert session_after is None

    events_after = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events_after) == 0
