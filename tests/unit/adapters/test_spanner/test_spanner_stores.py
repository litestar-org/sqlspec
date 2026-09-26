"""Unit tests for ADK and Litestar store session routing."""

from typing import Any
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.adk import SpannerSyncADKStore
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.spanner.litestar import SpannerSyncStore


def _context_manager_yielding(value: Any) -> Any:
    class _Ctx:
        def __enter__(self) -> Any:
            return value

        def __exit__(self, *_: Any) -> None:
            pass

    return _Ctx()


def test_adk_store_run_write_routes_through_provide_session() -> None:
    """Verify that SpannerSyncADKStore._run_write executes via config.provide_session."""
    config = MagicMock(spec=SpannerSyncConfig)
    executed_statements: list[tuple[str, Any]] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_driver.execute.side_effect = lambda sql, params=None, param_types=None, **kw: executed_statements.append((
        sql,
        params,
    ))
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncADKStore(config=config)
    statements = [
        ("INSERT INTO t (id) VALUES (@id)", {"id": "1"}, {"id": MagicMock()}),
        ("INSERT INTO t (id) VALUES (@id)", {"id": "2"}, {"id": MagicMock()}),
    ]
    store._run_write(statements)

    config.provide_session.assert_called_once()
    assert len(executed_statements) == 2


def test_litestar_store_writes_route_through_provide_session() -> None:
    """Verify that SpannerSyncStore write operations execute via config.provide_session."""
    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"litestar": {"session_table": "sessions"}}
    executed_sqls: list[str] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_result.rows_affected = 1

    def mock_execute(sql: Any, *a: Any, **kw: Any) -> Any:
        executed_sqls.append(str(sql))
        return mock_result

    mock_driver.execute.side_effect = mock_execute
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncStore(config=config)

    store._set("session_1", b"payload", expires_in=3600)
    assert config.provide_session.call_count == 1

    store._delete("session_1")
    assert config.provide_session.call_count == 2

    store._delete_all()
    assert config.provide_session.call_count == 3

    expired_count = store._delete_expired()
    assert config.provide_session.call_count == 4
    assert expired_count == 1


def test_litestar_store_single_base64_roundtrip() -> None:
    """Verify SpannerSyncStore passes raw bytes to driver.execute and decodes wire bytes once on _get."""
    from sqlspec.adapters.spanner.type_converter import bytes_to_spanner
    from sqlspec.core import TypedParameter

    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"litestar": {"session_table": "sessions"}}
    captured_params: list[dict[str, Any]] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_result = MagicMock()
    mock_result.rows_affected = 1

    def mock_execute(_sql: Any, params: Any = None, *_a: Any, **_kw: Any) -> Any:
        if isinstance(params, dict):
            captured_params.append(params)
        return mock_result

    mock_driver.execute.side_effect = mock_execute
    mock_driver.select_one_or_none.return_value = {"data": bytes_to_spanner(b"raw-payload"), "expires_at": None}
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncStore(config=config)
    store._set("session_1", b"raw-payload", expires_in=None)

    assert len(captured_params) == 1
    assert captured_params[0]["data"] == b"raw-payload"
    assert isinstance(captured_params[0]["expires_at"], TypedParameter)
    assert captured_params[0]["expires_at"].value is None

    fetched = store._get("session_1")
    assert fetched == b"raw-payload"


def test_adk_memory_store_write_and_decode_json() -> None:
    """Verify SpannerSyncADKMemoryStore prepares JSON/null write params and unwraps JsonObject."""
    from sqlspec.adapters.spanner._typing import spanner_param_types as param_types
    from sqlspec.adapters.spanner.adk import SpannerSyncADKMemoryStore
    from sqlspec.adapters.spanner.type_converter import spanner_json
    from sqlspec.core import TypedParameter

    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"adk": {"enable_memory": True}}
    captured_params: list[dict[str, Any]] = []

    mock_driver = MagicMock(spec=SpannerSyncDriver)
    mock_result = MagicMock()
    mock_result.rows_affected = 5

    def mock_execute(_sql: Any, params: Any = None, *_a: Any, **_kw: Any) -> Any:
        if isinstance(params, dict):
            captured_params.append(params)
        return mock_result

    mock_driver.execute.side_effect = mock_execute
    config.provide_session.side_effect = lambda *a, **kw: _context_manager_yielding(mock_driver)

    store = SpannerSyncADKMemoryStore(config=config)
    store._run_write([
        (
            "INSERT INTO adk_memory VALUES (@content_json, @metadata_json, @owner_id)",
            {"content_json": '{"text":"hi"}', "metadata_json": None, "owner_id": None},
            {"content_json": param_types.JSON, "metadata_json": param_types.JSON, "owner_id": param_types.STRING},
        )
    ])

    assert len(captured_params) == 1
    assert captured_params[0]["content_json"] == {"text": "hi"}
    assert isinstance(captured_params[0]["metadata_json"], TypedParameter)
    assert captured_params[0]["metadata_json"].original_type is dict
    assert isinstance(captured_params[0]["owner_id"], TypedParameter)
    assert captured_params[0]["owner_id"].original_type is str

    deleted = store._execute_update(
        "DELETE FROM adk_memory WHERE session_id = @session_id",
        {"session_id": "s1"},
        {"session_id": param_types.STRING},
    )
    assert deleted == 5

    decoded = store._decode_json(spanner_json({"k": "v"}))
    assert decoded == {"k": "v"}
