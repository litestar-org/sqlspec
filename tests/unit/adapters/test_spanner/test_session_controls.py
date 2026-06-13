"""Unit tests for Spanner session-control behavior."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from google.cloud.spanner_v1 import Transaction
from google.cloud.spanner_v1.streamed import StreamedResultSet

import sqlspec.adapters.spanner.config as spanner_config
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver


def _mock_result_set() -> MagicMock:
    result = MagicMock(spec=StreamedResultSet)
    field = MagicMock()
    field.name = "id"
    result.metadata.row_type.fields = [field]
    result.__iter__.return_value = iter([(1,)])
    return result


def _mock_transaction() -> MagicMock:
    transaction = MagicMock(spec=Transaction)
    transaction.execute_sql = MagicMock()
    transaction.execute_update = MagicMock()
    transaction.batch_update = MagicMock()
    return transaction


def test_provide_session_uses_config_driver_features(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSessionContext", _SessionContext)

    context = config.provide_session()

    assert isinstance(context, _SessionContext)
    assert captured["driver_features"] is config.driver_features
    assert captured["driver_features"]["request_options"] is request_options
    assert "database_provider" not in captured["driver_features"]


def test_provide_session_accepts_spanner_execution_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    config_request_options = {"request_tag": "config"}
    session_request_options = {"request_tag": "session"}
    directed_read_options = cast(Any, SimpleNamespace(tag="directed"))
    retry = cast(Any, object())
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": config_request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSessionContext", _SessionContext)

    config.provide_session(
        request_options=session_request_options, directed_read_options=directed_read_options, retry=retry, timeout=12.0
    )

    assert captured["driver_features"] is not config.driver_features
    assert captured["driver_features"]["request_options"] is session_request_options
    assert captured["driver_features"]["directed_read_options"] is directed_read_options
    assert captured["driver_features"]["retry"] is retry
    assert captured["driver_features"]["timeout"] == 12.0
    assert config.driver_features["request_options"] is config_request_options
    assert "database_provider" not in captured["driver_features"]


def test_no_extra_public_database_operation_surface() -> None:
    driver = SpannerSyncDriver(MagicMock())
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})

    assert not hasattr(driver, "execute_with_options")
    assert not hasattr(driver, "execute_partitioned_dml")
    assert not hasattr(driver, "apply_mutations")
    assert not hasattr(config, "provide_batch_snapshot")


def test_driver_feature_request_options_forwarded_to_select() -> None:
    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": request_options})

    statement = driver.prepare_statement("SELECT id FROM users", statement_config=driver.statement_config)
    result = driver.dispatch_execute(connection, statement)  # type: ignore[protected-access]

    assert result.is_select_result
    connection.execute_sql.assert_called_once_with(
        "SELECT id FROM users", params=None, param_types={}, request_options=request_options
    )
    assert result.selected_data is not None
    assert result.selected_data[0] == (1,)


def test_driver_feature_directed_read_options_forwarded_to_select() -> None:
    directed_read_options = SimpleNamespace(tag="directed")
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"directed_read_options": directed_read_options})

    statement = driver.prepare_statement("SELECT id FROM users", statement_config=driver.statement_config)
    result = driver.dispatch_execute(connection, statement)  # type: ignore[protected-access]

    assert result.is_select_result
    assert connection.execute_sql.call_args.kwargs["directed_read_options"] is directed_read_options


def test_driver_feature_request_options_forwarded_to_dml() -> None:
    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    connection = _mock_transaction()
    connection.execute_update.return_value = 10
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": request_options})

    statement = driver.prepare_statement("UPDATE users SET name = 'Bob'", statement_config=driver.statement_config)
    result = driver.dispatch_execute(connection, statement)  # type: ignore[protected-access]

    assert result.rowcount_override == 10
    connection.execute_update.assert_called_once_with(
        "UPDATE users SET name = 'Bob'", params=None, param_types={}, request_options=request_options
    )


def test_driver_feature_request_options_forwarded_to_batch_update() -> None:
    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    connection = _mock_transaction()
    connection.batch_update.return_value = (None, [1, 1])
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": request_options})

    statement = driver.prepare_statement(
        "UPDATE users SET name = @name WHERE id = @id",
        parameters=([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],),
        statement_config=driver.statement_config,
    )
    result = driver.dispatch_execute_many(connection, statement)  # type: ignore[protected-access]

    assert result.rowcount_override == 2
    connection.batch_update.assert_called_once()
    assert connection.batch_update.call_args.kwargs["request_options"] == request_options


def test_execute_overrides_feature_defaults() -> None:
    feature_request_options = {"priority": 0, "request_tag": "feature"}
    override_request_options = {"priority": 1, "request_tag": "override"}
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": feature_request_options})

    result = driver.execute("SELECT id FROM users", request_options=override_request_options)

    connection.execute_sql.assert_called_once_with(
        "SELECT id FROM users", params=None, param_types={}, request_options=override_request_options
    )
    assert result.get_data()[0]["id"] == 1
    assert driver._pending_execute_options is None  # type: ignore[attr-defined]


def test_execute_directed_read_only_on_select() -> None:
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection))

    directed_read_options = SimpleNamespace(tag="directed")
    driver.execute("SELECT id FROM users", directed_read_options=directed_read_options)

    assert connection.execute_sql.call_args.kwargs["directed_read_options"] is directed_read_options


def test_execute_directed_read_not_forwarded_to_dml() -> None:
    connection = _mock_transaction()
    connection.execute_update.return_value = 10
    driver = SpannerSyncDriver(cast(Any, connection))

    directed_read_options = SimpleNamespace(tag="directed")
    result = driver.execute("UPDATE users SET name = 'Bob'", directed_read_options=directed_read_options)

    assert result.rows_affected == 10
    assert "directed_read_options" not in connection.execute_update.call_args.kwargs


def test_execute_many_overrides_feature_defaults() -> None:
    feature_request_options = {"priority": 0, "request_tag": "feature"}
    override_request_options = {"priority": 1, "request_tag": "override"}
    connection = _mock_transaction()
    connection.batch_update.return_value = (None, [1])
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": feature_request_options})

    result = driver.execute_many(
        "UPDATE users SET name = @name WHERE id = @id",
        [{"id": 1, "name": "alice"}],
        request_options=override_request_options,
    )

    assert result.rows_affected == 1
    assert connection.batch_update.call_args.kwargs["request_options"] is override_request_options
    assert driver._pending_execute_options is None  # type: ignore[attr-defined]


def test_execute_script_overrides_feature_defaults() -> None:
    feature_request_options = {"priority": 0, "request_tag": "feature"}
    override_request_options = {"priority": 1, "request_tag": "override"}
    connection = _mock_transaction()
    connection.execute_sql.return_value = _mock_result_set()
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": feature_request_options})

    driver.execute_script("SELECT id FROM users", request_options=override_request_options)

    connection.execute_sql.assert_called_once()
    assert connection.execute_sql.call_args.kwargs["request_options"] is override_request_options
    assert driver._pending_execute_options is None  # type: ignore[attr-defined]


def test_execute_clears_stash_on_error() -> None:
    connection = _mock_transaction()
    connection.execute_sql.side_effect = RuntimeError("boom")
    driver = SpannerSyncDriver(cast(Any, connection))

    with pytest.raises(RuntimeError, match="boom"):
        driver.execute("SELECT id FROM users", request_options={"request_tag": "x"})

    assert driver._pending_execute_options is None  # type: ignore[attr-defined]
