"""Unit tests for Spanner session-control behavior."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from google.cloud.spanner_v1 import Transaction
from google.cloud.spanner_v1.keyset import KeySet
from google.cloud.spanner_v1.streamed import StreamedResultSet

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import ImproperConfigurationError


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


def test_provide_session_injects_database_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    sentinel_database = object()
    config.get_database = lambda: sentinel_database  # type: ignore[assignment]
    monkeypatch.setattr("sqlspec.adapters.spanner.config.SpannerSessionContext", _SessionContext)

    context = config.provide_session()

    assert isinstance(context, _SessionContext)
    assert captured["driver_features"] is not config.driver_features
    assert captured["driver_features"]["database_provider"] is config.get_database
    assert "database_provider" not in config.driver_features


def test_require_database_raises_without_provider() -> None:
    driver = SpannerSyncDriver(MagicMock())

    with pytest.raises(ImproperConfigurationError, match="provide_session"):
        driver._require_database()  # type: ignore[attr-defined]


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


def test_execute_with_options_overrides_feature_defaults() -> None:
    feature_request_options = {"priority": 0, "request_tag": "feature"}
    override_request_options = {"priority": 1, "request_tag": "override"}
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection), driver_features={"request_options": feature_request_options})

    result = driver.execute_with_options("SELECT id FROM users", request_options=override_request_options)

    connection.execute_sql.assert_called_once_with(
        "SELECT id FROM users", params=None, param_types={}, request_options=override_request_options
    )
    assert result.get_data()[0]["id"] == 1
    assert driver._pending_execute_options is None  # type: ignore[attr-defined]


def test_execute_with_options_directed_read_only_on_select() -> None:
    connection = _mock_transaction()
    result_set = _mock_result_set()
    connection.execute_sql.return_value = result_set
    driver = SpannerSyncDriver(cast(Any, connection))

    directed_read_options = SimpleNamespace(tag="directed")
    driver.execute_with_options("SELECT id FROM users", directed_read_options=directed_read_options)

    assert connection.execute_sql.call_args.kwargs["directed_read_options"] is directed_read_options


def test_execute_with_options_directed_read_not_forwarded_to_dml() -> None:
    connection = _mock_transaction()
    connection.execute_update.return_value = 10
    driver = SpannerSyncDriver(cast(Any, connection))

    directed_read_options = SimpleNamespace(tag="directed")
    result = driver.execute_with_options("UPDATE users SET name = 'Bob'", directed_read_options=directed_read_options)

    assert result.rows_affected == 10
    assert "directed_read_options" not in connection.execute_update.call_args.kwargs


def test_execute_with_options_clears_stash_on_error() -> None:
    connection = _mock_transaction()
    connection.execute_sql.side_effect = RuntimeError("boom")
    driver = SpannerSyncDriver(cast(Any, connection))

    with pytest.raises(RuntimeError, match="boom"):
        driver.execute_with_options("SELECT id FROM users")

    assert driver._pending_execute_options is None  # type: ignore[attr-defined]


def test_execute_partitioned_dml_raises_without_database_provider() -> None:
    driver = SpannerSyncDriver(MagicMock())

    with pytest.raises(ImproperConfigurationError, match="provide_session"):
        driver.execute_partitioned_dml("UPDATE users SET name = 'Bob'")


def test_execute_partitioned_dml_forwards_to_database() -> None:
    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    database = MagicMock()
    database.execute_partitioned_dml.return_value = 7
    driver = SpannerSyncDriver(
        MagicMock(), driver_features={"request_options": request_options, "database_provider": lambda: database}
    )

    row_count = driver.execute_partitioned_dml(
        "UPDATE users SET name = 'Bob' WHERE TRUE", request_options=request_options
    )

    assert row_count == 7
    database.execute_partitioned_dml.assert_called_once_with(
        "UPDATE users SET name = 'Bob' WHERE TRUE",
        params=None,
        param_types={},
        request_options=request_options,
        exclude_txn_from_change_streams=False,
    )


def test_apply_mutations_requires_columns_for_rows() -> None:
    driver = SpannerSyncDriver(MagicMock(), driver_features={"database_provider": lambda: MagicMock()})

    with pytest.raises(ImproperConfigurationError, match="columns"):
        driver.apply_mutations("users", insert=[(1, "alice")])


def test_apply_mutations_routes_each_group() -> None:
    batch = MagicMock()
    batch.__enter__.return_value = batch
    batch.__exit__.return_value = False
    database = MagicMock()
    database.batch.return_value = batch
    driver = SpannerSyncDriver(MagicMock(), driver_features={"database_provider": lambda: database})

    driver.apply_mutations(
        "users",
        columns=("id", "name"),
        insert=[(1, "alice")],
        update=[(2, "bob")],
        insert_or_update=[(3, "carol")],
        replace=[(4, "dave")],
        delete_keys=[(5,)],
        request_options={"priority": 1},
        max_commit_delay=3.0,
    )

    database.batch.assert_called_once_with(request_options={"priority": 1}, max_commit_delay=3.0)
    batch.insert.assert_called_once_with("users", ("id", "name"), [(1, "alice")])
    batch.update.assert_called_once_with("users", ("id", "name"), [(2, "bob")])
    batch.insert_or_update.assert_called_once_with("users", ("id", "name"), [(3, "carol")])
    batch.replace.assert_called_once_with("users", ("id", "name"), [(4, "dave")])
    batch.delete.assert_called_once_with("users", KeySet(keys=[(5,)]))  # type: ignore[no-untyped-call]


def test_apply_mutations_delete_all_wins_over_delete_keys() -> None:
    batch = MagicMock()
    batch.__enter__.return_value = batch
    batch.__exit__.return_value = False
    database = MagicMock()
    database.batch.return_value = batch
    driver = SpannerSyncDriver(MagicMock(), driver_features={"database_provider": lambda: database})

    driver.apply_mutations("users", delete_keys=[(5,)], delete_all=True)

    batch.delete.assert_called_once_with("users", KeySet(all_=True))  # type: ignore[no-untyped-call]


def test_provide_batch_snapshot_closes_on_normal_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = MagicMock()
    database = MagicMock()
    database.batch_snapshot.return_value = snapshot
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: database  # type: ignore[assignment]

    with config.provide_batch_snapshot() as yielded:
        assert yielded is snapshot

    snapshot.close.assert_called_once()


def test_provide_batch_snapshot_closes_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = MagicMock()
    database = MagicMock()
    database.batch_snapshot.return_value = snapshot
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: database  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="boom"):
        with config.provide_batch_snapshot() as yielded:
            assert yielded is snapshot
            raise RuntimeError("boom")

    snapshot.close.assert_called_once()
