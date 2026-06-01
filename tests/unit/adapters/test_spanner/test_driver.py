import inspect
from unittest.mock import MagicMock, Mock

import pyarrow as pa
import pytest
from google.cloud.spanner_v1 import Transaction
from google.cloud.spanner_v1.streamed import StreamedResultSet

from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import SQLConversionError

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": ["fixed"],
}


@pytest.fixture
def mock_connection() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_transaction() -> MagicMock:
    # Create a MagicMock that specs Transaction but ensure execute_update is present
    m = MagicMock(spec=Transaction)
    m.execute_update = MagicMock()
    m.batch_update = MagicMock()
    return m


def test_driver_initialization(mock_connection: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_connection)
    assert driver.connection == mock_connection
    assert driver.dialect == "spanner"


def test_execute_statement_select(mock_connection: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_connection)

    # Mock result set
    mock_result = MagicMock(spec=StreamedResultSet)

    # Create mock fields
    f1 = Mock()
    f1.name = "id"
    f2 = Mock()
    f2.name = "name"

    mock_result.metadata.row_type.fields = [f1, f2]

    mock_result.__iter__.return_value = iter([(1, "Alice"), (2, "Bob")])
    mock_connection.execute_sql.return_value = mock_result

    statement = driver.prepare_statement("SELECT * FROM users", statement_config=driver.statement_config)
    result = driver.dispatch_execute(mock_connection, statement)  # type: ignore[protected-access]

    assert result.is_select_result
    assert result.selected_data is not None
    assert len(result.selected_data) == 2
    assert result.selected_data[0] == (1, "Alice")
    assert result.selected_data[1] == (2, "Bob")


def test_execute_statement_dml_in_transaction(mock_transaction: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_transaction)
    mock_transaction.execute_update.return_value = 10

    statement = driver.prepare_statement("UPDATE users SET name = 'Bob'", statement_config=driver.statement_config)
    result = driver.dispatch_execute(mock_transaction, statement)  # type: ignore[protected-access]

    assert result.rowcount_override == 10
    mock_transaction.execute_update.assert_called_once()


def test_insert_requires_transaction_or_update_method(mock_connection: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_connection)
    # If connection doesn't have execute_update, DML should fail (Snapshot)
    if hasattr(mock_connection, "execute_update"):
        del mock_connection.execute_update

    statement = driver.prepare_statement(
        "INSERT INTO users (name) VALUES ('Alice')", statement_config=driver.statement_config
    )

    with pytest.raises(SQLConversionError, match="Cannot execute DML"):
        driver.dispatch_execute(mock_connection, statement)  # type: ignore[protected-access]


def test_execute_many_caches_inferred_param_types(mock_transaction: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    driver = SpannerSyncDriver(mock_transaction)

    infer_call_count = 0

    def _mock_infer_param_types(params: dict[str, object] | list[object] | tuple[object, ...] | None) -> dict[str, str]:
        nonlocal infer_call_count
        infer_call_count += 1
        if not isinstance(params, dict):
            return {}
        return dict.fromkeys(params, "TYPE")

    monkeypatch.setattr("sqlspec.adapters.spanner.driver.infer_param_types", _mock_infer_param_types)

    statement = driver.prepare_statement(
        "UPDATE users SET name = @name WHERE id = @id",
        parameters=([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],),
        statement_config=driver.statement_config,
    )
    mock_transaction.batch_update.return_value = (None, [1, 1])

    result = driver.dispatch_execute_many(mock_transaction, statement)  # type: ignore[protected-access]

    assert result.rowcount_override == 2
    assert infer_call_count == 1


def test_load_from_arrow_caches_inferred_param_types(
    mock_transaction: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = SpannerSyncDriver(mock_transaction, driver_features={"storage_capabilities": CAPABILITIES})

    infer_call_count = 0

    def _mock_infer_param_types(params: dict[str, object] | list[object] | tuple[object, ...] | None) -> dict[str, str]:
        nonlocal infer_call_count
        infer_call_count += 1
        if not isinstance(params, dict):
            return {}
        return dict.fromkeys(params, "TYPE")

    monkeypatch.setattr("sqlspec.adapters.spanner.driver.Transaction", type(mock_transaction))
    monkeypatch.setattr("sqlspec.adapters.spanner.driver.infer_param_types", _mock_infer_param_types)
    arrow_table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})

    result = driver.load_from_arrow("users", arrow_table)

    assert result.telemetry["rows_processed"] == 2
    assert infer_call_count == 1
    mock_transaction.batch_update.assert_called_once()


def test_dispatch_execute_script_cte_select_detected_as_select(mock_transaction: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_transaction)
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    mock_transaction.execute_sql = MagicMock(return_value=mock_result)
    mock_transaction.execute_sql.return_value = mock_result
    mock_transaction.execute_update.side_effect = AssertionError("execute_update called for SELECT CTE")

    statement = driver.prepare_statement(
        "WITH cte AS (SELECT id FROM users) SELECT * FROM cte", statement_config=driver.statement_config
    )
    driver.dispatch_execute_script(mock_transaction, statement)

    mock_transaction.execute_sql.assert_called_once()
    mock_transaction.execute_update.assert_not_called()


def test_dispatch_execute_script_plain_select_detected_as_select(mock_transaction: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_transaction)
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    mock_transaction.execute_sql = MagicMock(return_value=mock_result)
    mock_transaction.execute_sql.return_value = mock_result
    mock_transaction.execute_update.side_effect = AssertionError("execute_update should not be called for SELECT")

    statement = driver.prepare_statement("SELECT 1", statement_config=driver.statement_config)
    driver.dispatch_execute_script(mock_transaction, statement)

    mock_transaction.execute_sql.assert_called_once()
    mock_transaction.execute_update.assert_not_called()


def test_dispatch_execute_script_insert_detected_as_non_select(mock_transaction: MagicMock) -> None:
    driver = SpannerSyncDriver(mock_transaction)
    mock_transaction.execute_sql = MagicMock()
    mock_transaction.execute_update.return_value = 1

    statement = driver.prepare_statement(
        "INSERT INTO users (id, name) VALUES (1, 'Alice')", statement_config=driver.statement_config
    )
    driver.dispatch_execute_script(mock_transaction, statement)

    mock_transaction.execute_update.assert_called_once()
    mock_transaction.execute_sql.assert_not_called()


def test_dispatch_execute_script_reuses_coerced_params_and_inferred_types(
    mock_transaction: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = SpannerSyncDriver(mock_transaction)
    script = """
    UPDATE users SET name = @name WHERE id = @id;
    UPDATE users SET name = @name WHERE id = @id;
    UPDATE users SET name = @name WHERE id = @id;
    """
    params = {"id": 1, "name": "Alice"}
    coerce_calls = 0
    infer_calls = 0

    monkeypatch.setattr(SpannerSyncDriver, "_get_compiled_sql", lambda _self, _statement, _config: (script, params))

    def coerce(script_params: dict[str, object] | None) -> dict[str, object] | None:
        nonlocal coerce_calls
        coerce_calls += 1
        return script_params

    def infer(coerced_params: dict[str, object] | None) -> dict[str, str]:
        nonlocal infer_calls
        infer_calls += 1
        assert coerced_params == params
        return {"id": "INT64", "name": "STRING"}

    monkeypatch.setattr(SpannerSyncDriver, "_coerce_params", lambda _self, script_params: coerce(script_params))
    monkeypatch.setattr(SpannerSyncDriver, "_infer_param_types", lambda _self, coerced_params: infer(coerced_params))
    mock_transaction.execute_update.return_value = 1
    statement = driver.prepare_statement(script, statement_config=driver.statement_config)

    driver.dispatch_execute_script(mock_transaction, statement)

    assert coerce_calls == 1
    assert infer_calls == 1
    assert mock_transaction.execute_update.call_count == 3


def test_dispatch_execute_many_no_shadowed_local_aliases() -> None:
    source = inspect.getsource(SpannerSyncDriver.dispatch_execute_many)

    assert "coerce_params = self._coerce_params" not in source
    assert "infer_param_types = self._infer_param_types" not in source
    assert "_coerce = self._coerce_params" in source
    assert "_infer = self._infer_param_types" in source
    assert "_coerce(cast" in source
    assert "_infer(coerced_params)" in source


def test_module_level_coerce_params_not_shadowed_by_local_alias() -> None:
    import sqlspec.adapters.spanner.driver as driver_module
    from sqlspec.adapters.spanner.core import coerce_params as core_coerce_params

    assert driver_module.coerce_params is core_coerce_params


def test_module_level_infer_param_types_not_shadowed_by_local_alias() -> None:
    import sqlspec.adapters.spanner.driver as driver_module
    from sqlspec.adapters.spanner.core import infer_param_types as core_infer_param_types

    assert driver_module.infer_param_types is core_infer_param_types
