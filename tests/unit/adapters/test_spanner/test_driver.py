from unittest.mock import MagicMock, Mock

import pytest
from google.cloud.spanner_v1 import Transaction
from google.cloud.spanner_v1.streamed import StreamedResultSet

from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import SQLConversionError


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
