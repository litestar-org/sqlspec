"""Exercise session hooks and exceptional lease cleanup through public configs."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata import TeradataConfig, TeradataDriver
from sqlspec.exceptions import TransactionError


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    result = []

    def connect(**kwargs: Any) -> Any:
        connection = MagicMock()
        connection.autocommit = True
        result.append(connection)
        return connection

    monkeypatch.setattr("sqlspec.adapters.teradata.pool.import_optional", lambda name: SimpleNamespace(connect=connect))
    return result


def test_config_session_releases_connection_after_success(connections: list[Any]) -> None:
    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()
    with config.provide_session() as session:
        assert isinstance(session, TeradataDriver)
        assert pool.checked_out() == 1
    assert pool.checked_out() == 0
    with config.provide_session() as session:
        assert session.connection is connections[0]
    pool.close()


def test_session_prepare_hook_failure_discards_lease(connections: list[Any]) -> None:
    def fail(session: Any) -> None:
        raise ValueError("session setup failed")

    from sqlspec.adapters.teradata import TeradataSessionContext

    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()

    def release(connection: Any, **kwargs: Any) -> None:
        pool.release(connection, discard=kwargs.get("exc_type") is not None)

    context = TeradataSessionContext(pool.acquire, release, config.statement_config, dict(config.driver_features), fail)
    with pytest.raises(ValueError, match="session setup failed"):
        with context:
            pytest.fail("setup hook must fail before yielding session")
    assert pool.checked_out() == 0
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    pool.close()


def test_session_rollback_failure_discards_lease(connections: list[Any]) -> None:
    config = TeradataConfig(connection_config={"pool_max_size": 1})
    pool = config.provide_pool()
    with pytest.raises(TransactionError, match="rollback"):
        with config.provide_session() as session:
            session.begin()
            session.connection.rollback.side_effect = RuntimeError("socket gone")
    assert pool.checked_out() == 0
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    pool.close()


def test_config_signature_namespace_and_standalone_connection(connections: list[Any]) -> None:
    config = TeradataConfig()
    namespace = config.get_signature_namespace()
    assert namespace["TeradataConfig"] is TeradataConfig
    assert namespace["TeradataDriver"] is TeradataDriver
    standalone = config.create_connection()
    assert standalone is connections[0]
    assert config.provide_pool().checked_out() == 0
    standalone.close()
    config.close_pool()


@pytest.mark.parametrize("error", [False, True])
def test_native_cursor_context_closes_after_use(error: bool) -> None:
    from sqlspec.adapters.teradata import TeradataCursor

    connection = MagicMock()
    if error:
        with pytest.raises(ValueError, match="application failed"):
            with TeradataCursor(connection) as cursor:
                cursor.execute("SELECT 1")
                raise ValueError("application failed")
    else:
        with TeradataCursor(connection) as cursor:
            cursor.execute("SELECT 1")
    connection.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    connection.cursor.return_value.close.assert_called_once_with()
