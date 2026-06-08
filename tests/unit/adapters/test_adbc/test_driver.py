"""Unit tests for ADBC driver execute path optimizations."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc.driver import AdbcDriver, AdbcExceptionHandler


def test_adbc_driver_classes_are_final() -> None:
    assert getattr(AdbcExceptionHandler, "__final__", False) is True
    assert getattr(AdbcDriver, "__final__", False) is True


def test_dispatch_execute_many_non_postgres_uses_compiled_parameter_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "sqlite", "driver_name": "sqlite"}
    driver = AdbcDriver(connection)

    prepared_parameters = [(1,), (2,), (3,)]
    monkeypatch.setattr(
        AdbcDriver,
        "_get_compiled_sql",
        lambda self, statement, config: ("INSERT INTO test (id) VALUES (?)", prepared_parameters),
    )

    statement = driver.prepare_statement("INSERT INTO test (id) VALUES (?)", statement_config=driver.statement_config)
    cursor = MagicMock()
    cursor.rowcount = -1

    result = driver.dispatch_execute_many(cursor, statement)  # type: ignore[protected-access]

    cursor.executemany.assert_called_once_with("INSERT INTO test (id) VALUES (?)", prepared_parameters)
    assert result.rowcount_override == 3


def test_dispatch_execute_postgres_uses_compiled_parameters_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "postgresql", "driver_name": "postgresql"}
    driver = AdbcDriver(connection)

    prepared_parameters = (1, "alice")
    monkeypatch.setattr(
        AdbcDriver,
        "_get_compiled_sql",
        lambda self, statement, config: ("UPDATE users SET name = $2 WHERE id = $1", prepared_parameters),
    )

    def _unexpected_prepare(*_: object, **__: object) -> object:
        msg = "prepare_postgres_parameters should not run in dispatch_execute"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.adapters.adbc.driver.prepare_postgres_parameters", _unexpected_prepare)

    statement = driver.prepare_statement(
        "UPDATE users SET name = @name WHERE id = @id", statement_config=driver.statement_config
    )
    cursor = MagicMock()
    cursor.rowcount = 1

    result = driver.dispatch_execute(cursor, statement)  # type: ignore[protected-access]

    cursor.execute.assert_called_once_with("UPDATE users SET name = $2 WHERE id = $1", parameters=prepared_parameters)
    assert result.rowcount_override == 1


def test_prepare_driver_parameters_postgres_applies_cast_aware_preparation_when_casts_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "postgresql", "driver_name": "postgresql"}
    driver = AdbcDriver(connection)

    prepared_parameters = (1, {"name": "alice"})
    monkeypatch.setattr("sqlspec.adapters.adbc.driver.resolve_parameter_casts", lambda statement: {2: "JSONB"})
    monkeypatch.setattr(
        "sqlspec.adapters.adbc.driver.prepare_postgres_parameters",
        lambda parameters, parameter_casts, statement_config, dialect, json_serializer: (
            parameters[0],
            '{"name": "alice"}',
        ),
    )

    statement = driver.prepare_statement(
        "SELECT $2::jsonb FROM users WHERE id = $1", statement_config=driver.statement_config
    )

    result = driver.prepare_driver_parameters(
        prepared_parameters, driver.statement_config, prepared_statement=statement
    )

    assert result == (1, '{"name": "alice"}')


def test_dispatch_execute_many_postgres_without_casts_uses_compiled_parameter_sets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "postgresql", "driver_name": "postgresql"}
    driver = AdbcDriver(connection)

    prepared_parameters = [(1, "alice"), (2, "bob")]
    monkeypatch.setattr(
        AdbcDriver,
        "_get_compiled_sql",
        lambda self, statement, config: ("INSERT INTO users (id, name) VALUES ($1, $2)", prepared_parameters),
    )
    monkeypatch.setattr("sqlspec.adapters.adbc.driver.resolve_parameter_casts", lambda statement: {})

    def _unexpected_normalize(*_: object, **__: object) -> object:
        msg = "normalize_postgres_empty_parameters should not run for cast-free execute_many rows"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.adapters.adbc.driver.normalize_postgres_empty_parameters", _unexpected_normalize)

    statement = driver.prepare_statement(
        "INSERT INTO users (id, name) VALUES ($1, $2)", statement_config=driver.statement_config
    )
    cursor = MagicMock()
    cursor.rowcount = -1

    result = driver.dispatch_execute_many(cursor, statement)  # type: ignore[protected-access]

    cursor.executemany.assert_called_once_with("INSERT INTO users (id, name) VALUES ($1, $2)", prepared_parameters)
    assert result.rowcount_override == 2


def test_prepare_driver_parameters_many_postgres_applies_cast_processing_per_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "postgresql", "driver_name": "postgresql"}
    driver = AdbcDriver(connection)
    statement = driver.prepare_statement(
        "INSERT INTO events (id, payload) VALUES ($1, $2::jsonb)", statement_config=driver.statement_config
    )
    prepared_rows = [(1, "alpha"), (2, "beta")]
    calls: list[object] = []

    monkeypatch.setattr("sqlspec.adapters.adbc.driver.resolve_parameter_casts", lambda _statement: {2: "JSONB"})

    def prepare_batch(parameters: object, *_args: object, **_kwargs: object) -> object:
        calls.append(parameters)
        return [(1, "prepared"), (2, "prepared")]

    monkeypatch.setattr("sqlspec.adapters.adbc.driver._prepare_batch_with_casts", prepare_batch)

    result = driver.prepare_driver_parameters(
        prepared_rows, driver.statement_config, is_many=True, prepared_statement=statement
    )

    assert result == [(1, "prepared"), (2, "prepared")]
    assert calls == [prepared_rows]


def test_dispatch_execute_many_postgres_with_casts_calls_batch_helper_once(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "postgresql", "driver_name": "postgresql"}
    driver = AdbcDriver(connection)
    statement = driver.prepare_statement(
        "INSERT INTO events (id, payload) VALUES ($1, $2::jsonb)",
        ([(1, "alpha"), (2, "beta")],),
        statement_config=driver.statement_config,
        kwargs={"is_many": True},
    )
    prepared_rows = [(1, "alpha"), (2, "beta")]
    calls: list[object] = []

    monkeypatch.setattr("sqlspec.adapters.adbc.driver.resolve_parameter_casts", lambda _statement: {2: "JSONB"})

    def prepare_batch(parameters: object, *_args: object, **_kwargs: object) -> object:
        calls.append(parameters)
        return [(1, "prepared"), (2, "prepared")]

    monkeypatch.setattr("sqlspec.adapters.adbc.driver._prepare_batch_with_casts", prepare_batch)

    cursor = MagicMock()
    cursor.rowcount = -1

    result = driver.dispatch_execute_many(cursor, statement)  # type: ignore[protected-access]

    assert calls == [prepared_rows]
    cursor.executemany.assert_called_once_with(
        "INSERT INTO events (id, payload) VALUES ($1, CAST($2 AS JSONB))", [(1, "prepared"), (2, "prepared")]
    )
    assert result.rowcount_override == 2
