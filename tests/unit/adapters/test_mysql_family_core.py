"""Unit tests for MySQL-family adapter core helpers."""

import importlib
import json
from types import SimpleNamespace
from typing import Any

import pytest

MODULE_PATHS = (
    "sqlspec.adapters.aiomysql.core",
    "sqlspec.adapters.asyncmy.core",
    "sqlspec.adapters.mysqlconnector.core",
    "sqlspec.adapters.pymysql.core",
)


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_resolve_many_rowcount_prefers_driver_rowcount(module_path: str) -> None:
    core = importlib.import_module(module_path)
    cursor = SimpleNamespace(rowcount=7)
    assert core.resolve_many_rowcount(cursor, [{"id": 1}], fallback_count=1) == 7


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_resolve_many_rowcount_falls_back_to_parameter_count(module_path: str) -> None:
    core = importlib.import_module(module_path)
    cursor = SimpleNamespace(rowcount=0)
    assert core.resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}, {"id": 3}]) == 3


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_resolve_row_plan_combines_column_names_and_json_indexes(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245), ("name", 253)]

    column_names, json_indexes = core.resolve_row_plan(description, {245})

    assert column_names == ["id", "payload", "name"]
    assert json_indexes == [1]


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_rows_accepts_precomputed_row_plan(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    row_plan = core.resolve_row_plan(description, {245})
    rows = [{"id": 1, "payload": json.dumps({"a": 1})}]

    data, resolved_column_names, row_format = core.collect_rows(rows, row_plan, json.loads)

    assert resolved_column_names is row_plan[0]
    assert row_format == "dict"
    assert data is rows
    assert data[0]["payload"] == {"a": 1}


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_rows_without_json_reuses_input_list(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("name", 253)]
    row_plan = core.resolve_row_plan(description, set())
    rows = [{"id": 1, "name": "a"}]

    data, resolved_column_names, row_format = core.collect_rows(rows, row_plan, json.loads)

    assert data is rows
    assert resolved_column_names is row_plan[0]
    assert row_format == "dict"


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_stream_rows_decodes_json_using_row_plan(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    row_plan = core.resolve_row_plan(description, {245})
    rows = [(1, json.dumps({"a": 1}))]

    data = core.collect_stream_rows(rows, row_plan, json.loads)

    assert data == [{"id": 1, "payload": {"a": 1}}]


@pytest.mark.parametrize("module_path", MODULE_PATHS)
@pytest.mark.parametrize(
    ("column_descriptor", "expected"),
    [
        (("amount", 246, None, 12, 12, 2, True), {"amount": "decimal(11,2)"}),
        (("n", 246, None, 11, 11, 0, True), {"n": "decimal(11,0)"}),
        (("n", 0, None, 11, 11, 2, True), {"n": "decimal"}),
        (("n", 246, None, None, None, 2, True), {"n": "decimal"}),
        (("n", 246, None, 90, 90, 2, True), {"n": "decimal"}),
    ],
)
def test_resolve_column_types_newdecimal_parity(
    module_path: str, column_descriptor: tuple[Any, ...], expected: dict[str, str]
) -> None:
    core = importlib.import_module(module_path)

    assert core._resolve_column_types([column_descriptor]) == expected


@pytest.mark.parametrize(
    ("config_path", "config_name", "params_name", "pool_params_name"),
    [
        ("sqlspec.adapters.aiomysql.config", "AiomysqlConfig", "AiomysqlConnectionParams", "AiomysqlPoolParams"),
        ("sqlspec.adapters.asyncmy.config", "AsyncmyConfig", "AsyncmyConnectionParams", "AsyncmyPoolParams"),
        (
            "sqlspec.adapters.mysqlconnector.config",
            "MysqlConnectorSyncConfig",
            "MysqlConnectorSyncConnectionParams",
            "MysqlConnectorPoolParams",
        ),
        (
            "sqlspec.adapters.mysqlconnector.config",
            "MysqlConnectorAsyncConfig",
            "MysqlConnectorAsyncConnectionParams",
            "MysqlConnectorAsyncConnectionParams",
        ),
        ("sqlspec.adapters.pymysql.config", "PyMysqlConfig", "PyMysqlConnectionParams", "PyMysqlPoolParams"),
    ],
)
def test_mysql_replica_readonly_is_internal_and_preserves_init_command(
    config_path: str, config_name: str, params_name: str, pool_params_name: str
) -> None:
    config_mod = importlib.import_module(config_path)
    core_mod = importlib.import_module(config_path.rsplit(".", 1)[0] + ".core")
    config_cls = getattr(config_mod, config_name)
    params_cls = getattr(config_mod, params_name)
    pool_params_cls = getattr(config_mod, pool_params_name)

    assert "read_only" not in params_cls.__annotations__
    assert "read_only" not in pool_params_cls.__annotations__
    assert not hasattr(core_mod, "append_read_only_init_command")

    primary = config_cls(
        connection_config={"host": "writer", "init_command": "SET @probe = 1"},
        replicas=[{"host": "ro-1"}, {"host": "rw-1", "read_only": False}],
    )
    ro_replica, rw_replica = primary.replica_groups["default"].configs
    assert ro_replica.connection_config["init_command"] == "SET @probe = 1"
    assert rw_replica.connection_config["init_command"] == "SET @probe = 1"
    assert "read_only" not in ro_replica.connection_config
    assert "read_only" not in rw_replica.connection_config
    assert "_replica_read_only" not in ro_replica.driver_features
    assert "_replica_read_only" not in rw_replica.driver_features
    assert ro_replica._read_only is True
    assert rw_replica._read_only is False
