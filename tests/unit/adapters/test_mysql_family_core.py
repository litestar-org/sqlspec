"""Unit tests for MySQL-family adapter core helpers."""

import importlib
import json
from types import SimpleNamespace

import pytest

MODULE_PATHS = (
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
def test_collect_rows_accepts_precomputed_column_names(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    column_names = ["id", "payload"]
    rows = [{"id": 1, "payload": json.dumps({"a": 1})}]

    data, resolved_column_names, row_format = core.collect_rows(
        rows, description, [1], json.loads, column_names=column_names
    )

    assert resolved_column_names is column_names
    assert row_format == "dict"
    assert data[0]["payload"] == {"a": 1}


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_detect_json_columns_uses_provided_description(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    cursor = SimpleNamespace(description=description)

    assert core.detect_json_columns(cursor, {245}, description=description) == [1]


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_rows_without_json_reuses_input_list(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("name", 253)]
    column_names = ["id", "name"]
    rows = [{"id": 1, "name": "a"}]

    data, resolved_column_names, row_format = core.collect_rows(
        rows, description, [], json.loads, column_names=column_names
    )

    assert data is rows
    assert resolved_column_names is column_names
    assert row_format == "dict"
