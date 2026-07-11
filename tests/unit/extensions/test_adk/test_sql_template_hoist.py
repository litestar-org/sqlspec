"""Regression tests for adapter ADK SQL template ownership."""

import ast
import importlib
import inspect

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "sqlspec.adapters.psycopg.adk.store",
        "sqlspec.adapters.cockroach_psycopg.adk.store",
        "sqlspec.adapters.mysqlconnector.adk.store",
        "sqlspec.adapters.oracledb.adk.store",
    ],
)
def test_adk_ddl_methods_reference_module_templates(module_name: str) -> None:
    module = importlib.import_module(module_name)
    tree = ast.parse(inspect.getsource(module))

    sql_owners = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and ("ddl" in node.name or "seed" in node.name)
    ]
    inline_templates = [node for owner in sql_owners for node in ast.walk(owner) if isinstance(node, ast.JoinedStr)]

    assert not inline_templates
