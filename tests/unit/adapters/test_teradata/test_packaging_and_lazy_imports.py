"""Runtime optional-dependency and entry-point contracts for Teradata."""

import subprocess
import sys
from importlib.metadata import EntryPoint

import pytest

from sqlspec.adapters.teradata import TeradataConfig
from sqlspec.exceptions import MissingDependencyError
from sqlspec.utils.module_loader import reset_dependency_cache


def test_cold_import_teradata_packages_without_teradatasql() -> None:
    script = """
import sys
sys.modules['teradatasql'] = None
from sqlspec.adapters.teradata import TeradataConfig
import sqlspec.adapters.teradata.litestar
import sqlspec.adapters.teradata.events
import sqlspec.adapters.teradata.adk
import sqlspec.dialects.teradata
import sqlspec.data_dictionary.dialects.teradata
config = TeradataConfig(connection_config={'host': 'td.local'})
assert config.connection_config['dbs_port'] == '1025'
assert config.connection_config['charset'] == 'UTF8'
print('cold-import:ok')
"""
    result = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cold-import:ok"


def test_create_connection_raises_missing_dependency_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "teradatasql", None)
    reset_dependency_cache("teradatasql")
    try:
        config = TeradataConfig(connection_config={"host": "td.local", "user": "alice", "password": "pw"})
        with pytest.raises(MissingDependencyError, match="teradatasql"):
            config.create_connection()
        with pytest.raises(MissingDependencyError, match="teradata"):
            with config.provide_session():
                pytest.fail("Session opened without teradatasql")
    finally:
        reset_dependency_cache("teradatasql")


def test_dialect_entry_point_loads_without_teradatasql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "teradatasql", None)
    from sqlspec import sql
    from sqlspec.data_dictionary import get_dialect_config

    dialect = EntryPoint(name="teradata", value="sqlspec.dialects.teradata:Teradata", group="sqlglot.dialects").load()
    assert dialect.__name__ == "Teradata"
    result = sql.select("id").from_("orders").order_by("id").limit(5).offset(10).build(dialect="teradata")
    assert "BETWEEN 11 AND 15" in result.sql
    assert get_dialect_config("vantage").name == "teradata"
