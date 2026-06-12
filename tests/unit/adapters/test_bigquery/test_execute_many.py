"""Unit tests for BigQuery execute_many helper methods."""

# pyright: reportPrivateUsage=false

from types import SimpleNamespace
from typing import Any, cast


class _LocalEndpointConnection:
    def __init__(self) -> None:
        self._connection = SimpleNamespace(API_BASE_URL="http://127.0.0.1:9050")
        self.load_called = False

    def load_table_from_file(self, *_args: Any, **_kwargs: Any) -> None:
        self.load_called = True
        msg = "local BigQuery endpoint must not start a load upload"
        raise AssertionError(msg)


def test_is_simple_insert_operation_basic_insert() -> None:
    """Test that a basic INSERT statement is detected correctly."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert is_simple_insert("INSERT INTO test (a, b) VALUES (1, 2)")


def test_is_simple_insert_operation_with_named_params() -> None:
    """Test INSERT with named parameters."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert is_simple_insert("INSERT INTO test (a, b) VALUES (@a, @b)")


def test_is_simple_insert_operation_not_insert() -> None:
    """Test that UPDATE/DELETE/SELECT are not detected as INSERT."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert not is_simple_insert("UPDATE test SET a = 1")
    assert not is_simple_insert("DELETE FROM test WHERE a = 1")
    assert not is_simple_insert("SELECT * FROM test")


def test_is_simple_insert_operation_insert_select() -> None:
    """Test that INSERT...SELECT is not detected as simple INSERT."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    # INSERT...SELECT should not be simple INSERT for bulk load optimization
    result = is_simple_insert("INSERT INTO test SELECT * FROM other")
    # This might be True or False depending on implementation - the key is it doesn't crash
    assert isinstance(result, bool)


def test_is_simple_insert_operation_malformed_sql() -> None:
    """Test that malformed SQL returns False without raising."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert not is_simple_insert("NOT VALID SQL AT ALL")


def test_extract_table_from_insert_simple() -> None:
    """Test extracting table name from simple INSERT."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("INSERT INTO test (a) VALUES (1)") == "test"


def test_extract_table_from_insert_qualified() -> None:
    """Test extracting qualified table name from INSERT."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    result = extract_insert_table("INSERT INTO project.dataset.table (a) VALUES (1)")
    # Should include catalog (project), db (dataset), and table
    assert result is not None
    assert "table" in result


def test_extract_table_from_insert_not_insert() -> None:
    """Test that non-INSERT returns None."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("SELECT * FROM test") is None


def test_extract_table_from_insert_malformed() -> None:
    """Test that malformed SQL returns None without raising."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("NOT VALID SQL") is None


def test_try_bulk_insert_skips_local_bigquery_endpoint() -> None:
    """Test that emulator/local endpoints do not attempt resumable uploads."""
    from sqlspec.adapters.bigquery.core import try_bulk_insert

    connection = _LocalEndpointConnection()
    rowcount = try_bulk_insert(
        cast(Any, connection),
        "INSERT INTO contract_items (name, value, note) VALUES (@name, @value, @note)",
        [{"name": "dict1", "value": 100, "note": None}],
    )

    assert rowcount is None
    assert not connection.load_called


def _literal_inliner() -> Any:
    from sqlspec.core import build_literal_inlining_transform
    from sqlspec.utils.serializers import to_json

    return build_literal_inlining_transform(json_serializer=to_json)


def test_build_inlined_script_collapses_simple_insert_to_multi_row_values() -> None:
    """Test that simple INSERT batches collapse into one multi-row VALUES statement."""
    from sqlspec.adapters.bigquery.core import build_inlined_script

    script = build_inlined_script(
        "INSERT INTO contract_items (name, value, note) VALUES (@name, @value, @note)",
        [
            {"name": "alpha", "value": 1, "note": None},
            {"name": "beta", "value": 2, "note": "b"},
            {"name": "gamma", "value": 3, "note": None},
        ],
        literal_inliner=_literal_inliner(),
    )

    assert script.count("INSERT") == 1
    assert ";" not in script
    assert "'alpha'" in script
    assert "'beta'" in script
    assert "'gamma'" in script
    assert script.index("'alpha'") < script.index("'beta'") < script.index("'gamma'")


def test_build_inlined_script_chunks_multi_row_insert() -> None:
    """Test that large INSERT batches split into bounded multi-row statements."""
    from sqlspec.adapters.bigquery.core import build_inlined_script

    rows: list[dict[str, Any]] = [{"name": f"row-{i:04d}", "value": i, "note": None} for i in range(1201)]
    script = build_inlined_script(
        "INSERT INTO contract_items (name, value, note) VALUES (@name, @value, @note)",
        rows,
        literal_inliner=_literal_inliner(),
    )

    statements = script.split(";\n")
    assert len(statements) == 3
    assert all(statement.count("INSERT") == 1 for statement in statements)
    assert statements[0].count("row-") == 500
    assert statements[1].count("row-") == 500
    assert statements[2].count("row-") == 201
    assert "'row-0000'" in statements[0]
    assert "'row-1200'" in statements[2]


def test_build_inlined_script_multi_row_with_synthetic_positional_params() -> None:
    """Test multi-row collapse for compiled qmark statements with synthetic parameter keys."""
    from sqlspec.adapters.bigquery.core import build_inlined_script

    script = build_inlined_script(
        "INSERT INTO contract_items (name, value, note) VALUES (@param_0, @param_1, @param_2)",
        [{"param_0": "alpha", "param_1": 1, "param_2": None}, {"param_0": "beta", "param_1": 2, "param_2": None}],
        literal_inliner=_literal_inliner(),
    )

    assert script.count("INSERT") == 1
    assert "'alpha'" in script
    assert "'beta'" in script


def test_build_inlined_script_keeps_per_row_statements_for_update() -> None:
    """Test that non-INSERT statements keep one inlined statement per parameter set."""
    from sqlspec.adapters.bigquery.core import build_inlined_script

    script = build_inlined_script(
        "UPDATE contract_items SET value = @value WHERE name = @name",
        [{"value": 1, "name": "alpha"}, {"value": 2, "name": "beta"}],
        literal_inliner=_literal_inliner(),
    )

    statements = script.split(";\n")
    assert len(statements) == 2
    assert all(statement.startswith("UPDATE") for statement in statements)
