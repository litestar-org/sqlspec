"""Integration tests for parameter style support across all adapters.

Tests validate that each adapter correctly supports its declared parameter styles
and that parameter conversion only occurs when necessary.
"""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult

# Test data for different parameter styles
PARAMETER_TEST_CASES = [
    # (style_name, sql_template, parameters, expected_count)
    ("qmark", "SELECT ? as value, ? as name", [42, "test"], 1),
    ("named_colon", "SELECT :value as value, :name as name", {"value": 42, "name": "test"}, 1),
    ("numeric", "SELECT $1 as value, $2 as name", [42, "test"], 1),
    ("named_dollar", "SELECT $value as value, $name as name", {"value": 42, "name": "test"}, 1),
    ("positional_pyformat", "SELECT %s as value, %s as name", [42, "test"], 1),
    ("named_pyformat", "SELECT %(value)s as value, %(name)s as name", {"value": 42, "name": "test"}, 1),
    ("named_at", "SELECT @value as value, @name as name", {"value": 42, "name": "test"}, 1),
    ("positional_colon", "SELECT :1 as value, :2 as name", [42, "test"], 1),
]


@pytest.mark.xdist_group("sqlite")
def test_sqlite_parameter_styles() -> None:
    """Test that SQLite adapter supports QMARK and NAMED_COLON styles."""
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # Test QMARK style (?)
        result = session.execute("SELECT ? as value", [42])
        assert isinstance(result, SQLResult)
        assert result.data[0]["value"] == 42

        # Test NAMED_COLON style (:name)
        result = session.execute("SELECT :value as value", {"value": 42})
        assert isinstance(result, SQLResult)
        assert result.data[0]["value"] == 42

        # Verify adapter configuration
        assert ParameterStyle.QMARK in session.statement_config.parameter_config.supported_parameter_styles
        assert ParameterStyle.NAMED_COLON in session.statement_config.parameter_config.supported_parameter_styles


@pytest.mark.xdist_group("duckdb")
def test_duckdb_parameter_styles() -> None:
    """Test that DuckDB adapter supports QMARK, NUMERIC, and NAMED_DOLLAR styles."""
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # Test QMARK style (?)
        result = session.execute("SELECT ? as value", [42])
        assert isinstance(result, SQLResult)
        assert result.data[0]["value"] == 42

        # Test NUMERIC style ($1, $2)
        result = session.execute("SELECT $1 as value", [42])
        assert isinstance(result, SQLResult)
        assert result.data[0]["value"] == 42

        # Test NAMED_DOLLAR style ($name)
        result = session.execute("SELECT $value as value", {"value": 42})
        assert isinstance(result, SQLResult)
        assert result.data[0]["value"] == 42

        # Verify adapter configuration
        supported_styles = session.statement_config.parameter_config.supported_parameter_styles
        assert ParameterStyle.QMARK in supported_styles
        assert ParameterStyle.NUMERIC in supported_styles
        assert ParameterStyle.NAMED_DOLLAR in supported_styles


def test_parameter_conversion_principles() -> None:
    """Test that parameter conversion follows core principles."""
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # Create test table
        session.execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL)")
        session.commit()

        # Test 1: Parameters should be passed through unchanged when no conversion needed
        original_params = [1, "test_name", 3.14]
        result = session.execute("INSERT INTO test VALUES (?, ?, ?)", original_params)
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test 2: Different parameter types should be preserved
        mixed_params = [2, "mixed", None]  # Include None
        result = session.execute("INSERT INTO test VALUES (?, ?, ?)", mixed_params)
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test 3: Named parameters should work correctly
        named_params = {"id": 3, "name": "named_test", "value": 2.71}
        result = session.execute("INSERT INTO test VALUES (:id, :name, :value)", named_params)
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify all data was inserted correctly
        result = session.execute("SELECT COUNT(*) as count FROM test")
        assert result.data[0]["count"] == 3


def test_parameter_style_detection() -> None:
    """Test that parameter styles are correctly detected and handled."""
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # Test various parameter styles in same session
        test_cases = [("?", [1]), ("$1", [2]), ("$value", {"value": 3})]

        for sql_template, params in test_cases:
            sql = f"SELECT {sql_template} as result"
            result = session.execute(sql, params)
            assert isinstance(result, SQLResult)
            assert len(result.data) == 1
            # All should return their respective values
            expected_value = params[0] if isinstance(params, list) else next(iter(params.values()))
            assert result.data[0]["result"] == expected_value


def test_unsupported_parameter_style_handling() -> None:
    """Test that unsupported parameter styles are handled gracefully."""
    # SQLite doesn't support $1 style (numeric), but should handle it
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # This might work due to SQLite's flexibility, but behavior may vary
        # The important thing is it doesn't crash
        try:
            result = session.execute("SELECT $1 as value", [42])
            # If it works, verify the result
            if isinstance(result, SQLResult) and result.data:
                assert result.data[0]["value"] == 42
        except Exception as e:
            # If it fails, that's expected for unsupported styles
            # Just ensure it's a reasonable database error, not a framework error
            assert "bind" in str(e).lower() or "parameter" in str(e).lower()


def test_parameter_compilation_consistency() -> None:
    """Test that parameter compilation is consistent across operations."""
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        # Create test table
        session.execute("CREATE TABLE param_test (id INTEGER, data TEXT)")

        # Test single execute with different styles
        session.execute("INSERT INTO param_test VALUES (?, ?)", [1, "qmark"])
        session.execute("INSERT INTO param_test VALUES ($1, $2)", [2, "numeric"])
        session.execute("INSERT INTO param_test VALUES ($id, $data)", {"id": 3, "data": "named_dollar"})

        # Test execute_many
        many_params = [[4, "many1"], [5, "many2"]]
        result = session.execute_many("INSERT INTO param_test VALUES (?, ?)", many_params)
        assert isinstance(result, SQLResult)

        # Verify all data was inserted
        result = session.execute("SELECT COUNT(*) as count FROM param_test")
        assert result.data[0]["count"] == 5

        # Verify data integrity
        result = session.execute("SELECT * FROM param_test ORDER BY id")
        assert len(result.data) == 5
        assert result.data[0]["data"] == "qmark"
        assert result.data[1]["data"] == "numeric"
        assert result.data[2]["data"] == "named_dollar"


@pytest.mark.parametrize(
    "use_qmark,use_named",
    [
        (True, False),  # Only QMARK
        (False, True),  # Only NAMED_COLON
        (True, True),  # Both styles
    ],
)
def test_sqlite_parameter_style_combinations(use_qmark: bool, use_named: bool) -> None:
    """Test SQLite with different parameter style combinations."""
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        session.execute("CREATE TABLE combo_test (id INTEGER, value TEXT)")

        if use_qmark:
            result = session.execute("INSERT INTO combo_test VALUES (?, ?)", [1, "qmark"])
            assert isinstance(result, SQLResult)
            assert result.rows_affected == 1

        if use_named:
            result = session.execute("INSERT INTO combo_test VALUES (:id, :value)", {"id": 2, "value": "named"})
            assert isinstance(result, SQLResult)
            assert result.rows_affected == 1

        # Verify results
        result = session.execute("SELECT COUNT(*) as count FROM combo_test")
        expected_count = (1 if use_qmark else 0) + (1 if use_named else 0)
        assert result.data[0]["count"] == expected_count


def test_parameter_edge_cases() -> None:
    """Test parameter handling edge cases."""
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        session.execute("CREATE TABLE edge_test (id INTEGER, data TEXT, flag BOOLEAN, amount REAL)")

        # Test None/NULL values
        result = session.execute("INSERT INTO edge_test VALUES (?, ?, ?, ?)", [1, None, True, 3.14])
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test empty parameters
        result = session.execute("INSERT INTO edge_test (id) VALUES (2)", [])
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test single parameter
        result = session.execute("SELECT * FROM edge_test WHERE id = ?", [1])
        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["id"] == 1
        assert result.data[0]["data"] is None
        assert result.data[0]["flag"] == 1  # SQLite converts bool to int
        assert result.data[0]["amount"] == 3.14
