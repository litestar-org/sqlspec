"""Unit tests for EXPLAIN plan support.

This module tests the EXPLAIN statement building functionality including:
1. ExplainOptions - Configuration options for EXPLAIN statements
2. ExplainFormat - Output format enumeration
3. Explain builder - Fluent interface for building EXPLAIN statements
4. Dialect-specific SQL generation
5. Integration with QueryBuilders (Select, Insert, Update, Delete, Merge)
6. SQLFactory integration
7. SQL class integration

Test Coverage:
- ExplainOptions creation and methods (__init__, copy, to_dict, __eq__, __hash__, __repr__)
- ExplainFormat enumeration values
- Explain builder construction and method chaining
- Dialect-specific EXPLAIN SQL generation (PostgreSQL, MySQL, SQLite, DuckDB, Oracle, BigQuery)
- Parameter preservation from underlying statements
- Integration with all query builders that use ExplainMixin
"""

import pytest
from sqlglot import exp

from sqlspec.builder import (
    Delete,
    Explain,
    Insert,
    Merge,
    Select,
    SQLFactory,
    Update,
    build_bigquery_explain,
    build_duckdb_explain,
    build_explain_sql,
    build_generic_explain,
    build_mysql_explain,
    build_oracle_explain,
    build_postgres_explain,
    build_sqlite_explain,
    normalize_dialect_name,
    sql,
)
from sqlspec.core import SQL
from sqlspec.core.explain import ExplainFormat, ExplainOptions
from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import SQLBuilderError

# pyright: reportPrivateUsage=false
# mypy: disable-error-code="comparison-overlap,arg-type"


pytestmark = pytest.mark.xdist_group("explain")


# -----------------------------------------------------------------------------
# ExplainFormat Tests
# -----------------------------------------------------------------------------


def test_explain_format_text_value():
    """Test ExplainFormat.TEXT has correct value."""
    assert ExplainFormat.TEXT.value == "text"


def test_explain_format_json_value():
    """Test ExplainFormat.JSON has correct value."""
    assert ExplainFormat.JSON.value == "json"


def test_explain_format_xml_value():
    """Test ExplainFormat.XML has correct value."""
    assert ExplainFormat.XML.value == "xml"


def test_explain_format_yaml_value():
    """Test ExplainFormat.YAML has correct value."""
    assert ExplainFormat.YAML.value == "yaml"


def test_explain_format_tree_value():
    """Test ExplainFormat.TREE has correct value."""
    assert ExplainFormat.TREE.value == "tree"


def test_explain_format_traditional_value():
    """Test ExplainFormat.TRADITIONAL has correct value."""
    assert ExplainFormat.TRADITIONAL.value == "traditional"


def test_explain_format_is_string_enum():
    """Test ExplainFormat inherits from str."""
    assert isinstance(ExplainFormat.TEXT, str)
    assert ExplainFormat.JSON == "json"


def test_explain_format_creation_from_string():
    """Test ExplainFormat can be created from lowercase string."""
    assert ExplainFormat("json") == ExplainFormat.JSON
    assert ExplainFormat("xml") == ExplainFormat.XML


# -----------------------------------------------------------------------------
# ExplainOptions Tests
# -----------------------------------------------------------------------------


def test_explain_options_default_values():
    """Test ExplainOptions has correct default values."""
    options = ExplainOptions()

    assert options.analyze is False
    assert options.verbose is False
    assert options.format is None
    assert options.costs is None
    assert options.buffers is None
    assert options.timing is None
    assert options.summary is None
    assert options.memory is None
    assert options.settings is None
    assert options.wal is None
    assert options.generic_plan is None


def test_explain_options_with_analyze():
    """Test ExplainOptions with analyze=True."""
    options = ExplainOptions(analyze=True)

    assert options.analyze is True
    assert options.verbose is False


def test_explain_options_with_verbose():
    """Test ExplainOptions with verbose=True."""
    options = ExplainOptions(verbose=True)

    assert options.verbose is True
    assert options.analyze is False


def test_explain_options_with_format_enum():
    """Test ExplainOptions with format as ExplainFormat enum."""
    options = ExplainOptions(format=ExplainFormat.JSON)

    assert options.format == ExplainFormat.JSON


def test_explain_options_with_format_string():
    """Test ExplainOptions with format as string (converted to enum)."""
    options = ExplainOptions(format="json")

    assert options.format == ExplainFormat.JSON


def test_explain_options_with_format_uppercase_string():
    """Test ExplainOptions with uppercase format string (converted to lowercase)."""
    options = ExplainOptions(format="JSON")

    assert options.format == ExplainFormat.JSON


def test_explain_options_with_all_boolean_options():
    """Test ExplainOptions with all boolean options set."""
    options = ExplainOptions(
        analyze=True,
        verbose=True,
        costs=True,
        buffers=True,
        timing=True,
        summary=True,
        memory=True,
        settings=True,
        wal=True,
        generic_plan=True,
    )

    assert options.analyze is True
    assert options.verbose is True
    assert options.costs is True
    assert options.buffers is True
    assert options.timing is True
    assert options.summary is True
    assert options.memory is True
    assert options.settings is True
    assert options.wal is True
    assert options.generic_plan is True


def test_explain_options_with_false_boolean_options():
    """Test ExplainOptions with explicitly False boolean options."""
    options = ExplainOptions(costs=False, buffers=False)

    assert options.costs is False
    assert options.buffers is False


def test_explain_options_copy_no_modifications():
    """Test ExplainOptions.copy() with no modifications returns equivalent object."""
    original = ExplainOptions(analyze=True, format=ExplainFormat.JSON)
    copied = original.copy()

    assert copied == original
    assert copied is not original


def test_explain_options_copy_with_analyze_override():
    """Test ExplainOptions.copy() with analyze override."""
    original = ExplainOptions(analyze=False)
    copied = original.copy(analyze=True)

    assert copied.analyze is True
    assert original.analyze is False


def test_explain_options_copy_with_format_override():
    """Test ExplainOptions.copy() with format override."""
    original = ExplainOptions(format=ExplainFormat.TEXT)
    copied = original.copy(format=ExplainFormat.JSON)

    assert copied.format == ExplainFormat.JSON
    assert original.format == ExplainFormat.TEXT


def test_explain_options_copy_with_multiple_overrides():
    """Test ExplainOptions.copy() with multiple overrides."""
    original = ExplainOptions(analyze=False, verbose=False, costs=None)
    copied = original.copy(analyze=True, verbose=True, buffers=True)

    assert copied.analyze is True
    assert copied.verbose is True
    assert copied.buffers is True
    assert copied.costs is None


def test_explain_options_to_dict_empty():
    """Test ExplainOptions.to_dict() with default options returns empty dict."""
    options = ExplainOptions()
    result = options.to_dict()

    assert result == {}


def test_explain_options_to_dict_with_analyze():
    """Test ExplainOptions.to_dict() includes analyze when True."""
    options = ExplainOptions(analyze=True)
    result = options.to_dict()

    assert result == {"analyze": True}


def test_explain_options_to_dict_with_verbose():
    """Test ExplainOptions.to_dict() includes verbose when True."""
    options = ExplainOptions(verbose=True)
    result = options.to_dict()

    assert result == {"verbose": True}


def test_explain_options_to_dict_with_format():
    """Test ExplainOptions.to_dict() includes format value as uppercase string."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = options.to_dict()

    assert result == {"format": "JSON"}


def test_explain_options_to_dict_with_boolean_options():
    """Test ExplainOptions.to_dict() includes boolean options when set."""
    options = ExplainOptions(costs=True, buffers=False, timing=True)
    result = options.to_dict()

    assert result == {"costs": True, "buffers": False, "timing": True}


def test_explain_options_to_dict_full():
    """Test ExplainOptions.to_dict() with all options set."""
    options = ExplainOptions(
        analyze=True,
        verbose=True,
        format=ExplainFormat.JSON,
        costs=True,
        buffers=True,
        timing=True,
        summary=True,
        memory=True,
        settings=True,
        wal=True,
        generic_plan=True,
    )
    result = options.to_dict()

    assert result == {
        "analyze": True,
        "verbose": True,
        "format": "JSON",
        "costs": True,
        "buffers": True,
        "timing": True,
        "summary": True,
        "memory": True,
        "settings": True,
        "wal": True,
        "generic_plan": True,
    }


def test_explain_options_equality_same_values():
    """Test ExplainOptions equality with same values."""
    options1 = ExplainOptions(analyze=True, format=ExplainFormat.JSON)
    options2 = ExplainOptions(analyze=True, format=ExplainFormat.JSON)

    assert options1 == options2


def test_explain_options_equality_different_values():
    """Test ExplainOptions inequality with different values."""
    options1 = ExplainOptions(analyze=True)
    options2 = ExplainOptions(analyze=False)

    assert options1 != options2


def test_explain_options_equality_non_explain_options():
    """Test ExplainOptions inequality with non-ExplainOptions object."""
    options = ExplainOptions()

    assert options != "not an ExplainOptions"
    assert options != 123
    assert options is not None
    assert (options == None) is False  # noqa: E711


def test_explain_options_hash_same_values():
    """Test ExplainOptions hash is same for equal objects."""
    options1 = ExplainOptions(analyze=True, format=ExplainFormat.JSON)
    options2 = ExplainOptions(analyze=True, format=ExplainFormat.JSON)

    assert hash(options1) == hash(options2)


def test_explain_options_hash_different_values():
    """Test ExplainOptions hash differs for different objects."""
    options1 = ExplainOptions(analyze=True)
    options2 = ExplainOptions(analyze=False)

    assert hash(options1) != hash(options2)


def test_explain_options_hash_usable_in_set():
    """Test ExplainOptions can be used in sets."""
    options1 = ExplainOptions(analyze=True)
    options2 = ExplainOptions(analyze=True)
    options3 = ExplainOptions(analyze=False)

    option_set = {options1, options2, options3}

    assert len(option_set) == 2


def test_explain_options_repr_empty():
    """Test ExplainOptions.__repr__ with default options."""
    options = ExplainOptions()
    repr_str = repr(options)

    assert repr_str == "ExplainOptions()"


def test_explain_options_repr_with_analyze():
    """Test ExplainOptions.__repr__ with analyze=True."""
    options = ExplainOptions(analyze=True)
    repr_str = repr(options)

    assert "analyze=True" in repr_str


def test_explain_options_repr_with_format():
    """Test ExplainOptions.__repr__ with format."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    repr_str = repr(options)

    assert "format='json'" in repr_str


def test_explain_options_repr_with_multiple_options():
    """Test ExplainOptions.__repr__ with multiple options."""
    options = ExplainOptions(analyze=True, verbose=True, costs=True)
    repr_str = repr(options)

    assert "analyze=True" in repr_str
    assert "verbose=True" in repr_str
    assert "costs=True" in repr_str


def test_explain_options_has_slots():
    """Test ExplainOptions uses __slots__ for memory efficiency."""
    options = ExplainOptions()

    assert not hasattr(options, "__dict__")


# -----------------------------------------------------------------------------
# Explain Builder Tests - Basic Construction
# -----------------------------------------------------------------------------


def test_explain_basic_construction_from_string():
    """Test Explain construction from SQL string."""
    explain = Explain("SELECT * FROM users")

    assert explain._statement_sql == "SELECT * FROM users"
    assert explain._dialect is None
    assert explain._options == ExplainOptions()


def test_explain_construction_with_dialect():
    """Test Explain construction with dialect."""
    explain = Explain("SELECT * FROM users", dialect="postgres")

    assert explain._dialect == "postgres"


def test_explain_construction_with_options():
    """Test Explain construction with ExplainOptions."""
    options = ExplainOptions(analyze=True, verbose=True)
    explain = Explain("SELECT * FROM users", options=options)

    assert explain._options == options


def test_explain_construction_from_sql_object():
    """Test Explain construction from SQL object."""
    sql_obj = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    explain = Explain(sql_obj)

    assert "SELECT * FROM users" in explain._statement_sql
    assert explain._parameters == {"id": 1}


def test_explain_construction_from_sqlglot_expression():
    """Test Explain construction from sqlglot expression."""
    expression = exp.Select().select("*").from_("users")
    explain = Explain(expression, dialect="postgres")

    assert "USERS" in explain._statement_sql.upper()
    assert "SELECT" in explain._statement_sql.upper()


def test_explain_construction_from_query_builder():
    """Test Explain construction from QueryBuilder."""
    builder = Select("*").from_("users").where_eq("id", 1)
    explain = Explain(builder)  # pyright: ignore[reportArgumentType]

    assert "USERS" in explain._statement_sql.upper()
    assert "SELECT" in explain._statement_sql.upper()


# -----------------------------------------------------------------------------
# Explain Builder Tests - Method Chaining
# -----------------------------------------------------------------------------


def test_explain_analyze_method_chaining():
    """Test Explain.analyze() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.analyze()

    assert result is explain
    assert explain._options.analyze is True


def test_explain_analyze_disabled():
    """Test Explain.analyze(False) disables analyze."""
    explain = Explain("SELECT * FROM users").analyze(True).analyze(False)

    assert explain._options.analyze is False


def test_explain_verbose_method_chaining():
    """Test Explain.verbose() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.verbose()

    assert result is explain
    assert explain._options.verbose is True


def test_explain_format_method_chaining_with_enum():
    """Test Explain.format() with ExplainFormat enum."""
    explain = Explain("SELECT * FROM users").format(ExplainFormat.JSON)

    assert explain._options.format == ExplainFormat.JSON


def test_explain_format_method_chaining_with_string():
    """Test Explain.format() with string converts to enum."""
    explain = Explain("SELECT * FROM users").format("json")

    assert explain._options.format == ExplainFormat.JSON


def test_explain_costs_method_chaining():
    """Test Explain.costs() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.costs()

    assert result is explain
    assert explain._options.costs is True


def test_explain_buffers_method_chaining():
    """Test Explain.buffers() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.buffers()

    assert result is explain
    assert explain._options.buffers is True


def test_explain_timing_method_chaining():
    """Test Explain.timing() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.timing()

    assert result is explain
    assert explain._options.timing is True


def test_explain_summary_method_chaining():
    """Test Explain.summary() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.summary()

    assert result is explain
    assert explain._options.summary is True


def test_explain_memory_method_chaining():
    """Test Explain.memory() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.memory()

    assert result is explain
    assert explain._options.memory is True


def test_explain_settings_method_chaining():
    """Test Explain.settings() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.settings()

    assert result is explain
    assert explain._options.settings is True


def test_explain_wal_method_chaining():
    """Test Explain.wal() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.wal()

    assert result is explain
    assert explain._options.wal is True


def test_explain_generic_plan_method_chaining():
    """Test Explain.generic_plan() returns self for chaining."""
    explain = Explain("SELECT * FROM users")
    result = explain.generic_plan()

    assert result is explain
    assert explain._options.generic_plan is True


def test_explain_with_options_method():
    """Test Explain.with_options() replaces all options."""
    new_options = ExplainOptions(analyze=True, format=ExplainFormat.JSON)
    explain = Explain("SELECT * FROM users").with_options(new_options)

    assert explain._options is new_options


def test_explain_full_chaining():
    """Test full method chaining on Explain builder."""
    explain = (
        Explain("SELECT * FROM users", dialect="postgres")
        .analyze()
        .verbose()
        .format(ExplainFormat.JSON)
        .costs()
        .buffers()
        .timing()
    )

    assert explain._options.analyze is True
    assert explain._options.verbose is True
    assert explain._options.format == ExplainFormat.JSON
    assert explain._options.costs is True
    assert explain._options.buffers is True
    assert explain._options.timing is True


# -----------------------------------------------------------------------------
# Explain Builder Tests - Properties
# -----------------------------------------------------------------------------


def test_explain_options_property():
    """Test Explain.options property returns current options."""
    options = ExplainOptions(analyze=True)
    explain = Explain("SELECT * FROM users", options=options)

    assert explain.options == options


def test_explain_dialect_property():
    """Test Explain.dialect property returns current dialect."""
    explain = Explain("SELECT * FROM users", dialect="postgres")

    assert explain.dialect == "postgres"


def test_explain_parameters_property_empty():
    """Test Explain.parameters returns empty dict when no parameters."""
    explain = Explain("SELECT * FROM users")

    assert explain.parameters == {}


def test_explain_parameters_property_from_sql_object():
    """Test Explain.parameters returns parameters from SQL object."""
    sql_obj = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    explain = Explain(sql_obj)

    assert explain.parameters == {"id": 1}


def test_explain_parameters_property_is_copy():
    """Test Explain.parameters returns a copy."""
    sql_obj = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    explain = Explain(sql_obj)

    params = explain.parameters
    params["new_key"] = "new_value"

    assert "new_key" not in explain._parameters


# -----------------------------------------------------------------------------
# Explain Builder Tests - Build Methods
# -----------------------------------------------------------------------------


def test_explain_build_returns_sql_object():
    """Test Explain.build() returns SQL object."""
    explain = Explain("SELECT * FROM users")
    result = explain.build()

    assert isinstance(result, SQL)


def test_explain_build_with_dialect_override():
    """Test Explain.build() respects dialect override."""
    explain = Explain("SELECT * FROM users", dialect="postgres")
    result = explain.build(dialect="mysql")

    assert "EXPLAIN" in result.raw_sql.upper()


def test_explain_build_preserves_parameters():
    """Test Explain.build() preserves parameters from SQL object."""
    sql_obj = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    explain = Explain(sql_obj)
    result = explain.build()

    assert result.named_parameters == {"id": 1}


def test_explain_to_sql_returns_string():
    """Test Explain.to_sql() returns SQL string."""
    explain = Explain("SELECT * FROM users")
    result = explain.to_sql()

    assert isinstance(result, str)
    assert "EXPLAIN" in result.upper()


def test_explain_to_sql_with_dialect_override():
    """Test Explain.to_sql() respects dialect override."""
    explain = Explain("SELECT * FROM users", dialect="postgres")
    result_postgres = explain.to_sql()
    result_mysql = explain.to_sql(dialect="mysql")

    assert "EXPLAIN" in result_postgres.upper()
    assert "EXPLAIN" in result_mysql.upper()


def test_explain_repr():
    """Test Explain.__repr__() returns useful string."""
    explain = Explain("SELECT * FROM users", dialect="postgres")
    repr_str = repr(explain)

    assert "Explain" in repr_str
    assert "SELECT * FROM users" in repr_str
    assert "postgres" in repr_str


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - PostgreSQL
# -----------------------------------------------------------------------------


def testbuild_postgres_explain_basic():
    """Test PostgreSQL basic EXPLAIN generation."""
    options = ExplainOptions()
    result = build_postgres_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


def testbuild_postgres_explain_analyze():
    """Test PostgreSQL EXPLAIN ANALYZE generation."""
    options = ExplainOptions(analyze=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "EXPLAIN (ANALYZE)" in result
    assert "SELECT * FROM users" in result


def testbuild_postgres_explain_verbose():
    """Test PostgreSQL EXPLAIN VERBOSE generation."""
    options = ExplainOptions(verbose=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "EXPLAIN (VERBOSE)" in result


def testbuild_postgres_explain_format_json():
    """Test PostgreSQL EXPLAIN FORMAT JSON generation."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "EXPLAIN (FORMAT JSON)" in result


def testbuild_postgres_explain_format_xml():
    """Test PostgreSQL EXPLAIN FORMAT XML generation."""
    options = ExplainOptions(format=ExplainFormat.XML)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "FORMAT XML" in result


def testbuild_postgres_explain_format_yaml():
    """Test PostgreSQL EXPLAIN FORMAT YAML generation."""
    options = ExplainOptions(format=ExplainFormat.YAML)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "FORMAT YAML" in result


def testbuild_postgres_explain_costs_true():
    """Test PostgreSQL EXPLAIN COSTS TRUE generation."""
    options = ExplainOptions(costs=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "COSTS TRUE" in result


def testbuild_postgres_explain_costs_false():
    """Test PostgreSQL EXPLAIN COSTS FALSE generation."""
    options = ExplainOptions(costs=False)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "COSTS FALSE" in result


def testbuild_postgres_explain_buffers():
    """Test PostgreSQL EXPLAIN BUFFERS generation."""
    options = ExplainOptions(buffers=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "BUFFERS TRUE" in result


def testbuild_postgres_explain_timing():
    """Test PostgreSQL EXPLAIN TIMING generation."""
    options = ExplainOptions(timing=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "TIMING TRUE" in result


def testbuild_postgres_explain_summary():
    """Test PostgreSQL EXPLAIN SUMMARY generation."""
    options = ExplainOptions(summary=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "SUMMARY TRUE" in result


def testbuild_postgres_explain_memory():
    """Test PostgreSQL EXPLAIN MEMORY generation."""
    options = ExplainOptions(memory=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "MEMORY TRUE" in result


def testbuild_postgres_explain_settings():
    """Test PostgreSQL EXPLAIN SETTINGS generation."""
    options = ExplainOptions(settings=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "SETTINGS TRUE" in result


def testbuild_postgres_explain_wal():
    """Test PostgreSQL EXPLAIN WAL generation."""
    options = ExplainOptions(wal=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "WAL TRUE" in result


def testbuild_postgres_explain_generic_plan():
    """Test PostgreSQL EXPLAIN GENERIC_PLAN generation."""
    options = ExplainOptions(generic_plan=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "GENERIC_PLAN TRUE" in result


def testbuild_postgres_explain_full_options():
    """Test PostgreSQL EXPLAIN with multiple options."""
    options = ExplainOptions(analyze=True, verbose=True, format=ExplainFormat.JSON, costs=True, buffers=True)
    result = build_postgres_explain("SELECT * FROM users", options)

    assert "EXPLAIN (" in result
    assert "ANALYZE" in result
    assert "VERBOSE" in result
    assert "FORMAT JSON" in result
    assert "COSTS TRUE" in result
    assert "BUFFERS TRUE" in result


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - MySQL
# -----------------------------------------------------------------------------


def testbuild_mysql_explain_basic():
    """Test MySQL basic EXPLAIN generation."""
    options = ExplainOptions()
    result = build_mysql_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


def testbuild_mysql_explain_analyze():
    """Test MySQL EXPLAIN ANALYZE generation."""
    options = ExplainOptions(analyze=True)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN ANALYZE SELECT * FROM users"


def testbuild_mysql_explain_format_json():
    """Test MySQL EXPLAIN FORMAT JSON generation."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert "EXPLAIN FORMAT = JSON" in result


def testbuild_mysql_explain_format_tree():
    """Test MySQL EXPLAIN FORMAT TREE generation."""
    options = ExplainOptions(format=ExplainFormat.TREE)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert "EXPLAIN FORMAT = TREE" in result


def testbuild_mysql_explain_format_traditional():
    """Test MySQL EXPLAIN FORMAT TRADITIONAL generation."""
    options = ExplainOptions(format=ExplainFormat.TRADITIONAL)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert "EXPLAIN FORMAT = TRADITIONAL" in result


def testbuild_mysql_explain_format_text_maps_to_traditional():
    """Test MySQL maps TEXT format to TRADITIONAL."""
    options = ExplainOptions(format=ExplainFormat.TEXT)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert "EXPLAIN FORMAT = TRADITIONAL" in result


def testbuild_mysql_explain_analyze_ignores_format():
    """Test MySQL EXPLAIN ANALYZE ignores format (always uses TREE)."""
    options = ExplainOptions(analyze=True, format=ExplainFormat.JSON)
    result = build_mysql_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN ANALYZE SELECT * FROM users"


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - SQLite
# -----------------------------------------------------------------------------


def testbuild_sqlite_explain_basic():
    """Test SQLite basic EXPLAIN QUERY PLAN generation."""
    options = ExplainOptions()
    result = build_sqlite_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN QUERY PLAN SELECT * FROM users"


def testbuild_sqlite_explain_ignores_analyze():
    """Test SQLite EXPLAIN ignores analyze option."""
    options = ExplainOptions(analyze=True)
    result = build_sqlite_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN QUERY PLAN SELECT * FROM users"


def testbuild_sqlite_explain_ignores_format():
    """Test SQLite EXPLAIN ignores format option."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_sqlite_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN QUERY PLAN SELECT * FROM users"


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - DuckDB
# -----------------------------------------------------------------------------


def testbuild_duckdb_explain_basic():
    """Test DuckDB basic EXPLAIN generation."""
    options = ExplainOptions()
    result = build_duckdb_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


def testbuild_duckdb_explain_analyze():
    """Test DuckDB EXPLAIN ANALYZE generation."""
    options = ExplainOptions(analyze=True)
    result = build_duckdb_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN ANALYZE SELECT * FROM users"


def testbuild_duckdb_explain_format_json():
    """Test DuckDB EXPLAIN FORMAT JSON generation."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_duckdb_explain("SELECT * FROM users", options)

    assert "EXPLAIN (FORMAT JSON)" in result


def testbuild_duckdb_explain_format_text_no_parentheses():
    """Test DuckDB EXPLAIN with TEXT format uses plain EXPLAIN."""
    options = ExplainOptions(format=ExplainFormat.TEXT)
    result = build_duckdb_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - Oracle
# -----------------------------------------------------------------------------


def testbuild_oracle_explain_basic():
    """Test Oracle EXPLAIN PLAN FOR generation."""
    options = ExplainOptions()
    result = build_oracle_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN PLAN FOR SELECT * FROM users"


def testbuild_oracle_explain_ignores_analyze():
    """Test Oracle EXPLAIN PLAN ignores analyze option."""
    options = ExplainOptions(analyze=True)
    result = build_oracle_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN PLAN FOR SELECT * FROM users"


def testbuild_oracle_explain_ignores_format():
    """Test Oracle EXPLAIN PLAN ignores format option."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_oracle_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN PLAN FOR SELECT * FROM users"


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - BigQuery
# -----------------------------------------------------------------------------


def testbuild_bigquery_explain_basic():
    """Test BigQuery basic EXPLAIN generation."""
    options = ExplainOptions()
    result = build_bigquery_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


def testbuild_bigquery_explain_analyze():
    """Test BigQuery EXPLAIN ANALYZE generation."""
    options = ExplainOptions(analyze=True)
    result = build_bigquery_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN ANALYZE SELECT * FROM users"


# -----------------------------------------------------------------------------
# Dialect-Specific SQL Generation Tests - Generic
# -----------------------------------------------------------------------------


def testbuild_generic_explain_basic():
    """Test generic EXPLAIN generation."""
    options = ExplainOptions()
    result = build_generic_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN SELECT * FROM users"


def testbuild_generic_explain_analyze():
    """Test generic EXPLAIN ANALYZE generation."""
    options = ExplainOptions(analyze=True)
    result = build_generic_explain("SELECT * FROM users", options)

    assert result == "EXPLAIN ANALYZE SELECT * FROM users"


# -----------------------------------------------------------------------------
# build_explain_sql Tests
# -----------------------------------------------------------------------------


def test_build_explain_sql_postgres():
    """Test build_explain_sql dispatches to PostgreSQL builder."""
    options = ExplainOptions(analyze=True)
    result = build_explain_sql("SELECT 1", options, dialect="postgres")

    assert "EXPLAIN (ANALYZE)" in result


def test_build_explain_sql_postgresql():
    """Test build_explain_sql handles 'postgresql' dialect."""
    options = ExplainOptions(analyze=True)
    result = build_explain_sql("SELECT 1", options, dialect="postgresql")

    assert "EXPLAIN (ANALYZE)" in result


def test_build_explain_sql_mysql():
    """Test build_explain_sql dispatches to MySQL builder."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_explain_sql("SELECT 1", options, dialect="mysql")

    assert "FORMAT = JSON" in result


def test_build_explain_sql_mariadb():
    """Test build_explain_sql handles 'mariadb' dialect."""
    options = ExplainOptions(analyze=True)
    result = build_explain_sql("SELECT 1", options, dialect="mariadb")

    assert "EXPLAIN ANALYZE" in result


def test_build_explain_sql_sqlite():
    """Test build_explain_sql dispatches to SQLite builder."""
    options = ExplainOptions()
    result = build_explain_sql("SELECT 1", options, dialect="sqlite")

    assert "EXPLAIN QUERY PLAN" in result


def test_build_explain_sql_duckdb():
    """Test build_explain_sql dispatches to DuckDB builder."""
    options = ExplainOptions(format=ExplainFormat.JSON)
    result = build_explain_sql("SELECT 1", options, dialect="duckdb")

    assert "FORMAT JSON" in result


def test_build_explain_sql_oracle():
    """Test build_explain_sql dispatches to Oracle builder."""
    options = ExplainOptions()
    result = build_explain_sql("SELECT 1", options, dialect="oracle")

    assert "EXPLAIN PLAN FOR" in result


def test_build_explain_sql_bigquery():
    """Test build_explain_sql dispatches to BigQuery builder."""
    options = ExplainOptions(analyze=True)
    result = build_explain_sql("SELECT 1", options, dialect="bigquery")

    assert "EXPLAIN ANALYZE" in result


def test_build_explain_sql_unknown_dialect():
    """Test build_explain_sql uses generic builder for unknown dialect."""
    options = ExplainOptions(analyze=True)
    result = build_explain_sql("SELECT 1", options, dialect="unknown_db")

    assert "EXPLAIN ANALYZE" in result


def test_build_explain_sql_none_dialect():
    """Test build_explain_sql uses generic builder when dialect is None."""
    options = ExplainOptions()
    result = build_explain_sql("SELECT 1", options, dialect=None)

    assert result == "EXPLAIN SELECT 1"


# -----------------------------------------------------------------------------
# normalize_dialect_name Tests
# -----------------------------------------------------------------------------


def testnormalize_dialect_name_none():
    """Test normalize_dialect_name with None."""
    assert normalize_dialect_name(None) is None


def testnormalize_dialect_name_lowercase_string():
    """Test normalize_dialect_name with lowercase string."""
    assert normalize_dialect_name("postgres") == "postgres"


def testnormalize_dialect_name_uppercase_string():
    """Test normalize_dialect_name converts to lowercase."""
    assert normalize_dialect_name("POSTGRES") == "postgres"


def testnormalize_dialect_name_mixed_case_string():
    """Test normalize_dialect_name converts mixed case to lowercase."""
    assert normalize_dialect_name("PostgreSQL") == "postgresql"


# -----------------------------------------------------------------------------
# QueryBuilder Integration Tests - ExplainMixin
# -----------------------------------------------------------------------------


def test_select_has_explain_method():
    """Test Select has explain() method from ExplainMixin."""
    query = Select("*").from_("users")

    assert hasattr(query, "explain")
    assert callable(query.explain)


def test_insert_has_explain_method():
    """Test Insert has explain() method from ExplainMixin."""
    query = Insert().into("users")

    assert hasattr(query, "explain")
    assert callable(query.explain)


def test_update_has_explain_method():
    """Test Update has explain() method from ExplainMixin."""
    query = Update("users")

    assert hasattr(query, "explain")
    assert callable(query.explain)


def test_delete_has_explain_method():
    """Test Delete has explain() method from ExplainMixin."""
    query = Delete("users")

    assert hasattr(query, "explain")
    assert callable(query.explain)


def test_merge_has_explain_method():
    """Test Merge has explain() method from ExplainMixin."""
    query = Merge("products")

    assert hasattr(query, "explain")
    assert callable(query.explain)


def test_select_explain_returns_explain_builder():
    """Test Select.explain() returns Explain builder."""
    query = Select("*").from_("users")
    explain = query.explain()

    assert isinstance(explain, Explain)


def test_select_explain_with_options():
    """Test Select.explain() accepts options."""
    query = Select("*").from_("users")
    explain = query.explain(analyze=True, verbose=True, format="json")

    assert explain._options.analyze is True
    assert explain._options.verbose is True
    assert explain._options.format == ExplainFormat.JSON


def test_select_explain_preserves_parameters():
    """Test Select.explain() preserves query parameters."""
    query = Select("*").from_("users").where_eq("id", 1)
    explain = query.explain()

    assert explain._parameters.get("id") == 1


def test_select_explain_build_generates_sql():
    """Test Select.explain().build() generates EXPLAIN SQL."""
    query = Select("*", dialect="postgres").from_("users")
    explain_sql = query.explain().build()

    assert "EXPLAIN" in explain_sql.raw_sql.upper()
    assert "SELECT" in explain_sql.raw_sql.upper()


def test_insert_explain_build_generates_sql():
    """Test Insert.explain().build() generates EXPLAIN SQL."""
    query = Insert(dialect="postgres").into("users").columns("name", "email").values("John", "john@example.com")
    explain_sql = query.explain().build()

    assert "EXPLAIN" in explain_sql.raw_sql.upper()
    assert "INSERT" in explain_sql.raw_sql.upper()


def test_update_explain_build_generates_sql():
    """Test Update.explain().build() generates EXPLAIN SQL."""
    query = Update("users", dialect="postgres").set("name", "John").where_eq("id", 1)
    explain_sql = query.explain().build()

    assert "EXPLAIN" in explain_sql.raw_sql.upper()
    assert "UPDATE" in explain_sql.raw_sql.upper()


def test_delete_explain_build_generates_sql():
    """Test Delete.explain().build() generates EXPLAIN SQL."""
    query = Delete("users", dialect="postgres").where_eq("id", 1)
    explain_sql = query.explain().build()

    assert "EXPLAIN" in explain_sql.raw_sql.upper()
    assert "DELETE" in explain_sql.raw_sql.upper()


def test_select_explain_with_dialect_postgres():
    """Test Select.explain() uses query dialect for PostgreSQL."""
    query = Select("*", dialect="postgres").from_("users")
    explain_sql = query.explain(analyze=True).build()

    assert "EXPLAIN (ANALYZE)" in explain_sql.raw_sql


def test_select_explain_with_dialect_mysql():
    """Test Select.explain() uses query dialect for MySQL."""
    query = Select("*", dialect="mysql").from_("users")
    explain_sql = query.explain(format="json").build()

    assert "FORMAT = JSON" in explain_sql.raw_sql


# -----------------------------------------------------------------------------
# SQLFactory Integration Tests
# -----------------------------------------------------------------------------


def test_sqlfactory_explain_with_string():
    """Test SQLFactory.explain() with SQL string."""
    factory = SQLFactory(dialect="postgres")
    explain = factory.explain("SELECT * FROM users")

    assert isinstance(explain, Explain)


def test_sqlfactory_explain_with_options():
    """Test SQLFactory.explain() with options."""
    factory = SQLFactory(dialect="postgres")
    explain = factory.explain("SELECT * FROM users", analyze=True, verbose=True, format="json")

    assert explain._options.analyze is True
    assert explain._options.verbose is True
    assert explain._options.format == ExplainFormat.JSON


def test_sqlfactory_explain_with_query_builder():
    """Test SQLFactory.explain() with QueryBuilder."""
    factory = SQLFactory(dialect="postgres")
    query = factory.select("*").from_("users")
    explain = factory.explain(query, analyze=True)  # pyright: ignore[reportArgumentType]

    assert isinstance(explain, Explain)


def test_sqlfactory_explain_with_sql_object():
    """Test SQLFactory.explain() with SQL object."""
    factory = SQLFactory(dialect="postgres")
    sql_obj = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    explain = factory.explain(sql_obj, analyze=True)
    result = explain.build()

    assert result.named_parameters == {"id": 1}


def test_sqlfactory_explain_with_dialect_override():
    """Test SQLFactory.explain() with dialect override."""
    factory = SQLFactory(dialect="postgres")
    explain = factory.explain("SELECT * FROM users", dialect="mysql")

    assert explain._dialect == "mysql"


def test_sqlfactory_explain_uses_factory_dialect():
    """Test SQLFactory.explain() uses factory dialect when not overridden."""
    factory = SQLFactory(dialect="postgres")
    explain = factory.explain("SELECT * FROM users")

    assert explain._dialect == "postgres"


def test_sql_global_instance_explain():
    """Test global sql instance has explain() method."""
    explain = sql.explain("SELECT * FROM users")

    assert isinstance(explain, Explain)


def test_sql_global_instance_explain_with_builder():
    """Test global sql instance explain() with builder."""
    query = sql.select("*").from_("users")
    explain = sql.explain(query, analyze=True)  # pyright: ignore[reportArgumentType]
    result = explain.build()

    assert "EXPLAIN" in result.raw_sql.upper()


# -----------------------------------------------------------------------------
# SQL Class Integration Tests
# -----------------------------------------------------------------------------


def test_sql_class_explain_method():
    """Test SQL class has explain() method."""
    stmt = SQL("SELECT * FROM users")

    assert hasattr(stmt, "explain")
    assert callable(stmt.explain)


def test_sql_class_explain_returns_sql_object():
    """Test SQL.explain() returns SQL object."""
    stmt = SQL("SELECT * FROM users")
    result = stmt.explain()

    assert isinstance(result, SQL)


def test_sql_class_explain_with_analyze():
    """Test SQL.explain() with analyze=True."""
    stmt = SQL("SELECT * FROM users", statement_config=None)
    result = stmt.explain(analyze=True)

    assert "EXPLAIN" in result.raw_sql.upper()


def test_sql_class_explain_with_verbose():
    """Test SQL.explain() with verbose=True."""
    stmt = SQL("SELECT * FROM users")
    result = stmt.explain(verbose=True)

    assert "EXPLAIN" in result.raw_sql.upper()


def test_sql_class_explain_with_format():
    """Test SQL.explain() with format."""
    stmt = SQL("SELECT * FROM users")
    result = stmt.explain(format="json")

    assert "EXPLAIN" in result.raw_sql.upper()


def test_sql_class_explain_preserves_parameters():
    """Test SQL.explain() preserves parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
    result = stmt.explain()

    assert result.named_parameters == {"id": 1}


def test_sql_class_explain_with_dialect():
    """Test SQL.explain() respects statement dialect."""

    config = StatementConfig(dialect="postgres")
    stmt = SQL("SELECT * FROM users", statement_config=config)
    result = stmt.explain(analyze=True)

    assert "EXPLAIN" in result.raw_sql.upper()


# -----------------------------------------------------------------------------
# Edge Cases and Error Handling Tests
# -----------------------------------------------------------------------------


def test_explain_empty_parameters_dict():
    """Test Explain handles empty parameters dict."""
    sql_obj = SQL("SELECT * FROM users", {})
    explain = Explain(sql_obj)
    result = explain.build()

    assert result.named_parameters == {}


def test_explain_complex_sql_with_joins():
    """Test Explain handles complex SQL with joins."""
    sql_str = """
    SELECT u.name, o.order_id
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.active = true
    """
    explain = Explain(sql_str, dialect="postgres")
    result = explain.analyze().build()

    assert "EXPLAIN (ANALYZE)" in result.raw_sql


def test_explain_with_cte():
    """Test Explain handles SQL with CTEs."""
    sql_str = """
    WITH active_users AS (SELECT * FROM users WHERE active = true)
    SELECT * FROM active_users
    """
    explain = Explain(sql_str, dialect="postgres")
    result = explain.build()

    assert "EXPLAIN" in result.raw_sql.upper()


def test_explain_builder_dialect_case_insensitive():
    """Test Explain handles mixed case dialect names."""
    explain = Explain("SELECT 1", dialect="PostgreSQL")
    result = explain.to_sql()

    assert "EXPLAIN" in result.upper()


def test_explain_options_immutable_through_copy():
    """Test ExplainOptions.copy() does not modify original."""
    original = ExplainOptions(analyze=True)
    _ = original.copy(verbose=True)

    assert original.verbose is False


def test_explain_builder_from_select_with_subquery():
    """Test Explain handles Select with subquery."""
    subquery = Select("id").from_("users").where("active = true")
    main_query = Select("*").from_(subquery, alias="active_users")
    explain = main_query.explain()
    result = explain.build()

    assert "EXPLAIN" in result.raw_sql.upper()


def test_explain_postgres_all_false_booleans():
    """Test PostgreSQL EXPLAIN with all boolean options set to False."""
    options = ExplainOptions(costs=False, buffers=False, timing=False)
    result = build_postgres_explain("SELECT 1", options)

    assert "COSTS FALSE" in result
    assert "BUFFERS FALSE" in result
    assert "TIMING FALSE" in result


def test_explain_builder_has_slots():
    """Test Explain uses __slots__ for memory efficiency."""
    explain = Explain("SELECT 1")

    assert hasattr(type(explain), "__slots__")


def testnormalize_dialect_name_with_dialect_object():
    """Test normalize_dialect_name with dialect instance (non-string)."""
    from sqlglot.dialects import postgres

    dialect_instance = postgres.Postgres()
    result = normalize_dialect_name(dialect_instance)

    assert result == "postgres"


def test_explain_construction_invalid_statement_raises():
    """Test Explain raises SQLBuilderError for unsupported statement type."""

    class UnsupportedType:
        pass

    unsupported = UnsupportedType()

    with pytest.raises(SQLBuilderError, match="Cannot resolve statement to SQL"):
        Explain(unsupported)  # pyright: ignore[reportArgumentType]


def test_explain_construction_with_has_expression_and_sql():
    """Test Explain construction from object with expression and sql attributes."""
    from sqlglot import exp as glot_exp

    class MockSQLObject:
        expression = glot_exp.Select()
        sql = "SELECT * FROM mock_table"

    mock_obj = MockSQLObject()
    explain = Explain(mock_obj)  # pyright: ignore[reportArgumentType]

    assert "SELECT * FROM mock_table" in explain._statement_sql
