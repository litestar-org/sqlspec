from typing import Any, cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder import Values
from sqlspec.exceptions import SQLBuilderError


def test_values_renders_with_parameters() -> None:
    """Test that sql.values() produces a parameterized VALUES expression."""
    query = sql.values([(1, "a"), (2, "b")], alias="v", columns=["id", "name"])
    stmt = query.build()

    assert stmt.parameters == {"id": 1, "name": "a", "id_1": 2, "name_1": "b"}
    assert "VALUES" in stmt.sql
    assert "v" in stmt.sql
    assert "id" in stmt.sql
    assert "name" in stmt.sql


def test_values_renders_dialect_parameters() -> None:
    """Test that sql.values() formats dialect-specific parameter placeholders."""
    query = sql.values([(1, "a"), (2, "b")], alias="v", columns=["id", "name"])
    stmt_pg = query.build(dialect="postgres")

    assert "%(id)s" in stmt_pg.sql
    assert "%(name)s" in stmt_pg.sql
    assert "%(id_1)s" in stmt_pg.sql
    assert "%(name_1)s" in stmt_pg.sql


def test_values_as_cte_on_select() -> None:
    """Test using sql.values() as a Common Table Expression on a SELECT query."""
    val = sql.values([(1, "alice"), (2, "bob")], columns=["id", "name"])
    query = sql.select("*").from_("v").with_cte("v", val)
    stmt = query.build(dialect="postgres")

    assert "WITH" in stmt.sql
    assert "v" in stmt.sql
    assert "VALUES" in stmt.sql
    assert "SELECT" in stmt.sql
    assert stmt.parameters.get("v_id") == 1
    assert stmt.parameters.get("v_name") == "alice"
    assert stmt.parameters.get("v_id_1") == 2
    assert stmt.parameters.get("v_name_1") == "bob"


def test_values_as_cte_on_update() -> None:
    """Test using sql.values() as a Common Table Expression on an UPDATE query."""
    val = sql.values([(1, "active"), (2, "inactive")], alias="v", columns=["id", "status"])
    query = sql.update("users").with_cte("v", val).set(status="v.status").where("users.id = v.id")
    stmt = query.build(dialect="postgres")

    assert "WITH" in stmt.sql
    assert "v" in stmt.sql
    assert "UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert stmt.parameters.get("v_id") == 1
    assert stmt.parameters.get("v_status") == "active"
    assert stmt.parameters.get("v_id_1") == 2
    assert stmt.parameters.get("v_status_1") == "inactive"


def test_values_as_update_from_source() -> None:
    """Test using sql.values() as an UPDATE ... FROM source table expression."""
    val = sql.values([(1, "alice"), (2, "bob")], alias="v", columns=["id", "name"])
    query = sql.update("users").set(name="v.name").from_(val).where("users.id = v.id")
    stmt = query.build(dialect="postgres")

    assert "UPDATE" in stmt.sql
    assert "FROM (VALUES" in stmt.sql
    assert 'AS "v"' in stmt.sql or "AS v" in stmt.sql
    assert "WHERE" in stmt.sql
    assert stmt.parameters.get("v_id") == 1
    assert stmt.parameters.get("v_name") == "alice"
    assert stmt.parameters.get("v_id_1") == 2
    assert stmt.parameters.get("v_name_1") == "bob"


def test_values_from_dict_rows() -> None:
    """Test creating sql.values() from mapping/dict rows."""
    query = sql.values([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], alias="v")
    stmt = query.build()

    assert stmt.parameters == {"id": 1, "name": "a", "id_1": 2, "name_1": "b"}
    assert "VALUES" in stmt.sql
    assert "v" in stmt.sql


def test_values_empty_rows_raises() -> None:
    """Test that empty row input raises SQLBuilderError."""
    with pytest.raises(SQLBuilderError, match=r"(?i)at least one row"):
        sql.values([])

    with pytest.raises(SQLBuilderError, match=r"(?i)at least one row"):
        Values([])


def test_values_ragged_rows_raises() -> None:
    """Test that non-uniform row lengths raise SQLBuilderError."""
    with pytest.raises(SQLBuilderError, match=r"(?i)same number of columns"):
        sql.values([(1, "a"), (2, "b", "extra")])


def test_values_column_count_mismatch_raises() -> None:
    """Test that column count mismatch with row width raises SQLBuilderError."""
    with pytest.raises(SQLBuilderError, match=r"(?i)does not match"):
        sql.values([(1, "a")], columns=["id"])


def test_column_named_values_still_works() -> None:
    """Test that referencing a column named values via sql.column still works."""
    col = sql.column("values")
    assert col.name == "values"

    query = sql.select(col).from_("events")
    stmt = query.build()
    assert "values" in stmt.sql


def test_values_as_and_set_columns() -> None:
    """Test as_ and set_columns methods on Values builder."""
    val = Values([(1, "a")]).as_("my_alias").set_columns("col1", "col2")
    assert val.alias_name == "my_alias"
    assert val.columns == ["col1", "col2"]

    with pytest.raises(SQLBuilderError, match=r"(?i)does not match"):
        val.set_columns("only_one")


def test_values_alias_without_columns() -> None:
    """Test Values with alias but without column list."""
    val = Values([(1, "a")], alias="v")
    stmt = val.build()
    assert 'AS "v"' in stmt.sql or "AS v" in stmt.sql


def test_values_build_empty_raises() -> None:
    """Test building an empty Values instance raises SQLBuilderError."""
    val = Values()
    with pytest.raises(SQLBuilderError, match=r"(?i)at least one row"):
        val.build()


def test_values_with_sqlglot_expressions() -> None:
    """Test Values containing SQLGlot expressions."""
    from sqlglot import exp

    val = Values([(exp.convert(1), "text")])
    stmt = val.build()
    assert "1" in stmt.sql


def test_values_add_rows_validation_errors() -> None:
    """Test various validation failure branches in add_rows."""
    with pytest.raises(SQLBuilderError, match=r"(?i)not a mapping"):
        Values([{"a": 1}, (2,)])

    with pytest.raises(SQLBuilderError, match=r"(?i)same keys"):
        Values([{"a": 1}, {"b": 2}])

    with pytest.raises(SQLBuilderError, match=r"(?i)must be a sequence"):
        Values(cast(Any, [(1, 2), 3]))

    with pytest.raises(SQLBuilderError, match=r"(?i)at least one column"):
        Values([()])

    with pytest.raises(SQLBuilderError, match=r"(?i)must be sequences or mappings"):
        Values(cast(Any, [1, 2]))


def test_values_rejects_row_width_changes_between_calls() -> None:
    values = sql.values([(1, 2)])
    with pytest.raises(SQLBuilderError, match="same number of columns"):
        values.add_rows([(3,)])
    assert list(values.build().parameters.values()) == [1, 2]


def test_values_rejects_empty_mapping() -> None:
    with pytest.raises(SQLBuilderError, match="at least one column"):
        sql.values([{}])


def test_values_rebuild_preserves_cte_parameters() -> None:
    query = sql.values([(1,)], columns=["id"]).with_cte("c", sql.select("id").from_("t").where_eq("id", 2))
    query.add_rows([(3,)])
    assert sorted(query.build().parameters.values()) == [1, 2, 3]


def test_values_accepts_general_row_sequences() -> None:
    assert list(sql.values([range(2)]).build().parameters.values()) == [0, 1]


def test_values_invalid_mapping_does_not_change_column_state() -> None:
    query = Values()
    with pytest.raises(SQLBuilderError, match="same keys"):
        query.add_rows([{"id": 1}, {"other": 2}])
    query.add_rows([{"other": 3}])
    assert query.columns == ["other"]
    assert list(query.build().parameters.values()) == [3]


def test_raw_values_cte_moves_alias_columns_without_mutation() -> None:
    values = exp.values([(1,)], alias="old", columns=["id"])
    query = sql.select("id").from_("v").with_cte("v", values)
    expression = query._build_final_expression(copy=True)
    cte = expression.args["with_"].expressions[0]
    assert cte.this.args.get("alias") is None
    assert [column.name for column in cte.args["alias"].columns] == ["id"]
    assert values.alias == "old"


def test_select_from_values_preserves_attached_cte() -> None:
    source = sql.select(exp.Literal.number(1).as_("id"))
    values = sql.values([(exp.Subquery(this=exp.select("id").from_("c")),)], columns=["id"])
    values.with_cte("c", source)
    query = sql.select("*").from_(values, alias="v")
    assert "WITH" in query.build(dialect="postgres").sql
    assert "WITH" in values.build(dialect="postgres").sql


@pytest.mark.parametrize("source_alias", [None, "original"])
def test_select_from_values_replaces_alias_and_preserves_columns(source_alias: str | None) -> None:
    source = sql.values([(1, "alice")], alias=source_alias, columns=["id", "name"])
    query = sql.select("renamed.id", "renamed.name").from_(source, alias="renamed")
    expression = query._build_final_expression(copy=True)
    from_source = expression.args["from_"].this

    assert isinstance(from_source, exp.Values)
    assert from_source.alias == "renamed"
    assert [column.name for column in from_source.args["alias"].columns] == ["id", "name"]
    assert source.alias_name == source_alias
    assert query.build(dialect="postgres").parameters == {"renamed_id": 1, "renamed_name": "alice"}
