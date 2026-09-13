import pytest

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
    query = (
        sql.update("users")
        .with_cte("v", val)
        .set(status="v.status")
        .where("users.id = v.id")
    )
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
    query = (
        sql.update("users")
        .set(name="v.name")
        .from_(val)
        .where("users.id = v.id")
    )
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
