"""Test the new builder implementations."""


from sqlspec.statement_new.builder import Delete, Insert, Merge, Select, Update


def test_select_builder() -> None:
    """Test SELECT builder basics."""
    # Basic select
    select = Select("id", "name").from_("users")
    result = select.build()
    assert "SELECT" in result.sql
    assert "FROM users" in result.sql

    # With WHERE clause
    select_where = Select("*").from_("users").where("age > 18")
    result_where = select_where.build()
    assert "WHERE" in result_where.sql

    # With parameters
    select_params = Select("*").from_("users").where_eq("name", "John")
    assert "John" in select_params.parameters.values()


def test_insert_builder() -> None:
    """Test INSERT builder basics."""
    # Basic insert with values
    insert = Insert("users").values({"name": "John", "age": 30})
    result = insert.build()
    assert "INSERT INTO users" in result.sql
    assert "John" in result.parameters.values()
    assert 30 in result.parameters.values()


def test_update_builder() -> None:
    """Test UPDATE builder basics."""
    # Basic update
    update = Update("users").set("name", "Jane").where_eq("id", 1)
    result = update.build()
    assert "UPDATE users" in result.sql
    assert "SET" in result.sql
    assert "WHERE" in result.sql
    assert "Jane" in result.parameters.values()
    assert 1 in result.parameters.values()


def test_delete_builder() -> None:
    """Test DELETE builder basics."""
    # Basic delete
    delete = Delete("users").where_eq("id", 1)
    result = delete.build()
    assert "DELETE FROM users" in result.sql
    assert "WHERE" in result.sql
    assert 1 in result.parameters.values()


def test_merge_builder() -> None:
    """Test MERGE builder basics."""
    # Basic merge
    merge = (
        Merge()
        .into("target_table")
        .using("source_table", "s")
        .on("target_table.id = s.id")
        .when_matched_then_update({"name": "updated"})
    )
    result = merge.build()
    assert "MERGE" in result.sql
    assert "INTO" in result.sql
    assert "USING" in result.sql
    assert "ON" in result.sql
    assert "WHEN MATCHED" in result.sql
    assert "updated" in result.parameters.values()
