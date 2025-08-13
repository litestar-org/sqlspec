"""Unit tests for INSERT builder functionality including ON CONFLICT operations."""

import pytest

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


def test_insert_basic_functionality() -> None:
    """Test basic INSERT builder functionality."""
    query = sql.insert("users").columns("name", "email").values("John", "john@test.com")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert '"users"' in stmt.sql or "users" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["email"] == "john@test.com"


def test_insert_with_table_in_constructor() -> None:
    """Test INSERT with table specified in constructor."""
    query = sql.insert("products").values(name="Widget", price=29.99)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "products" in stmt.sql
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters


def test_insert_values_from_dict() -> None:
    """Test INSERT using values_from_dict method."""
    data = {"id": 1, "name": "John", "status": "active"}
    query = sql.insert("users").values_from_dict(data)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert len(stmt.parameters) == 3
    assert stmt.parameters["id"] == 1
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["status"] == "active"


def test_insert_values_from_dicts_multiple_rows() -> None:
    """Test INSERT using values_from_dicts for multiple rows."""
    data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}, {"id": 3, "name": "Bob"}]
    query = sql.insert("users").values_from_dicts(data)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    # Should have parameters for all rows
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "id_1" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert "id_2" in stmt.parameters
    assert "name_2" in stmt.parameters


def test_insert_with_kwargs() -> None:
    """Test INSERT using kwargs in values method."""
    query = sql.insert("products").values(name="Widget", price=29.99, in_stock=True)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters
    assert "in_stock" in stmt.parameters
    assert stmt.parameters["name"] == "Widget"
    assert stmt.parameters["price"] == 29.99
    assert stmt.parameters["in_stock"] is True


def test_insert_mixed_args_kwargs_error() -> None:
    """Test that mixing positional and keyword arguments raises error."""
    with pytest.raises(SQLBuilderError, match="Cannot mix positional values with keyword values"):
        sql.insert("users").values("John", email="john@test.com")


def test_insert_multiple_values_calls() -> None:
    """Test multiple calls to values() method for multi-row insert."""
    query = sql.insert("users").columns("name", "email").values("John", "john@test.com").values("Jane", "jane@test.com")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    # Should have parameters for both rows
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert "email_1" in stmt.parameters


def test_insert_with_returning() -> None:
    """Test INSERT with RETURNING clause."""
    query = sql.insert("users").values(name="John").returning("id", "created_at")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "RETURNING" in stmt.sql
    assert "id" in stmt.sql
    assert "created_at" in stmt.sql


def test_insert_without_table_error() -> None:
    """Test that values() without table raises error."""
    with pytest.raises(SQLBuilderError, match="The target table must be set"):
        sql.insert().values(name="John")


def test_insert_values_columns_mismatch_error() -> None:
    """Test that mismatched columns and values raises error."""
    with pytest.raises(SQLBuilderError, match="Number of values"):
        sql.insert("users").columns("name", "email").values("John")  # Missing email value


def test_insert_columns_and_values_consistency() -> None:
    """Test that columns and values are consistent."""
    query = sql.insert("users").columns("name", "email", "age").values("John", "john@test.com", 25)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert len(stmt.parameters) == 3
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["email"] == "john@test.com"
    assert stmt.parameters["age"] == 25


def test_insert_inconsistent_dict_keys_error() -> None:
    """Test that inconsistent dictionary keys in values_from_dicts raises error."""
    data = [
        {"id": 1, "name": "John"},
        {"id": 2, "email": "jane@test.com"},  # Missing name, has email instead
    ]
    with pytest.raises(SQLBuilderError, match="do not match expected keys"):
        sql.insert("users").values_from_dicts(data).build()


def test_insert_with_sql_raw_expressions() -> None:
    """Test INSERT with sql.raw expressions."""
    query = sql.insert("logs").values(message="Test message", created_at=sql.raw("NOW()"), uuid=sql.raw("UUID()"))
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "UUID()" in stmt.sql
    assert "message" in stmt.parameters
    assert stmt.parameters["message"] == "Test message"


def test_insert_with_sql_raw_parameters() -> None:
    """Test INSERT with sql.raw that has parameters."""
    query = sql.insert("users").values(
        name="John", computed_field=sql.raw("COALESCE(:fallback, 'default')", fallback="custom")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "COALESCE" in stmt.sql
    assert "name" in stmt.parameters
    assert "fallback" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["fallback"] == "custom"


# ON CONFLICT functionality tests
def test_on_conflict_do_nothing_basic() -> None:
    """Test basic ON CONFLICT DO NOTHING."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_nothing()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    assert "id" in stmt.parameters


def test_on_conflict_do_update_basic() -> None:
    """Test basic ON CONFLICT DO UPDATE."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update(name="Updated")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"


def test_on_conflict_multiple_columns() -> None:
    """Test ON CONFLICT with multiple columns."""
    query = (
        sql.insert("users")
        .values(email="john@test.com", username="john", name="John")
        .on_conflict("email", "username")
        .do_nothing()
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "email" in stmt.sql
    assert "username" in stmt.sql


def test_on_conflict_no_columns() -> None:
    """Test ON CONFLICT without specific columns."""
    query = sql.insert("users").values(id=1, name="John").on_conflict().do_nothing()
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    # Should not specify columns
    assert "ON CONFLICT(" not in stmt.sql


def test_on_conflict_do_update_with_sql_raw() -> None:
    """Test ON CONFLICT DO UPDATE with sql.raw expressions."""
    query = (
        sql.insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(updated_at=sql.raw("NOW()"), name="Updated")
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "name_1" in stmt.parameters


def test_on_conflict_convenience_method() -> None:
    """Test on_conflict_do_nothing convenience method."""
    query = sql.insert("users").values(id=1, name="John").on_conflict_do_nothing("id")
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql


def test_legacy_on_duplicate_key_update() -> None:
    """Test legacy on_duplicate_key_update method."""
    query = (
        sql.insert("users")
        .values(id=1, name="John")
        .on_duplicate_key_update(name="Updated", updated_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql


def test_on_conflict_chaining() -> None:
    """Test ON CONFLICT method chaining."""
    query = (
        sql.insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name="Updated")
        .returning("id", "name")
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "RETURNING" in stmt.sql


def test_on_conflict_type_safety() -> None:
    """Test ON CONFLICT method return types for chaining."""
    insert_builder = sql.insert("users").values(id=1, name="John")

    # on_conflict should return ConflictBuilder
    conflict_builder = insert_builder.on_conflict("id")
    assert hasattr(conflict_builder, "do_nothing")
    assert hasattr(conflict_builder, "do_update")

    # do_nothing should return Insert for further chaining
    final_builder = conflict_builder.do_nothing()
    assert hasattr(final_builder, "returning")
    assert hasattr(final_builder, "build")


def test_on_conflict_empty_do_update() -> None:
    """Test ON CONFLICT DO UPDATE with no arguments."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update()
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql


def test_on_conflict_parameter_merging() -> None:
    """Test that ON CONFLICT properly merges parameters from SQL objects."""
    query = (
        sql.insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name=sql.raw("COALESCE(:new_name, name)", new_name="Updated"), updated_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "new_name" in stmt.parameters
    assert stmt.parameters["new_name"] == "Updated"
    assert "NOW()" in stmt.sql


def test_on_conflict_with_values_from_dict() -> None:
    """Test ON CONFLICT with values_from_dict."""
    data = {"id": 1, "name": "John", "email": "john@test.com"}
    query = sql.insert("users").values_from_dict(data).on_conflict("id").do_update(name="Updated")
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"
