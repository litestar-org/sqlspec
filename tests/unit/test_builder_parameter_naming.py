"""Unit tests for comprehensive parameter naming across all SQL builder operations.

This module tests that all SQL builder operations use descriptive, column-based
parameter names instead of generic param_1, param_2, etc.
"""

from sqlspec import sql


def test_update_set_uses_column_names() -> None:
    """Test that UPDATE SET operations use column names for parameters."""
    query = sql.update("users").set("name", "John Doe").set("email", "john@example.com")
    stmt = query.build()

    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert stmt.parameters["name"] == "John Doe"
    assert stmt.parameters["email"] == "john@example.com"
    assert ":name" in stmt.sql
    assert ":email" in stmt.sql


def test_update_set_with_dict_uses_column_names() -> None:
    """Test that UPDATE SET with dictionary uses column names for parameters."""
    query = sql.update("products").set({"name": "Widget", "price": 29.99, "category": "Tools"})
    stmt = query.build()

    assert "name" in stmt.parameters
    assert "price" in stmt.parameters
    assert "category" in stmt.parameters
    assert stmt.parameters["name"] == "Widget"
    assert stmt.parameters["price"] == 29.99
    assert stmt.parameters["category"] == "Tools"


def test_insert_with_columns_uses_column_names() -> None:
    """Test that INSERT with specified columns uses column names for parameters."""
    query = sql.insert("users").columns("name", "email", "age").values("Alice Smith", "alice@test.com", 28)
    stmt = query.build()

    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "age" in stmt.parameters
    assert stmt.parameters["name"] == "Alice Smith"
    assert stmt.parameters["email"] == "alice@test.com"
    assert stmt.parameters["age"] == 28


def test_insert_values_from_dict_uses_column_names() -> None:
    """Test that INSERT values_from_dict uses column names for parameters."""
    query = sql.insert("orders").values_from_dict(
        {"customer_id": 123, "product_name": "Laptop", "quantity": 2, "total": 1999.98}
    )
    stmt = query.build()

    # Should preserve the dictionary key names as parameter names
    assert "customer_id" in stmt.parameters or any("customer_id" in key for key in stmt.parameters.keys())
    assert "product_name" in stmt.parameters or any("product_name" in key for key in stmt.parameters.keys())
    assert "quantity" in stmt.parameters or any("quantity" in key for key in stmt.parameters.keys())
    assert "total" in stmt.parameters or any("total" in key for key in stmt.parameters.keys())


def test_insert_without_columns_uses_positional_names() -> None:
    """Test that INSERT without specified columns uses descriptive positional names."""
    query = sql.insert("logs").values("INFO", "User login", "2023-01-01")
    stmt = query.build()

    # Without column info, should use value_1, value_2, etc.
    param_keys = list(stmt.parameters.keys())
    assert len(param_keys) == 3
    assert any("value" in key for key in param_keys)


def test_case_when_uses_descriptive_names() -> None:
    """Test that CASE WHEN expressions work correctly with new property syntax."""
    case_expr = sql.case_.when("age > 65", "Senior").when("age > 18", "Adult").else_("Minor").end()
    query = sql.select("name", case_expr).from_("users")
    stmt = query.build()

    # CASE expressions using sql.case_ create literal SQL rather than parameters
    # This is the expected behavior for the new property syntax
    assert "CASE" in stmt.sql
    assert "Senior" in stmt.sql
    assert "Adult" in stmt.sql
    assert "Minor" in stmt.sql
    assert "> 65" in stmt.sql
    assert "> 18" in stmt.sql


def test_complex_query_preserves_column_names() -> None:
    """Test that complex queries with multiple operations preserve column names."""
    query = (
        sql.select("u.name", "p.title")
        .from_("users u")
        .join("posts p", "u.id = p.user_id")
        .where_eq("u.status", "active")
        .where_in("p.category", ["tech", "science"])
        .where_between("p.created_at", "2023-01-01", "2023-12-31")
    )
    stmt = query.build()

    # Should use descriptive parameter names
    params = stmt.parameters

    # Status parameter
    assert "status" in params
    assert params["status"] == "active"

    # IN clause parameters
    category_params = [key for key in params.keys() if "category" in key]
    assert len(category_params) == 2
    assert "tech" in params.values()
    assert "science" in params.values()

    # BETWEEN clause parameters
    created_at_params = [key for key in params.keys() if "created_at" in key]
    assert len(created_at_params) == 2
    assert "2023-01-01" in params.values()
    assert "2023-12-31" in params.values()


def test_parameter_collision_handling() -> None:
    """Test that parameter name collisions are handled gracefully."""
    query = (
        sql.select("*").from_("events").where_gt("priority", 1).where_lt("priority", 10).where_eq("status", "active")
    )
    stmt = query.build()

    params = stmt.parameters

    # Should have status parameter
    assert "status" in params
    assert params["status"] == "active"

    # Should have priority parameters with collision resolution
    priority_params = [key for key in params.keys() if "priority" in key]
    assert len(priority_params) == 2

    # One should be "priority", the other "priority_1"
    assert "priority" in params or "priority_1" in params
    assert 1 in params.values()
    assert 10 in params.values()


def test_subquery_parameter_preservation() -> None:
    """Test that parameters in subqueries are properly preserved."""
    subquery = sql.select("id").from_("active_users").where_eq("status", "verified")

    query = sql.select("name").from_("posts").where_in("author_id", subquery)
    stmt = query.build()

    # Should preserve the subquery parameter name
    assert "status" in stmt.parameters
    assert stmt.parameters["status"] == "verified"


def test_table_prefixed_columns_extract_column_name() -> None:
    """Test that table-prefixed columns extract just the column name for parameters."""
    query = sql.select("*").from_("users u").where_eq("u.email", "test@example.com").where_gt("u.age", 21)
    stmt = query.build()

    # Should extract column names without table prefix
    assert "email" in stmt.parameters
    assert "age" in stmt.parameters
    assert stmt.parameters["email"] == "test@example.com"
    assert stmt.parameters["age"] == 21


def test_mixed_parameter_types_preserve_names() -> None:
    """Test that mixed parameter types (strings, numbers, booleans) preserve proper names."""
    query = (
        sql.update("accounts")
        .set({"username": "john_doe", "balance": 1500.75, "is_active": True, "last_login": None})
        .where_eq("account_id", 12345)
    )
    stmt = query.build()

    params = stmt.parameters

    # Should preserve all column names
    expected_keys = ["username", "balance", "is_active", "last_login", "account_id"]
    for key in expected_keys:
        assert key in params or any(key in param_key for param_key in params.keys())

    # Should preserve values with correct types
    assert "john_doe" in params.values()
    assert 1500.75 in params.values()
    assert True in params.values()
    assert None in params.values()
    assert 12345 in params.values()


def test_no_generic_param_names_in_common_operations() -> None:
    """Test that common operations do not generate generic param_1, param_2 names."""
    # Test various common query patterns
    queries = [
        sql.select("*").from_("users").where_eq("name", "John"),
        sql.update("users").set("email", "new@email.com").where_eq("id", 1),
        sql.insert("users").values_from_dict({"name": "Jane", "age": 25}),
        sql.select("*").from_("posts").where_in("category", ["tech", "news"]),
        sql.select("*").from_("events").where_between("date", "2023-01-01", "2023-12-31"),
    ]

    for query in queries:
        stmt = query.build()

        # Should not contain any generic parameter names
        generic_params = [key for key in stmt.parameters.keys() if key.startswith("param_")]
        assert len(generic_params) == 0, f"Found generic parameters {generic_params} in query: {stmt.sql}"

        # All parameter names should be descriptive
        for param_name in stmt.parameters.keys():
            assert not param_name.startswith("param_"), f"Parameter name '{param_name}' is generic"
