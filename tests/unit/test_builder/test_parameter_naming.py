"""Unit tests for SQL builder parameter naming.

This module tests that all SQL builder operations use descriptive, column-based
parameter names instead of generic param_1, param_2, etc.

Tests cover:
- WHERE clause methods with column name preservation
- UPDATE operations with SET clauses
- INSERT operations with values and columns
- MERGE operations
- Complex queries with multiple parameter types
- Parameter collision handling
- Edge cases and error conditions
"""

from sqlspec import sql


def test_where_eq_uses_column_name_as_parameter() -> None:
    """Test that where_eq uses column name as parameter name."""
    query = sql.select("*").from_("users").where_eq("email", "test@example.com")
    stmt = query.build()

    assert "email" in stmt.parameters
    assert stmt.parameters["email"] == "test@example.com"
    assert ":email" in stmt.sql


def test_where_in_uses_column_name_with_numbering() -> None:
    """Test that where_in uses column name with numbering for multiple values."""
    query = sql.select("*").from_("posts").where_in("category", ["tech", "science", "business"])
    stmt = query.build()

    assert "category_1" in stmt.parameters
    assert "category_2" in stmt.parameters
    assert "category_3" in stmt.parameters
    assert stmt.parameters["category_1"] == "tech"
    assert stmt.parameters["category_2"] == "science"
    assert stmt.parameters["category_3"] == "business"


def test_where_between_uses_descriptive_suffixes() -> None:
    """Test that where_between uses descriptive low/high suffixes."""
    query = sql.select("*").from_("events").where_between("date", "2023-01-01", "2023-12-31")
    stmt = query.build()

    assert "date_low" in stmt.parameters
    assert "date_high" in stmt.parameters
    assert stmt.parameters["date_low"] == "2023-01-01"
    assert stmt.parameters["date_high"] == "2023-12-31"


def test_where_any_uses_column_name_with_any_suffix() -> None:
    """Test that where_any uses column name with any suffix."""
    query = sql.select("*").from_("users").where_any("role", ["admin", "moderator"])
    stmt = query.build()

    assert "role_any_1" in stmt.parameters
    assert "role_any_2" in stmt.parameters
    assert stmt.parameters["role_any_1"] == "admin"
    assert stmt.parameters["role_any_2"] == "moderator"


def test_multiple_where_conditions_preserve_column_names() -> None:
    """Test that multiple WHERE conditions preserve individual column names."""
    query = (
        sql.select("*")
        .from_("orders")
        .where_eq("status", "pending")
        .where_gt("total", 100.0)
        .where_like("customer_email", "%@company.com")
    )
    stmt = query.build()

    assert "status" in stmt.parameters
    assert "total" in stmt.parameters
    assert "customer_email" in stmt.parameters
    assert stmt.parameters["status"] == "pending"
    assert stmt.parameters["total"] == 100.0
    assert stmt.parameters["customer_email"] == "%@company.com"


def test_parameter_collision_handling() -> None:
    """Test that parameter name collisions are resolved with numbering."""
    query = sql.select("*").from_("products").where_gt("price", 10).where_lt("price", 100)
    stmt = query.build()

    # Should have both price parameters with collision resolution
    price_params = [key for key in stmt.parameters.keys() if "price" in key]
    assert len(price_params) == 2

    # One should be "price", the other "price_1"
    assert "price" in stmt.parameters
    assert "price_1" in stmt.parameters
    assert 10 in stmt.parameters.values()
    assert 100 in stmt.parameters.values()


def test_table_prefixed_columns_extract_column_name() -> None:
    """Test that table-prefixed columns extract just the column name."""
    query = (
        sql.select("*")
        .from_("users u")
        .join("profiles p", "u.id = p.user_id")
        .where_eq("u.email", "test@example.com")
        .where_eq("p.status", "active")
    )
    stmt = query.build()

    # Should extract column names without table prefix
    assert "email" in stmt.parameters
    assert "status" in stmt.parameters
    assert stmt.parameters["email"] == "test@example.com"
    assert stmt.parameters["status"] == "active"


def test_update_set_single_column_uses_column_name() -> None:
    """Test that UPDATE SET with single column uses column name."""
    query = sql.update("users").set("last_login", "2023-01-01 10:00:00")
    stmt = query.build()

    assert "last_login" in stmt.parameters
    assert stmt.parameters["last_login"] == "2023-01-01 10:00:00"
    assert ":last_login" in stmt.sql


def test_update_set_multiple_columns_preserve_names() -> None:
    """Test that UPDATE SET with multiple columns preserves all names."""
    query = sql.update("products").set("name", "Updated Product").set("price", 49.99).set("in_stock", True)
    stmt = query.build()

    assert "name" in stmt.parameters
    assert "price" in stmt.parameters
    assert "in_stock" in stmt.parameters
    assert stmt.parameters["name"] == "Updated Product"
    assert stmt.parameters["price"] == 49.99
    assert stmt.parameters["in_stock"] is True


def test_update_set_with_dict_uses_column_names() -> None:
    """Test that UPDATE SET with dictionary uses column names."""
    query = sql.update("accounts").set({"balance": 1500.75, "last_transaction": "2023-01-15", "is_verified": True})
    stmt = query.build()

    # Should use dictionary keys as parameter names
    expected_keys = ["balance", "last_transaction", "is_verified"]
    for key in expected_keys:
        # Parameter might have collision suffix, so check if base name exists
        assert any(key in param_key for param_key in stmt.parameters.keys())

    # Should preserve values
    assert 1500.75 in stmt.parameters.values()
    assert "2023-01-15" in stmt.parameters.values()
    assert True in stmt.parameters.values()


def test_insert_with_columns_uses_column_names() -> None:
    """Test that INSERT with specified columns uses column names."""
    query = (
        sql.insert("employees").columns("first_name", "last_name", "department").values("John", "Smith", "Engineering")
    )
    stmt = query.build()

    # Should use column names for parameters
    assert "first_name" in stmt.parameters
    assert "last_name" in stmt.parameters
    assert "department" in stmt.parameters
    assert stmt.parameters["first_name"] == "John"
    assert stmt.parameters["last_name"] == "Smith"
    assert stmt.parameters["department"] == "Engineering"


def test_insert_values_from_dict_preserves_keys() -> None:
    """Test that INSERT values_from_dict preserves dictionary keys."""
    query = sql.insert("orders").values_from_dict(
        {"customer_id": 12345, "product_name": "Widget", "quantity": 3, "order_date": "2023-01-01"}
    )
    stmt = query.build()

    # Should preserve dictionary keys in parameter names
    expected_keys = ["customer_id", "product_name", "quantity", "order_date"]
    for key in expected_keys:
        # Check if key exists directly or as part of parameter name
        assert key in stmt.parameters or any(key in param_key for param_key in stmt.parameters.keys())


def test_complex_query_preserves_all_column_names() -> None:
    """Test that complex queries preserve column names across all operations."""
    query = (
        sql.select("u.username", "p.title")
        .from_("users u")
        .join("posts p", "u.id = p.author_id")
        .where_eq("u.status", "active")
        .where_in("p.category", ["tech", "science"])
        .where_between("p.views", 100, 10000)
        .where_like("p.title", "%python%")
    )
    stmt = query.build()

    params = stmt.parameters

    # Should preserve all column names
    assert "status" in params
    assert params["status"] == "active"

    # Category parameters with numbering
    category_params = [key for key in params.keys() if "category" in key]
    assert len(category_params) == 2
    assert "tech" in params.values()
    assert "science" in params.values()

    # Views parameters with descriptive suffixes
    views_params = [key for key in params.keys() if "views" in key]
    assert len(views_params) == 2
    assert 100 in params.values()
    assert 10000 in params.values()

    # Title parameter
    assert "title" in params
    assert params["title"] == "%python%"


def test_subquery_parameters_are_preserved() -> None:
    """Test that subquery parameters maintain their names."""
    subquery = sql.select("user_id").from_("subscriptions").where_eq("plan_type", "premium")

    query = sql.select("name", "email").from_("users").where_in("id", subquery)
    stmt = query.build()

    # Subquery parameter should be preserved
    assert "plan_type" in stmt.parameters
    assert stmt.parameters["plan_type"] == "premium"


def test_mixed_parameter_types_preserve_names() -> None:
    """Test that mixed parameter types preserve proper column names."""
    query = (
        sql.update("user_profiles")
        .set(
            {
                "username": "john_doe",  # string
                "age": 28,  # int
                "salary": 75000.50,  # float
                "is_active": True,  # bool
                "last_seen": None,  # None
            }
        )
        .where_eq("user_id", 12345)
    )
    stmt = query.build()

    params = stmt.parameters

    # Should preserve all column names regardless of value type
    expected_columns = ["username", "age", "salary", "is_active", "last_seen", "user_id"]
    for col in expected_columns:
        assert any(col in param_key for param_key in params.keys())

    # Should preserve value types
    assert "john_doe" in params.values()
    assert 28 in params.values()
    assert 75000.50 in params.values()
    assert True in params.values()
    assert None in params.values()
    assert 12345 in params.values()


def test_no_generic_param_names_in_where_clauses() -> None:
    """Test that WHERE clauses never use generic param_1, param_2 names."""
    test_cases = [
        sql.select("*").from_("users").where_eq("name", "John"),
        sql.select("*").from_("posts").where_in("status", ["draft", "published"]),
        sql.select("*").from_("events").where_between("date", "2023-01-01", "2023-12-31"),
        sql.select("*").from_("products").where_like("name", "%widget%"),
        sql.select("*").from_("orders").where_gt("total", 100),
    ]

    for query in test_cases:
        stmt = query.build()

        # Should not contain any generic parameter names
        generic_params = [key for key in stmt.parameters.keys() if key.startswith("param_")]
        assert len(generic_params) == 0, f"Found generic parameters {generic_params} in: {stmt.sql}"


def test_no_generic_param_names_in_update_operations() -> None:
    """Test that UPDATE operations never use generic parameter names."""
    test_cases = [
        sql.update("users").set("email", "new@email.com"),
        sql.update("posts").set({"title": "New Title", "content": "New content"}),
        sql.update("products").set("price", 29.99).where_eq("id", 123),
    ]

    for query in test_cases:
        stmt = query.build()

        # Should not contain any generic parameter names
        generic_params = [key for key in stmt.parameters.keys() if key.startswith("param_")]
        assert len(generic_params) == 0, f"Found generic parameters {generic_params} in: {stmt.sql}"


def test_no_generic_param_names_in_insert_operations() -> None:
    """Test that INSERT operations use descriptive parameter names."""
    test_cases = [
        sql.insert("users").values_from_dict({"name": "John", "email": "john@test.com"}),
        sql.insert("posts").columns("title", "content").values("Hello", "World"),
    ]

    for query in test_cases:
        stmt = query.build()

        # Should not contain any generic parameter names
        generic_params = [key for key in stmt.parameters.keys() if key.startswith("param_")]
        assert len(generic_params) == 0, f"Found generic parameters {generic_params} in: {stmt.sql}"


def test_parameter_names_are_sql_safe() -> None:
    """Test that generated parameter names are safe for SQL usage."""
    query = (
        sql.select("*").from_("test_table").where_eq("column_name", "value").where_in("other_column", ["a", "b", "c"])
    )
    stmt = query.build()

    for param_name in stmt.parameters.keys():
        # Should not contain SQL injection characters
        assert "'" not in param_name
        assert '"' not in param_name
        assert ";" not in param_name
        assert "--" not in param_name

        # Should be valid identifier-like
        assert param_name.replace("_", "").replace("0123456789", "").isalpha() or "_" in param_name


def test_empty_and_null_values_preserve_column_names() -> None:
    """Test that empty and null values still preserve column names."""
    query = (
        sql.update("users")
        .set(
            {
                "middle_name": "",  # empty string
                "phone": None,  # null
                "notes": "   ",  # whitespace
            }
        )
        .where_eq("id", 1)
    )
    stmt = query.build()

    params = stmt.parameters

    # Should preserve column names even for empty/null values
    expected_columns = ["middle_name", "phone", "notes", "id"]
    for col in expected_columns:
        assert any(col in param_key for param_key in params.keys())

    # Should preserve actual values
    assert "" in params.values()
    assert None in params.values()
    assert "   " in params.values()
    assert 1 in params.values()


def test_original_user_example_works_correctly() -> None:
    """Test the exact user example that was originally failing."""
    query = sql.select("id", "name", "slug").from_("test_table").where_eq("slug", "test-item")
    stmt = query.build()

    # Should use :slug parameter, not :param_1
    assert "slug" in stmt.parameters
    assert stmt.parameters["slug"] == "test-item"
    assert ":slug" in stmt.sql

    # Should not contain any generic parameters
    assert not any(key.startswith("param_") for key in stmt.parameters.keys())


def test_parameter_naming_with_special_characters_in_values() -> None:
    """Test that parameter naming works with special characters in values."""
    query = (
        sql.select("*")
        .from_("logs")
        .where_eq("message", "Error: Connection failed!")
        .where_like("details", "%SQL injection attempt: DROP TABLE%")
    )
    stmt = query.build()

    # Should preserve column names despite special characters in values
    assert "message" in stmt.parameters
    assert "details" in stmt.parameters
    assert stmt.parameters["message"] == "Error: Connection failed!"
    assert stmt.parameters["details"] == "%SQL injection attempt: DROP TABLE%"
