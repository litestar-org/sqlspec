"""Unit tests for DML Safety Validator.

This module tests the DML Safety validator including:
- Statement categorization (DDL, DML, DQL, DCL, TCL)
- DDL prevention when configured
- Risky DML detection (DELETE/UPDATE without WHERE)
- DCL restrictions
- Row limit enforcement
- Affected table extraction
"""

from typing import Any

import pytest
from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators._dml_safety import DMLSafetyConfig, DMLSafetyValidator, StatementCategory
from sqlspec.statement.sql import SQLConfig


# Test Data
@pytest.fixture
def validator() -> DMLSafetyValidator:
    """Create a DML safety validator instance."""
    return DMLSafetyValidator()


@pytest.fixture
def context() -> SQLProcessingContext:
    """Create a processing context."""
    return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())


# Statement Categorization Tests
@pytest.mark.parametrize(
    "sql,expected_category",
    [
        # DDL statements
        ("CREATE TABLE users (id INT)", StatementCategory.DDL),
        ("ALTER TABLE users ADD COLUMN name VARCHAR(255)", StatementCategory.DDL),
        ("DROP TABLE users", StatementCategory.DDL),
        ("TRUNCATE TABLE users", StatementCategory.DDL),
        ("CREATE INDEX idx_users ON users(id)", StatementCategory.DDL),
        ("DROP INDEX idx_users", StatementCategory.DDL),
        ("COMMENT ON TABLE users IS 'User table'", StatementCategory.DDL),
        # DML statements
        ("INSERT INTO users VALUES (1, 'John')", StatementCategory.DML),
        ("UPDATE users SET name = 'Jane' WHERE id = 1", StatementCategory.DML),
        ("DELETE FROM users WHERE id = 1", StatementCategory.DML),
        (
            "MERGE INTO users USING temp_users ON users.id = temp_users.id WHEN MATCHED THEN UPDATE SET name = temp_users.name",
            StatementCategory.DML,
        ),
        # DQL statements
        ("SELECT * FROM users", StatementCategory.DQL),
        ("SELECT COUNT(*) FROM users", StatementCategory.DQL),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", StatementCategory.DQL),
        ("SELECT * FROM users UNION SELECT * FROM temp_users", StatementCategory.DQL),
        ("SELECT * FROM users INTERSECT SELECT * FROM temp_users", StatementCategory.DQL),
        ("SELECT * FROM users EXCEPT SELECT * FROM temp_users", StatementCategory.DQL),
        # DCL statements
        ("GRANT SELECT ON users TO john", StatementCategory.DCL),
        # TCL statements
        ("COMMIT", StatementCategory.TCL),
        ("ROLLBACK", StatementCategory.TCL),
    ],
    ids=[
        "create_table",
        "alter_table",
        "drop_table",
        "truncate_table",
        "create_index",
        "drop_index",
        "comment",
        "insert",
        "update",
        "delete",
        "merge",
        "select",
        "select_count",
        "cte",
        "union",
        "intersect",
        "except",
        "grant",
        "commit",
        "rollback",
    ],
)
def test_statement_categorization(sql: str, expected_category: StatementCategory) -> None:
    """Test that statements are categorized correctly."""
    parsed = parse_one(sql)
    category = DMLSafetyValidator._categorize_statement(parsed)
    assert category == expected_category


@pytest.mark.parametrize(
    "sql,expected_operation",
    [
        ("CREATE TABLE users (id INT)", "CREATE"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("SELECT * FROM users", "SELECT"),
        ("UPDATE users SET name = 'test'", "UPDATE"),
        ("DELETE FROM users", "DELETE"),
        ("GRANT SELECT ON users TO john", "GRANT"),
        ("COMMIT", "COMMIT"),
    ],
    ids=["create", "insert", "select", "update", "delete", "grant", "commit"],
)
def test_operation_type_extraction(sql: str, expected_operation: str) -> None:
    """Test operation type extraction from expressions."""
    parsed = parse_one(sql)
    operation = DMLSafetyValidator._get_operation_type(parsed)
    assert operation == expected_operation


# DDL Prevention Tests
@pytest.mark.parametrize(
    "sql,config_prevent_ddl,expected_errors",
    [
        ("CREATE TABLE test (id INT)", True, 1),
        ("CREATE TABLE test (id INT)", False, 0),
        ("ALTER TABLE users ADD name VARCHAR(50)", True, 1),
        ("ALTER TABLE users ADD name VARCHAR(50)", False, 0),
        ("DROP TABLE users", True, 1),
        ("DROP TABLE users", False, 0),
        ("TRUNCATE TABLE users", True, 1),
        ("TRUNCATE TABLE users", False, 0),
    ],
    ids=[
        "create_blocked",
        "create_allowed",
        "alter_blocked",
        "alter_allowed",
        "drop_blocked",
        "drop_allowed",
        "truncate_blocked",
        "truncate_allowed",
    ],
)
def test_ddl_prevention(
    sql: str, config_prevent_ddl: bool, expected_errors: int, context: SQLProcessingContext
) -> None:
    """Test DDL prevention configuration."""
    validator = DMLSafetyValidator(config=DMLSafetyConfig(prevent_ddl=config_prevent_ddl))
    context.initial_sql_string = sql
    context.current_expression = parse_one(sql)

    validator.validate(context.current_expression, context)

    assert len(context.validation_errors) == expected_errors
    if expected_errors > 0:
        error = context.validation_errors[0]
        assert error.risk_level == RiskLevel.CRITICAL
        assert "DDL operation" in error.message
        assert "is not allowed" in error.message


def test_ddl_allowed_operations(context: SQLProcessingContext) -> None:
    """Test that specific DDL operations can be allowed."""
    config = DMLSafetyConfig(prevent_ddl=True, allowed_ddl_operations={"CREATE"})
    validator = DMLSafetyValidator(config=config)

    # CREATE should be allowed
    context.initial_sql_string = "CREATE TABLE test (id INT)"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)
    assert len(context.validation_errors) == 0

    # DROP should still be blocked
    context.validation_errors.clear()
    context.initial_sql_string = "DROP TABLE test"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)
    assert len(context.validation_errors) == 1
    assert "DROP" in context.validation_errors[0].message


# DML Safety Tests
@pytest.mark.parametrize(
    "sql,expected_errors,expected_risk",
    [
        ("DELETE FROM users", 1, RiskLevel.HIGH),
        ("DELETE FROM users WHERE id = 1", 0, None),
        ("UPDATE users SET active = false", 1, RiskLevel.HIGH),
        ("UPDATE users SET active = false WHERE id = 1", 0, None),
        ("INSERT INTO users VALUES (1, 'test')", 0, None),  # INSERT doesn't require WHERE
    ],
    ids=["delete_no_where", "delete_with_where", "update_no_where", "update_with_where", "insert"],
)
def test_risky_dml_detection(
    sql: str, expected_errors: int, expected_risk: RiskLevel, context: SQLProcessingContext
) -> None:
    """Test detection of risky DML operations."""
    validator = DMLSafetyValidator(config=DMLSafetyConfig())
    context.initial_sql_string = sql
    context.current_expression = parse_one(sql)

    validator.validate(context.current_expression, context)

    assert len(context.validation_errors) == expected_errors
    if expected_errors > 0:
        error = context.validation_errors[0]
        assert error.risk_level == expected_risk
        assert "without WHERE clause affects all rows" in error.message


def test_custom_require_where_clause(context: SQLProcessingContext) -> None:
    """Test custom configuration for operations requiring WHERE clause."""
    # Configure to only require WHERE for DELETE
    config = DMLSafetyConfig(require_where_clause={"DELETE"})
    validator = DMLSafetyValidator(config=config)

    # DELETE without WHERE should trigger error
    context.initial_sql_string = "DELETE FROM users"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)
    assert len(context.validation_errors) == 1

    # UPDATE without WHERE should NOT trigger error (not in require_where_clause set)
    context.validation_errors.clear()
    context.initial_sql_string = "UPDATE users SET active = false"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)
    assert len(context.validation_errors) == 0


# DCL Prevention Tests
@pytest.mark.parametrize(
    "sql,config_prevent_dcl,expected_errors",
    [("GRANT SELECT ON users TO john", True, 1), ("GRANT SELECT ON users TO john", False, 0)],
    ids=["dcl_blocked", "dcl_allowed"],
)
def test_dcl_prevention(
    sql: str, config_prevent_dcl: bool, expected_errors: int, context: SQLProcessingContext
) -> None:
    """Test DCL prevention configuration."""
    validator = DMLSafetyValidator(config=DMLSafetyConfig(prevent_dcl=config_prevent_dcl))
    context.initial_sql_string = sql
    context.current_expression = parse_one(sql)

    validator.validate(context.current_expression, context)

    assert len(context.validation_errors) == expected_errors
    if expected_errors > 0:
        error = context.validation_errors[0]
        assert error.risk_level == RiskLevel.HIGH
        assert "DCL operation" in error.message
        assert "is not allowed" in error.message


# WHERE Clause Detection Tests
@pytest.mark.parametrize(
    "sql,expected_has_where",
    [
        ("DELETE FROM users WHERE id = 1", True),
        ("DELETE FROM users", False),
        ("UPDATE users SET name = 'test' WHERE id = 1", True),
        ("UPDATE users SET name = 'test'", False),
        ("INSERT INTO users VALUES (1)", True),  # INSERT doesn't need WHERE
        ("SELECT * FROM users", True),  # SELECT doesn't need WHERE for this check
    ],
    ids=["delete_with_where", "delete_no_where", "update_with_where", "update_no_where", "insert", "select"],
)
def test_where_clause_detection(sql: str, expected_has_where: bool) -> None:
    """Test WHERE clause detection in DML statements."""
    parsed = parse_one(sql)
    has_where = DMLSafetyValidator._has_where_clause(parsed)
    assert has_where == expected_has_where


# Row Estimation Tests
@pytest.mark.parametrize(
    "sql,expected_estimate_range",
    [
        ("DELETE FROM users", (999999999, 999999999)),  # No WHERE = all rows
        ("UPDATE users SET active = false", (999999999, 999999999)),  # No WHERE = all rows
        ("DELETE FROM users WHERE id = 1", (1, 1)),  # ID condition = 1 row
        ("UPDATE users SET active = false WHERE uuid = 'abc'", (1, 1)),  # UUID condition = 1 row
        ("DELETE FROM users WHERE email = 'test@example.com'", (100, 100)),  # Indexed column
        ("UPDATE users SET active = false WHERE created_at > '2023-01-01'", (100, 100)),  # Indexed column
        ("DELETE FROM users WHERE name = 'John'", (10000, 10000)),  # General condition
    ],
    ids=[
        "delete_all",
        "update_all",
        "delete_by_id",
        "update_by_uuid",
        "delete_by_email",
        "update_by_date",
        "delete_by_name",
    ],
)
def test_row_estimation(sql: str, expected_estimate_range: tuple[int, int]) -> None:
    """Test row estimation for DML operations."""
    parsed = parse_one(sql)
    validator = DMLSafetyValidator()
    estimate = validator._estimate_affected_rows(parsed)
    min_expected, max_expected = expected_estimate_range
    assert min_expected <= estimate <= max_expected


def test_max_affected_rows_enforcement(context: SQLProcessingContext) -> None:
    """Test enforcement of maximum affected rows limit."""
    config = DMLSafetyConfig(max_affected_rows=50)
    validator = DMLSafetyValidator(config=config)

    # Query affecting many rows should trigger error
    context.initial_sql_string = "DELETE FROM users WHERE name = 'John'"  # Estimated 10000 rows
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)

    assert len(context.validation_errors) == 1
    error = context.validation_errors[0]
    assert error.risk_level == RiskLevel.MEDIUM
    assert "may affect" in error.message
    assert "limit: 50" in error.message


# Unique Condition Detection Tests
@pytest.mark.parametrize(
    "where_clause,expected_unique",
    [
        ("id = 1", True),
        ("uuid = 'abc-123'", True),
        ("pk = 100", True),
        ("primary_key = 500", True),
        ("guid = 'xyz-789'", True),
        ("name = 'John'", False),
        ("email = 'test@example.com'", False),
        ("id > 1", False),  # Not equality
    ],
    ids=["id_eq", "uuid_eq", "pk_eq", "primary_key_eq", "guid_eq", "name_eq", "email_eq", "id_gt"],
)
def test_unique_condition_detection(where_clause: str, expected_unique: bool) -> None:
    """Test detection of unique column conditions."""
    sql = f"SELECT * FROM users WHERE {where_clause}"
    parsed = parse_one(sql)
    where_expr = parsed.args.get("where")

    is_unique = DMLSafetyValidator._has_unique_condition(where_expr) if where_expr is not None else False
    assert is_unique == expected_unique


# Indexed Condition Detection Tests
@pytest.mark.parametrize(
    "where_clause,expected_indexed",
    [
        ("created_at > '2023-01-01'", True),
        ("updated_at = '2023-12-01'", True),
        ("email = 'test@example.com'", True),
        ("username = 'john_doe'", True),
        ("status = 'active'", True),
        ("type = 'user'", True),
        ("name = 'John'", False),
        ("description LIKE '%test%'", False),
    ],
    ids=["created_at", "updated_at", "email", "username", "status", "type", "name", "description"],
)
def test_indexed_condition_detection(where_clause: str, expected_indexed: bool) -> None:
    """Test detection of indexed column conditions."""
    sql = f"SELECT * FROM users WHERE {where_clause}"
    parsed = parse_one(sql)
    where_expr = parsed.args.get("where")

    is_indexed = DMLSafetyValidator._has_indexed_condition(where_expr) if where_expr is not None else False
    assert is_indexed == expected_indexed


# Affected Tables Extraction Tests
@pytest.mark.parametrize(
    "sql,expected_tables",
    [
        ("INSERT INTO users VALUES (1, 'John')", ["users"]),
        ("UPDATE users SET name = 'Jane'", ["users"]),
        ("DELETE FROM orders", ["orders"]),
        ("CREATE TABLE products (id INT)", ["products"]),
        ("DROP TABLE categories", ["categories"]),
        ("ALTER TABLE items ADD COLUMN price DECIMAL", ["items"]),
        ("SELECT * FROM users", []),  # No affected tables for SELECT
        ("GRANT SELECT ON users TO john", []),  # Complex extraction not implemented
    ],
    ids=["insert", "update", "delete", "create", "drop", "alter", "select", "grant"],
)
def test_affected_tables_extraction(sql: str, expected_tables: list[str]) -> None:
    """Test extraction of affected table names."""
    parsed = parse_one(sql)
    tables = DMLSafetyValidator._extract_affected_tables(parsed)
    assert tables == expected_tables


# Complex Scenarios Tests
def test_migration_mode_metadata(context: SQLProcessingContext) -> None:
    """Test that migration mode is stored in metadata."""
    config = DMLSafetyConfig(migration_mode=True, prevent_ddl=False)
    validator = DMLSafetyValidator(config=config)

    context.initial_sql_string = "CREATE TABLE test (id INT)"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)

    metadata = context.metadata.get("DMLSafetyValidator", {})
    assert metadata["migration_mode"] is True
    assert metadata["statement_category"] == "ddl"
    assert metadata["operation"] == "CREATE"
    assert metadata["affected_tables"] == ["test"]


def test_metadata_storage_for_dml(context: SQLProcessingContext) -> None:
    """Test metadata storage for DML operations."""
    validator = DMLSafetyValidator()

    context.initial_sql_string = "UPDATE users SET active = false WHERE id = 1"
    context.current_expression = parse_one(context.initial_sql_string)
    validator.validate(context.current_expression, context)

    metadata = context.metadata.get("DMLSafetyValidator", {})
    assert metadata["statement_category"] == "dml"
    assert metadata["operation"] == "UPDATE"
    assert metadata["has_where_clause"] is True
    assert metadata["affected_tables"] == ["users"]
    assert metadata["migration_mode"] is False


def test_complex_query_categorization() -> None:
    """Test categorization of complex queries with multiple statement types."""
    # CTE with INSERT
    sql = """
    WITH new_users AS (
        SELECT * FROM temp_users
    )
    INSERT INTO users SELECT * FROM new_users
    """
    parsed = parse_one(sql)
    category = DMLSafetyValidator._categorize_statement(parsed)
    # The primary statement is INSERT, so it should be categorized as DML
    assert category == StatementCategory.DML


@pytest.mark.parametrize(
    "config_kwargs,sql,expected_error_count",
    [
        ({"prevent_ddl": True, "prevent_dcl": True}, "CREATE TABLE test (id INT)", 1),
        ({"prevent_ddl": False, "prevent_dcl": True}, "CREATE TABLE test (id INT)", 0),
        ({"prevent_ddl": True, "prevent_dcl": True}, "GRANT SELECT ON users TO john", 1),
        ({"prevent_ddl": True, "prevent_dcl": False}, "GRANT SELECT ON users TO john", 0),
        ({"require_where_clause": {"DELETE", "UPDATE"}}, "DELETE FROM users", 1),
        ({"require_where_clause": set()}, "DELETE FROM users", 0),
        ({"max_affected_rows": 10}, "DELETE FROM users WHERE name = 'John'", 1),  # Estimated 10000 > 10
        ({"max_affected_rows": 20000}, "DELETE FROM users WHERE name = 'John'", 0),  # Estimated 10000 < 20000
    ],
    ids=[
        "ddl_and_dcl_blocked",
        "ddl_allowed_dcl_blocked",
        "dcl_blocked",
        "dcl_allowed",
        "require_where",
        "no_require_where",
        "max_rows_exceeded",
        "max_rows_ok",
    ],
)
def test_comprehensive_configuration(
    config_kwargs: dict[str, Any], sql: str, expected_error_count: int, context: SQLProcessingContext
) -> None:
    """Test comprehensive configuration scenarios."""
    config = DMLSafetyConfig(**config_kwargs)
    validator = DMLSafetyValidator(config=config)

    context.initial_sql_string = sql
    context.current_expression = parse_one(sql)
    validator.validate(context.current_expression, context)

    assert len(context.validation_errors) == expected_error_count


def test_safe_operations_no_errors(context: SQLProcessingContext) -> None:
    """Test that safe operations generate no validation errors."""
    validator = DMLSafetyValidator()

    safe_queries = [
        "SELECT * FROM users",
        "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
        "INSERT INTO logs (message) VALUES ('test')",
        "UPDATE users SET last_login = NOW() WHERE id = 1",
        "DELETE FROM temp_data WHERE created_at < '2023-01-01'",
    ]

    for sql in safe_queries:
        context.validation_errors.clear()
        context.initial_sql_string = sql
        context.current_expression = parse_one(sql)
        validator.validate(context.current_expression, context)
        assert len(context.validation_errors) == 0, f"Query should be safe: {sql}"
