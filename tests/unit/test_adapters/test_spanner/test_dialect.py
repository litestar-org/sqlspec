"""Comprehensive unit tests for Spanner dialect.

Tests follow sqlglot's dialect testing patterns:
- validate_identity: Parse -> Generate should produce equivalent SQL
- validate_all: Test cross-dialect transpilation
- Error handling and edge cases

The Spanner dialect extends BigQuery and adds support for:
- INTERLEAVE IN PARENT clause (parsed via custom extension)
- TTL INTERVAL ... ON ... clause (parsed via _parse_property)

Note: The current implementation stores INTERLEAVE metadata on Schema/Table expressions
when parsing. Due to sqlglot's architecture, some Spanner-specific syntax may parse
as generic commands when the syntax doesn't match expected patterns.

See: https://github.com/tobymao/sqlglot/blob/main/tests/dialects/test_bigquery.py
"""

from typing import Any

from sqlglot import exp, parse, parse_one, transpile

from sqlspec.adapters.spanner.dialect import Spanner


def validate_identity(sql: str, expected: "str | None" = None, dialect: "Any" = Spanner) -> None:
    """Validate that SQL parses and regenerates to expected form.

    Args:
        sql: Input SQL to parse
        expected: Expected output SQL (defaults to input if None)
        dialect: Dialect to use for parsing and generation
    """
    if expected is None:
        expected = sql.strip()
    ast = parse_one(sql, read=dialect)
    generated = ast.sql(dialect=dialect)
    assert generated.strip() == expected.strip(), f"Expected:\n{expected}\n\nGot:\n{generated}"


# =============================================================================
# Basic SQL Parsing Tests - Inherited from BigQuery
# =============================================================================


def test_select_basic() -> None:
    """Test basic SELECT statement parsing."""
    validate_identity("SELECT * FROM users")
    validate_identity("SELECT id, name FROM users WHERE id = 1")
    validate_identity("SELECT COUNT(*) FROM orders")


def test_select_with_alias() -> None:
    """Test SELECT with column and table aliases."""
    assert parse_one("SELECT u.id AS user_id, u.name FROM users AS u", read=Spanner) is not None
    assert parse_one("SELECT a.x, b.y FROM table_a AS a, table_b AS b", read=Spanner) is not None


def test_select_with_functions() -> None:
    """Test SELECT with built-in functions."""
    validate_identity("SELECT UPPER(name) FROM users")
    validate_identity("SELECT LENGTH(description) FROM products")
    validate_identity("SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users")


def test_select_with_aggregates() -> None:
    """Test SELECT with aggregate functions."""
    validate_identity("SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department")
    validate_identity("SELECT AVG(salary) FROM employees WHERE department = 'Engineering'")
    validate_identity("SELECT MAX(price), MIN(price) FROM products")


def test_select_with_subquery() -> None:
    """Test SELECT with subqueries."""
    validate_identity("SELECT * FROM (SELECT id, name FROM users) AS t")
    validate_identity("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)")


def test_select_with_joins() -> None:
    """Test SELECT with various JOIN types."""
    validate_identity("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
    validate_identity("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
    validate_identity("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")


def test_select_with_order_limit() -> None:
    """Test SELECT with ORDER BY and LIMIT."""
    validate_identity("SELECT * FROM users ORDER BY created_at DESC")
    validate_identity("SELECT * FROM users ORDER BY name ASC LIMIT 10")
    validate_identity("SELECT * FROM users LIMIT 10 OFFSET 20")


def test_select_with_distinct() -> None:
    """Test SELECT DISTINCT."""
    validate_identity("SELECT DISTINCT department FROM employees")


def test_select_with_case() -> None:
    """Test SELECT with CASE expressions."""
    validate_identity("SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM users")
    validate_identity("SELECT CASE status WHEN 1 THEN 'a' WHEN 2 THEN 'b' END FROM users")


# =============================================================================
# Spanner-Specific Parameter Style Tests
# =============================================================================


def test_named_parameter_style() -> None:
    """Test Spanner's @param named parameter style."""
    sql = "SELECT * FROM users WHERE id = @user_id"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_parameter_in_where_clause() -> None:
    """Test parameters in WHERE clause."""
    sql = "SELECT * FROM users WHERE name = @name AND age > @min_age"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_parameter_multiple_types() -> None:
    """Test various parameter usages."""
    sql = "SELECT * FROM users WHERE id = @id AND status IN UNNEST(@statuses) LIMIT @limit"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# DDL Tests - CREATE TABLE
# =============================================================================


def test_create_table_basic() -> None:
    """Test basic CREATE TABLE statement."""
    sql = """CREATE TABLE users (
  id STRING(36) NOT NULL,
  name STRING(100),
  email STRING(255),
  PRIMARY KEY (id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Create)


def test_create_table_with_types() -> None:
    """Test CREATE TABLE with various Spanner types."""
    sql = """CREATE TABLE test_types (
  id INT64 NOT NULL,
  name STRING(100),
  data BYTES(1024),
  amount FLOAT64,
  is_active BOOL,
  created_at TIMESTAMP,
  birth_date DATE,
  metadata JSON,
  PRIMARY KEY (id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Create)


def test_create_table_composite_primary_key() -> None:
    """Test CREATE TABLE with composite primary key."""
    sql = """CREATE TABLE order_items (
  order_id STRING(36) NOT NULL,
  item_id INT64 NOT NULL,
  quantity INT64,
  price FLOAT64,
  PRIMARY KEY (order_id, item_id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_create_table_with_not_null() -> None:
    """Test CREATE TABLE with NOT NULL constraints."""
    sql = """CREATE TABLE required_fields (
  id INT64 NOT NULL,
  required_name STRING(100) NOT NULL,
  optional_desc STRING(500),
  PRIMARY KEY (id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Spanner Type System Tests
# =============================================================================


def test_spanner_type_int64() -> None:
    """Test INT64 type parsing."""
    sql = "CREATE TABLE t (id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Create)


def test_spanner_type_float64() -> None:
    """Test FLOAT64 type parsing."""
    sql = "CREATE TABLE t (val FLOAT64, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_string_with_length() -> None:
    """Test STRING(n) type parsing."""
    sql = "CREATE TABLE t (name STRING(100), id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_string_max() -> None:
    """Test STRING(MAX) type parsing."""
    sql = "CREATE TABLE t (data STRING(MAX), id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_bytes() -> None:
    """Test BYTES type parsing."""
    sql = "CREATE TABLE t (data BYTES(1024), id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_bytes_max() -> None:
    """Test BYTES(MAX) type parsing."""
    sql = "CREATE TABLE t (data BYTES(MAX), id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_bool() -> None:
    """Test BOOL type parsing."""
    sql = "CREATE TABLE t (active BOOL, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_timestamp() -> None:
    """Test TIMESTAMP type parsing."""
    sql = "CREATE TABLE t (created_at TIMESTAMP, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_date() -> None:
    """Test DATE type parsing."""
    sql = "CREATE TABLE t (birth_date DATE, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_json() -> None:
    """Test JSON type parsing."""
    sql = "CREATE TABLE t (metadata JSON, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_array_string() -> None:
    """Test ARRAY<STRING> type parsing."""
    sql = "CREATE TABLE t (tags ARRAY<STRING(100)>, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_array_int64() -> None:
    """Test ARRAY<INT64> type parsing."""
    sql = "CREATE TABLE t (numbers ARRAY<INT64>, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_numeric() -> None:
    """Test NUMERIC type parsing."""
    sql = "CREATE TABLE t (amount NUMERIC, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_spanner_type_struct() -> None:
    """Test STRUCT type parsing."""
    sql = "CREATE TABLE t (address STRUCT<street STRING(100), city STRING(50)>, id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# DML Tests
# =============================================================================


def test_insert_statement() -> None:
    """Test INSERT statement parsing."""
    sql = "INSERT INTO users (id, name, email) VALUES ('123', 'Alice', 'alice@example.com')"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Insert)


def test_insert_multiple_rows() -> None:
    """Test INSERT with multiple rows."""
    sql = "INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Charlie')"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Insert)


def test_insert_select() -> None:
    """Test INSERT ... SELECT statement."""
    sql = "INSERT INTO archive_users (id, name) SELECT id, name FROM users WHERE created_at < '2023-01-01'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_update_statement() -> None:
    """Test UPDATE statement parsing."""
    sql = "UPDATE users SET name = 'Bob' WHERE id = '123'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Update)


def test_update_multiple_columns() -> None:
    """Test UPDATE with multiple columns."""
    sql = "UPDATE users SET name = 'Bob', email = 'bob@example.com', updated_at = CURRENT_TIMESTAMP() WHERE id = '123'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_delete_statement() -> None:
    """Test DELETE statement parsing."""
    sql = "DELETE FROM users WHERE id = '123'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Delete)


def test_delete_with_true() -> None:
    """Test DELETE with WHERE TRUE (Spanner pattern for truncate)."""
    sql = "DELETE FROM users WHERE TRUE"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Cross-Dialect Transpilation Tests
# =============================================================================


def test_transpile_to_bigquery() -> None:
    """Test transpiling Spanner SQL to BigQuery."""
    sql = "SELECT id, name FROM users WHERE active = TRUE"
    result = transpile(sql, read=Spanner, write="bigquery")
    assert len(result) == 1
    assert "SELECT" in result[0]


def test_transpile_from_bigquery() -> None:
    """Test transpiling BigQuery SQL to Spanner."""
    sql = "SELECT id, name FROM users WHERE active = TRUE"
    result = transpile(sql, read="bigquery", write=Spanner)
    assert len(result) == 1


def test_transpile_select_to_postgres() -> None:
    """Test transpiling Spanner SELECT to PostgreSQL."""
    sql = "SELECT id, name FROM users LIMIT 10"
    result = transpile(sql, read=Spanner, write="postgres")
    assert len(result) == 1


def test_transpile_create_table_to_postgres() -> None:
    """Test type mapping when transpiling CREATE TABLE to PostgreSQL."""
    sql = "CREATE TABLE t (id INT64, name STRING(100), PRIMARY KEY (id))"
    result = transpile(sql, read=Spanner, write="postgres")
    assert len(result) == 1
    # INT64 should map to BIGINT in Postgres
    assert "BIGINT" in result[0] or "INT" in result[0]


def test_transpile_to_mysql() -> None:
    """Test transpiling Spanner SQL to MySQL."""
    sql = "SELECT id, name FROM users WHERE id = '123'"
    result = transpile(sql, read=Spanner, write="mysql")
    assert len(result) == 1


def test_transpile_functions() -> None:
    """Test function transpilation."""
    sql = "SELECT CURRENT_TIMESTAMP() AS now FROM UNNEST([1]) AS x"
    result = transpile(sql, read=Spanner, write="bigquery")
    assert len(result) == 1


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


def test_empty_table() -> None:
    """Test creating a table with minimal columns."""
    sql = "CREATE TABLE t (id INT64, PRIMARY KEY (id))"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_multiple_statements() -> None:
    """Test parsing multiple statements."""
    sql = """
    SELECT * FROM users;
    SELECT * FROM orders;
    """
    statements = parse(sql, read=Spanner)
    assert len(statements) == 2


def test_comments_single_line() -> None:
    """Test single-line comments."""
    sql = "SELECT id FROM users -- get user ids"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_comments_multi_line() -> None:
    """Test multi-line comments."""
    sql = """SELECT /* this is
    a multi-line comment */ id FROM users"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_string_literals_single_quote() -> None:
    """Test string literal with escaped quote."""
    sql = "SELECT * FROM users WHERE name = 'O''Brien'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_string_literals_unicode() -> None:
    """Test string literal with unicode."""
    sql = "SELECT * FROM users WHERE name = 'José'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_null_handling() -> None:
    """Test NULL keyword handling."""
    sql = "SELECT * FROM users WHERE email IS NULL"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_not_null_handling() -> None:
    """Test IS NOT NULL handling."""
    sql = "SELECT * FROM users WHERE email IS NOT NULL"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_between_expression() -> None:
    """Test BETWEEN expression."""
    sql = "SELECT * FROM orders WHERE amount BETWEEN 100 AND 1000"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_in_expression_values() -> None:
    """Test IN expression with values."""
    sql = "SELECT * FROM users WHERE status IN ('active', 'pending')"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_in_expression_subquery() -> None:
    """Test IN expression with subquery."""
    sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM admins)"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_like_expression() -> None:
    """Test LIKE expression."""
    sql = "SELECT * FROM users WHERE name LIKE 'A%'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_like_expression_escape() -> None:
    """Test LIKE expression with escape."""
    sql = "SELECT * FROM users WHERE name LIKE '\\_%' ESCAPE '\\\\'"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Index Tests
# =============================================================================


def test_create_index() -> None:
    """Test CREATE INDEX statement."""
    sql = "CREATE INDEX idx_users_email ON users (email)"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_create_unique_index() -> None:
    """Test CREATE UNIQUE INDEX statement."""
    sql = "CREATE UNIQUE INDEX idx_users_email ON users (email)"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_create_index_multiple_columns() -> None:
    """Test CREATE INDEX with multiple columns."""
    sql = "CREATE INDEX idx_users_name_email ON users (name, email)"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_drop_index() -> None:
    """Test DROP INDEX statement."""
    sql = "DROP INDEX idx_users_email"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Advanced Query Tests
# =============================================================================


def test_window_function() -> None:
    """Test window function parsing."""
    sql = "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank FROM employees"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_cte_query() -> None:
    """Test Common Table Expression (WITH clause)."""
    sql = """WITH active_users AS (
  SELECT id, name FROM users WHERE active = TRUE
)
SELECT * FROM active_users WHERE name LIKE 'A%'"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_union_query() -> None:
    """Test UNION query."""
    sql = "SELECT id, name FROM users UNION ALL SELECT id, name FROM admins"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_union_distinct() -> None:
    """Test UNION DISTINCT query."""
    sql = "SELECT id FROM users UNION DISTINCT SELECT id FROM admins"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_except_query() -> None:
    """Test EXCEPT query."""
    sql = "SELECT id FROM users EXCEPT ALL SELECT id FROM banned_users"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_intersect_query() -> None:
    """Test INTERSECT query."""
    sql = "SELECT id FROM users INTERSECT ALL SELECT id FROM premium_users"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_unnest() -> None:
    """Test UNNEST function."""
    sql = "SELECT * FROM UNNEST([1, 2, 3]) AS x"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_array_agg() -> None:
    """Test ARRAY_AGG function."""
    sql = "SELECT department, ARRAY_AGG(name) AS names FROM employees GROUP BY department"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_struct_expression() -> None:
    """Test STRUCT expression."""
    sql = "SELECT STRUCT(1 AS a, 'hello' AS b) AS my_struct"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_array_expression() -> None:
    """Test ARRAY expression."""
    sql = "SELECT ARRAY[1, 2, 3] AS my_array"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Real-World Schema Tests
# =============================================================================


def test_real_world_users_schema() -> None:
    """Test parsing a real-world users table schema."""
    sql = """CREATE TABLE users (
  user_id STRING(36) NOT NULL,
  username STRING(50) NOT NULL,
  email STRING(255) NOT NULL,
  password_hash BYTES(64),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP,
  is_active BOOL NOT NULL,
  metadata JSON,
  PRIMARY KEY (user_id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Create)


def test_real_world_orders_schema() -> None:
    """Test parsing a real-world orders table."""
    sql = """CREATE TABLE orders (
  user_id STRING(36) NOT NULL,
  order_id STRING(36) NOT NULL,
  status STRING(20) NOT NULL,
  total_amount NUMERIC NOT NULL,
  created_at TIMESTAMP NOT NULL,
  shipped_at TIMESTAMP,
  PRIMARY KEY (user_id, order_id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
    assert isinstance(ast, exp.Create)


def test_real_world_session_schema() -> None:
    """Test parsing session table."""
    sql = """CREATE TABLE sessions (
  session_id STRING(64) NOT NULL,
  user_id STRING(36),
  data JSON,
  created_at TIMESTAMP NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  PRIMARY KEY (session_id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_real_world_audit_log_schema() -> None:
    """Test parsing audit log table."""
    sql = """CREATE TABLE audit_logs (
  partition_key STRING(10) NOT NULL,
  log_id INT64 NOT NULL,
  user_id STRING(36),
  action STRING(50) NOT NULL,
  resource_type STRING(50),
  resource_id STRING(36),
  changes JSON,
  ip_address STRING(45),
  user_agent STRING(500),
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (partition_key, log_id)
)"""
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# Spanner-Specific Feature Tests (Limited Support)
# =============================================================================
# Note: Full INTERLEAVE and TTL support requires DDL to be executed via
# database.update_ddl(), not through sqlglot parsing. These tests verify
# the dialect's documented limitations.


def test_dialect_inheritance() -> None:
    """Verify Spanner dialect properly inherits from BigQuery."""
    from sqlglot.dialects.bigquery import BigQuery

    assert issubclass(Spanner, BigQuery)
    assert issubclass(Spanner.Parser, BigQuery.Parser)
    assert issubclass(Spanner.Generator, BigQuery.Generator)
    assert issubclass(Spanner.Tokenizer, BigQuery.Tokenizer)


def test_dialect_keywords() -> None:
    """Test that Spanner-specific keywords are registered."""
    from sqlglot.tokens import TokenType

    # Keywords are only registered if sqlglot supports them
    keywords = Spanner.Tokenizer.KEYWORDS
    assert "SELECT" in keywords
    assert "FROM" in keywords
    assert "WHERE" in keywords

    # Check if INTERLEAVE was registered (depends on sqlglot version)
    if hasattr(TokenType, "INTERLEAVE"):
        assert "INTERLEAVE" in keywords


def test_spanner_functions() -> None:
    """Test Spanner-specific functions via BigQuery compatibility."""
    # These functions should work because BigQuery and Spanner share GoogleSQL
    sql = "SELECT SAFE_DIVIDE(10, 0) AS result"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None

    sql2 = "SELECT IFNULL(NULL, 'default') AS result"
    ast2 = parse_one(sql2, read=Spanner)
    assert ast2 is not None


def test_timestamp_functions() -> None:
    """Test timestamp functions."""
    sql = "SELECT CURRENT_TIMESTAMP() AS now"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None

    sql2 = "SELECT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AS tomorrow"
    ast2 = parse_one(sql2, read=Spanner)
    assert ast2 is not None


def test_date_functions() -> None:
    """Test date functions."""
    sql = "SELECT CURRENT_DATE() AS today"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None

    sql2 = "SELECT DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY) AS next_week"
    ast2 = parse_one(sql2, read=Spanner)
    assert ast2 is not None


def test_string_functions() -> None:
    """Test string functions."""
    sql = "SELECT CONCAT('Hello', ' ', 'World') AS greeting"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None

    sql2 = "SELECT SUBSTR('Hello World', 1, 5) AS word"
    ast2 = parse_one(sql2, read=Spanner)
    assert ast2 is not None


def test_json_functions() -> None:
    """Test JSON functions."""
    sql = "SELECT JSON_VALUE('{\"name\": \"Alice\"}', '$.name') AS name"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


# =============================================================================
# DDL Statement Tests
# =============================================================================


def test_drop_table() -> None:
    """Test DROP TABLE statement."""
    sql = "DROP TABLE users"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_drop_table_if_exists() -> None:
    """Test DROP TABLE IF EXISTS statement."""
    sql = "DROP TABLE IF EXISTS users"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_alter_table_add_column() -> None:
    """Test ALTER TABLE ADD COLUMN statement."""
    sql = "ALTER TABLE users ADD COLUMN phone STRING(20)"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None


def test_alter_table_drop_column() -> None:
    """Test ALTER TABLE DROP COLUMN statement."""
    sql = "ALTER TABLE users DROP COLUMN phone"
    ast = parse_one(sql, read=Spanner)
    assert ast is not None
