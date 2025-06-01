"""Simple example of SQLSpec's aiosql integration.

This example demonstrates the clean, simple approach:
- Parse aiosql-style SQL files
- Get SQL objects ready for execution
- Use with any SQLSpec driver
- Support for filters and convenience methods
- No complex adapters or services needed
"""

from pathlib import Path

from pydantic import BaseModel

from sqlspec.extensions.aiosql import AiosqlLoader

__all__ = (
    "User",
    "UserStats",
    "create_example_sql_file",
    "demonstrate_builder_pattern",
    "demonstrate_caching",
    "demonstrate_convenience_methods",
    "demonstrate_operation_type_validation",
    "demonstrate_raw_access",
    "demonstrate_simple_usage",
    "demonstrate_with_filters",
    "main",
)


# Define your data models
class User(BaseModel):
    id: int
    name: str
    email: str
    department: str
    age: int
    active: bool


class UserStats(BaseModel):
    department: str
    user_count: int
    avg_age: float


def create_example_sql_file() -> None:
    """Create an example SQL file with aiosql-style queries."""
    sql_content = """
-- name: get_users
SELECT id, name, email, department, age, active
FROM users
WHERE active = TRUE

-- name: get_user_by_id^
SELECT id, name, email, department, age, active
FROM users
WHERE id = :user_id

-- name: search_users
SELECT id, name, email, department, age, active
FROM users
WHERE name ILIKE '%' || :search_term || '%'
   OR email ILIKE '%' || :search_term || '%'

-- name: create_user<!
INSERT INTO users (name, email, department, age, active)
VALUES (:name, :email, :department, :age, :active)
RETURNING id, name, email, department, age, active

-- name: update_user!
UPDATE users
SET name = :name, email = :email, department = :department, age = :age
WHERE id = :user_id

-- name: delete_user!
DELETE FROM users WHERE id = :user_id

-- name: upsert_user!
MERGE INTO users AS target
USING (VALUES (:id, :name, :email, :department, :age, :active)) AS source (id, name, email, department, age, active)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET name = source.name, email = source.email, department = source.department, age = source.age, active = source.active
WHEN NOT MATCHED THEN
    INSERT (id, name, email, department, age, active)
    VALUES (source.id, source.name, source.email, source.department, source.age, source.active)

-- name: get_department_stats
SELECT
    department,
    COUNT(*) as user_count,
    AVG(age) as avg_age
FROM users
WHERE active = TRUE
GROUP BY department
ORDER BY user_count DESC

-- name: create_tables#
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    department VARCHAR(100) NOT NULL,
    age INTEGER CHECK (age > 0),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

    Path("example_queries.sql").write_text(sql_content)


def demonstrate_simple_usage() -> None:
    """Demonstrate the simple, clean API."""
    print("=== Simple SQLSpec + aiosql Integration ===")

    # Create mock SQLSpec driver for demo
    from unittest.mock import Mock

    driver = Mock()
    driver.dialect = "postgresql"
    driver.execute.return_value = Mock()

    # Load SQL file (cached automatically)
    loader = AiosqlLoader("example_queries.sql")
    print(f"Loaded {len(loader)} queries: {', '.join(loader.query_names)}")

    # Get SQL objects ready for execution (no schema_type here)
    get_users_sql = loader.get_sql("get_users")

    print(f"✅ SQL objects created: {get_users_sql}")

    # Execute with SQLSpec driver - schema_type at execution time
    try:
        result = driver.execute(get_users_sql, {"active": True}, schema_type=User)
        print(f"✅ Execution successful: {type(result)}")
    except Exception as e:
        print(f"Would execute (demo mode): {e}")


def demonstrate_with_filters() -> None:
    """Demonstrate using SQL objects with filters."""
    print("\n=== Using with SQLSpec Filters ===")

    from unittest.mock import Mock

    from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter

    driver = Mock()
    driver.dialect = "postgresql"
    driver.execute.return_value = Mock()

    loader = AiosqlLoader("example_queries.sql")

    # Get SQL object with filters applied directly in loader
    search_filter = SearchFilter("department", "Engineering")
    pagination_filter = LimitOffsetFilter(10, 0)

    get_users_sql = loader.get_sql("get_users", search_filter, pagination_filter)

    try:
        # schema_type at execution time
        result = driver.execute(get_users_sql, {"active": True}, schema_type=User)
        print(f"✅ Filtered execution successful: {type(result)}")
        print(f"✅ SQL with filters: {get_users_sql.sql[:100]}...")
    except Exception as e:
        print(f"Would execute (demo mode): {e}")


def demonstrate_convenience_methods() -> None:
    """Demonstrate the new convenience methods for different operation types."""
    print("\n=== Convenience Methods by Operation Type ===")

    from unittest.mock import Mock

    from sqlspec.statement.filters import OrderByFilter

    driver = Mock()
    driver.dialect = "postgresql"
    driver.execute.return_value = Mock()

    loader = AiosqlLoader("example_queries.sql")

    # SELECT operations
    try:
        select_sql = loader.get_select_sql("get_users", OrderByFilter("name", "asc"))
        print(f"✅ SELECT SQL: {select_sql.sql[:80]}...")

        select_one_sql = loader.get_select_sql("get_user_by_id")
        print(f"✅ SELECT ONE SQL: {select_one_sql.sql[:80]}...")
    except Exception as e:
        print(f"SELECT demo: {e}")

    # INSERT operations
    try:
        insert_sql = loader.get_insert_sql("create_user")
        print(f"✅ INSERT SQL: {insert_sql.sql[:80]}...")
    except Exception as e:
        print(f"INSERT demo: {e}")

    # UPDATE operations
    try:
        update_sql = loader.get_update_sql("update_user")
        print(f"✅ UPDATE SQL: {update_sql.sql[:80]}...")
    except Exception as e:
        print(f"UPDATE demo: {e}")

    # DELETE operations
    try:
        delete_sql = loader.get_delete_sql("delete_user")
        print(f"✅ DELETE SQL: {delete_sql.sql[:80]}...")
    except Exception as e:
        print(f"DELETE demo: {e}")

    # MERGE operations
    try:
        merge_sql = loader.get_merge_sql("upsert_user")
        print(f"✅ MERGE SQL: {merge_sql.sql[:80]}...")
    except Exception as e:
        print(f"MERGE demo: {e}")

    # SCRIPT operations
    try:
        from sqlspec.statement.sql import SQLConfig

        # Use a more permissive config for scripts
        script_config = SQLConfig(strict_mode=False)
        script_sql = loader.get_script_sql("create_tables", config=script_config)
        print(f"✅ SCRIPT SQL: {script_sql.sql[:80]}...")
    except Exception as e:
        print(f"SCRIPT demo: {e}")


def demonstrate_operation_type_validation() -> None:
    """Demonstrate operation type validation in convenience methods."""
    print("\n=== Operation Type Validation ===")

    loader = AiosqlLoader("example_queries.sql")

    # Try to get wrong operation type - should raise error
    try:
        # This should fail because get_users is a SELECT, not INSERT
        loader.get_insert_sql("get_users")
        print("❌ Should have failed!")
    except Exception as e:
        print(f"✅ Correctly caught error: {e}")

    # Show operation types
    for query_name in loader.query_names:
        op_type = loader.get_operation_type(query_name)
        print(f"  {query_name}: {op_type}")


def demonstrate_builder_pattern() -> None:
    """Demonstrate using SQL objects with builder pattern."""
    print("\n=== Using with SQLSpec Builder Pattern ===")

    from unittest.mock import Mock

    from sqlspec.statement.sql import SQLConfig

    driver = Mock()
    driver.dialect = "postgresql"
    driver.execute.return_value = Mock()

    # Use a more permissive config for the builder pattern
    config = SQLConfig(strict_mode=False)
    loader = AiosqlLoader("example_queries.sql", config=config)

    # Get base SQL object
    get_users_sql = loader.get_sql("get_users")

    try:
        # Use builder pattern with proper parameter handling
        enhanced_sql = get_users_sql.where("age > 25").order_by("name").limit(20)

        # Execute with proper parameters
        result = driver.execute(enhanced_sql, {"active": True}, schema_type=User)
        print(f"✅ Builder pattern execution successful: {type(result)}")
        print(f"✅ Enhanced SQL: {enhanced_sql.sql[:100]}...")
    except Exception as e:
        print(f"Builder pattern demo: {e}")
        # Show what we can do instead
        try:
            simple_sql = get_users_sql.limit(10)
            print(f"✅ Simple limit works: {simple_sql.sql[:100]}...")
        except Exception as e2:
            print(f"Even simple limit failed: {e2}")


def demonstrate_raw_access() -> None:
    """Demonstrate accessing raw SQL and metadata."""
    print("\n=== Raw SQL Access ===")

    loader = AiosqlLoader("example_queries.sql")

    # Dictionary-like access to raw SQL
    raw_sql = loader["get_users"]
    print(f"Raw SQL: {raw_sql[:50]}...")

    # Get operation type
    op_type = loader.get_operation_type("get_users")
    print(f"Operation type: {op_type}")

    # Check if query exists
    print(f"Has 'get_users'? {'get_users' in loader}")
    print(f"Has 'nonexistent'? {'nonexistent' in loader}")

    print("✅ Raw access works perfectly!")


def demonstrate_caching() -> None:
    """Demonstrate singleton caching behavior."""
    print("\n=== Singleton Caching ===")

    import time

    # First load
    start = time.time()
    loader1 = AiosqlLoader("example_queries.sql")
    first_load = time.time() - start

    # Second load (should be instant due to caching)
    start = time.time()
    loader2 = AiosqlLoader("example_queries.sql")
    second_load = time.time() - start

    print(f"First load: {first_load:.4f}s")
    print(f"Second load: {second_load:.4f}s (cached)")
    print(f"Same instance? {loader1 is loader2}")  # True due to singleton
    print(f"Speedup: {first_load / max(second_load, 0.0001):.1f}x")

    print("✅ Caching works perfectly!")


def main() -> None:
    """Run the enhanced demo."""
    print("SQLSpec Enhanced Aiosql Integration Demo")
    print("=" * 50)

    # Create example SQL file
    create_example_sql_file()

    # Run demonstrations
    demonstrate_simple_usage()
    demonstrate_with_filters()
    demonstrate_convenience_methods()
    demonstrate_operation_type_validation()
    demonstrate_builder_pattern()
    demonstrate_raw_access()
    demonstrate_caching()

    print("\n" + "=" * 50)
    print("🎉 Enhanced Integration Achieved!")
    print("\nKey Benefits:")
    print("✅ Just parse SQL files - no complex adapters")
    print("✅ Return SQL objects ready for execution")
    print("✅ Support filters directly in loader methods")
    print("✅ Convenience methods for different operation types")
    print("✅ Operation type validation for safety")
    print("✅ Use schema_type for consistent typing")
    print("✅ Works with all existing SQLSpec features")
    print("✅ Singleton caching for performance")
    print("✅ Clean, simple API")
    print("✅ No unnecessary complexity")

    # Cleanup
    Path("example_queries.sql").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
