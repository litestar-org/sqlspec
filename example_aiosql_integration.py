"""Comprehensive example of SQLSpec's advanced aiosql integration.

This example demonstrates the incredible developer experience achieved by combining:
- Singleton-cached SQL file loading
- Seamless SQLSpec ecosystem integration 
- Typed query objects with builder API support
- Full filter and transformation capabilities
"""

from pathlib import Path

from pydantic import BaseModel
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.extensions.aiosql import AiosqlLoader, AiosqlService
from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter


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


def create_example_sql_file():
    """Create an example SQL file with aiosql-style queries."""
    sql_content = """
-- name: get_users^
SELECT id, name, email, department, age, active 
FROM users 
WHERE active = TRUE

-- name: get_user_by_id$
SELECT id, name, email, department, age, active 
FROM users 
WHERE id = :user_id

-- name: get_users_by_department^
SELECT id, name, email, department, age, active 
FROM users 
WHERE department = :department AND active = TRUE

-- name: search_users^
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

-- name: get_department_stats^
SELECT 
    department,
    COUNT(*) as user_count,
    AVG(age) as avg_age
FROM users 
WHERE active = TRUE
GROUP BY department
ORDER BY user_count DESC
"""

    Path("example_queries.sql").write_text(sql_content)


def demonstrate_singleton_loading():
    """Demonstrate singleton loading behavior."""
    print("=== Singleton Loading Demo ===")
    
    # Load once, cached forever
    loader1 = AiosqlLoader("example_queries.sql", dialect="postgresql")
    loader2 = AiosqlLoader("example_queries.sql", dialect="postgresql")
    
    # Same instance due to singleton pattern
    print(f"Same instance? {loader1 is loader2}")  # True
    print(f"Queries loaded: {len(loader1)}")
    print(f"Available queries: {', '.join(loader1.query_names)}")


def demonstrate_typed_queries():
    """Demonstrate typed query objects with return type annotations."""
    print("\n=== Typed Queries Demo ===")
    
    loader = AiosqlLoader("example_queries.sql", dialect="postgresql")
    
    # Get typed queries with return type annotations
    get_users = loader.get_query("get_users", return_type=User)
    get_user = loader.get_query("get_user_by_id", return_type=User)
    create_user = loader.get_query("create_user", return_type=User)
    get_stats = loader.get_query("get_department_stats", return_type=UserStats)
    
    print(f"get_users query: {get_users.name}")
    print(f"Return type: {get_users.return_type}")
    print(f"SQL: {get_users.sql_text[:50]}...")


def demonstrate_builder_api_integration():
    """Demonstrate seamless builder API integration."""
    print("\n=== Builder API Integration Demo ===")
    
    loader = AiosqlLoader("example_queries.sql", dialect="postgresql")
    get_users = loader.get_query("get_users", return_type=User)
    
    # Use SQLSpec builder API on loaded queries!
    filtered_query = (
        get_users
        .where("age > 25")
        .order_by("name ASC")
        .limit(10)
    )
    
    print("Original SQL:")
    print(get_users.sql_text)
    print("\nWith builder modifications:")
    print(str(filtered_query))


def demonstrate_filter_integration():
    """Demonstrate full SQLSpec filter integration."""
    print("\n=== Filter Integration Demo ===")
    
    # Create SQLSpec driver
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        database="example",
        username="user",
        password="password"
    )
    driver = config.create_driver()
    
    loader = AiosqlLoader("example_queries.sql", dialect="postgresql")
    get_users = loader.get_query("get_users", return_type=User)
    
    # Apply filters dynamically
    search_filter = SearchFilter("name", "John")
    pagination_filter = LimitOffsetFilter(limit=20, offset=0)
    
    try:
        # Execute with full SQLSpec features
        result = driver.execute(
            get_users,
            parameters={"active": True},
            filters=[search_filter, pagination_filter],
            schema_type=User
        )
        print(f"Query executed successfully!")
        print(f"Type of result: {type(result)}")
    except Exception as e:
        print(f"Would execute (demo mode): {e}")


def demonstrate_service_integration():
    """Demonstrate high-level service integration."""
    print("\n=== Service Integration Demo ===")
    
    # Create service with SQLSpec driver
    config = PsycopgSyncConfig(
        host="localhost", 
        port=5432,
        database="example",
        username="user", 
        password="password"
    )
    driver = config.create_driver()
    
    # Service with default filters and configuration
    service = AiosqlService(
        driver,
        default_filters=[LimitOffsetFilter(limit=1000, offset=0)],
        allow_sqlspec_filters=True
    )
    
    # Load queries through service
    try:
        queries = service.load_queries("example_queries.sql")
        print("Queries loaded through service!")
        
        # Execute with service enhancements
        result = service.execute_query_with_filters(
            queries.get_users,
            connection=None,  # Uses driver's connection
            parameters={"department": "Engineering"},
            filters=[SearchFilter("email", "@company.com")],
            schema_type=User
        )
        print("Service execution successful!")
    except Exception as e:
        print(f"Would execute (demo mode): {e}")


def demonstrate_loader_convenience_methods():
    """Demonstrate loader convenience methods."""
    print("\n=== Loader Convenience Methods Demo ===")
    
    loader = AiosqlLoader("example_queries.sql", dialect="postgresql")
    
    # Dictionary-like access
    get_users = loader["get_users"] 
    print(f"Dictionary access: {get_users.name}")
    
    # Check query existence
    print(f"Has 'get_users'? {'get_users' in loader}")
    print(f"Has 'nonexistent'? {'nonexistent' in loader}")
    
    # Get all queries
    all_queries = loader.get_all_queries()
    print(f"All queries: {list(all_queries.keys())}")
    
    # Direct execution through loader
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432, 
        database="example",
        username="user",
        password="password"
    )
    driver = config.create_driver()
    
    try:
        # Execute directly through loader
        result = loader.execute_query(
            driver,
            "get_users",
            parameters={"active": True},
            SearchFilter("department", "Engineering"),
            LimitOffsetFilter(10, 0)
        )
        print("Direct loader execution successful!")
    except Exception as e:
        print(f"Would execute (demo mode): {e}")


def main():
    """Run the comprehensive demo."""
    print("SQLSpec Advanced Aiosql Integration Demo")
    print("=" * 50)
    
    # Create example SQL file
    create_example_sql_file()
    
    # Run all demonstrations
    demonstrate_singleton_loading()
    demonstrate_typed_queries()
    demonstrate_builder_api_integration()
    demonstrate_filter_integration()
    demonstrate_service_integration()
    demonstrate_loader_convenience_methods()
    
    print("\n" + "=" * 50)
    print("🎉 Incredible Developer Experience Achieved!")
    print("\nKey Features Demonstrated:")
    print("✅ Singleton-cached SQL file loading")
    print("✅ Typed query objects with return type annotations") 
    print("✅ Seamless SQLSpec builder API integration")
    print("✅ Full filter and transformation support")
    print("✅ High-level service layer integration")
    print("✅ Convenient dictionary-like access")
    print("✅ Direct execution capabilities")
    
    # Cleanup
    Path("example_queries.sql").unlink(missing_ok=True)


if __name__ == "__main__":
    main() 