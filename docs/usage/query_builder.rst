====================
Query Builder (Beta)
====================

SQLSpec includes an experimental fluent query builder API for programmatically constructing SQL queries. While raw SQL is recommended for most use cases, the query builder is useful for dynamic query construction.

.. warning::

   The Query Builder API is **experimental** and subject to significant changes. Use raw SQL for production-critical queries where API stability is required.

Overview
--------

The query builder provides a fluent, chainable API for constructing SQL statements:

.. code-block:: python

   from sqlspec.builder import Select, Insert, Update, Delete

   # Build SELECT query
   query = (
       Select("id", "name", "email")
       .from_("users")
       .where("status = ?")
       .order_by("created_at DESC")
       .limit(10)
   )

   # Execute with session
   result = session.execute(query, "active")

Why Use the Query Builder?
---------------------------

**Benefits**

- Type-safe query construction
- Reusable query components
- Dynamic filtering
- Protection against syntax errors
- IDE autocomplete support

**When to Use**

- Complex dynamic queries with conditional filters
- Query templates with variable components
- Programmatic query generation
- API query builders (search, filtering)

**When to Use Raw SQL Instead**

- Static, well-defined queries
- Complex joins and subqueries
- Database-specific features
- Performance-critical queries
- Queries loaded from SQL files

SELECT Queries
--------------

Basic SELECT
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Select

   # Simple select
   query = Select("*").from_("users")
   # SQL: SELECT * FROM users

   # Specific columns
   query = Select("id", "name", "email").from_("users")
   # SQL: SELECT id, name, email FROM users

   # With table alias
   query = Select("u.id", "u.name").from_("users u")
   # SQL: SELECT u.id, u.name FROM users u

WHERE Clauses
^^^^^^^^^^^^^

.. code-block:: python

   # Simple WHERE
   query = Select("*").from_("users").where("status = ?")

   # Multiple conditions (AND)
   query = (
       Select("*")
       .from_("users")
       .where("status = ?")
       .where("created_at > ?")
   )
   # SQL: SELECT * FROM users WHERE status = ? AND created_at > ?

   # OR conditions
   query = (
       Select("*")
       .from_("users")
       .where("status = ? OR role = ?")
   )

   # IN clause
   query = Select("*").from_("users").where("id IN (?, ?, ?)")

Dynamic Filtering
^^^^^^^^^^^^^^^^^

Build queries conditionally based on runtime values:

.. code-block:: python

   def search_users(name=None, email=None, status=None):
       query = Select("id", "name", "email", "status").from_("users")
       params = []

       if name:
           query = query.where("name LIKE ?")
           params.append(f"%{name}%")

       if email:
           query = query.where("email = ?")
           params.append(email)

       if status:
           query = query.where("status = ?")
           params.append(status)

       return session.execute(query, *params)

   # Usage
   users = search_users(name="Alice", status="active")

JOINs
^^^^^

.. code-block:: python

   # INNER JOIN
   query = (
       Select("u.id", "u.name", "o.total")
       .from_("users u")
       .join("orders o", "u.id = o.user_id")
   )
   # SQL: SELECT u.id, u.name, o.total FROM users u
   #      INNER JOIN orders o ON u.id = o.user_id

   # LEFT JOIN
   query = (
       Select("u.id", "u.name", "COUNT(o.id) as order_count")
       .from_("users u")
       .left_join("orders o", "u.id = o.user_id")
       .group_by("u.id", "u.name")
   )

   # Multiple JOINs
   query = (
       Select("u.name", "o.id", "p.name as product")
       .from_("users u")
       .join("orders o", "u.id = o.user_id")
       .join("order_items oi", "o.id = oi.order_id")
       .join("products p", "oi.product_id = p.id")
   )

Ordering and Limiting
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # ORDER BY
   query = Select("*").from_("users").order_by("created_at DESC")

   # Multiple order columns
   query = (
       Select("*")
       .from_("users")
       .order_by("status ASC", "created_at DESC")
   )

   # LIMIT and OFFSET
   query = Select("*").from_("users").limit(10).offset(20)

   # Pagination helper
   def paginate(page=1, per_page=20):
       offset = (page - 1) * per_page
       return (
           Select("*")
           .from_("users")
           .order_by("id")
           .limit(per_page)
           .offset(offset)
       )

Aggregations
^^^^^^^^^^^^

.. code-block:: python

   # COUNT
   query = Select("COUNT(*) as total").from_("users")

   # GROUP BY
   query = (
       Select("status", "COUNT(*) as count")
       .from_("users")
       .group_by("status")
   )

   # HAVING
   query = (
       Select("user_id", "COUNT(*) as order_count")
       .from_("orders")
       .group_by("user_id")
       .having("COUNT(*) > ?")
   )

   # Multiple aggregations
   query = (
       Select(
           "DATE(created_at) as date",
           "COUNT(*) as orders",
           "SUM(total) as revenue"
       )
       .from_("orders")
       .group_by("DATE(created_at)")
   )

Subqueries
^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import SubqueryBuilder

   # Subquery in WHERE
   subquery = Select("id").from_("orders").where("total > ?")
   query = (
       Select("*")
       .from_("users")
       .where(f"id IN ({subquery})")
   )

   # Subquery in FROM
   subquery = (
       Select("user_id", "COUNT(*) as order_count")
       .from_("orders")
       .group_by("user_id")
   )
   query = (
       Select("u.name", "o.order_count")
       .from_("users u")
       .join(f"({subquery}) o", "u.id = o.user_id")
   )

INSERT Queries
--------------

Basic INSERT
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Insert

   # Single row insert
   query = Insert("users").columns("name", "email").values("?", "?")
   # SQL: INSERT INTO users (name, email) VALUES (?, ?)

   result = session.execute(query, "Alice", "alice@example.com")

Multiple Rows
^^^^^^^^^^^^^

.. code-block:: python

   # Multiple value sets
   query = (
       Insert("users")
       .columns("name", "email")
       .values("?", "?")
       .values("?", "?")
       .values("?", "?")
   )

   session.execute(
       query,
       "Alice", "alice@example.com",
       "Bob", "bob@example.com",
       "Charlie", "charlie@example.com"
   )

INSERT with RETURNING
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # PostgreSQL RETURNING clause
   query = (
       Insert("users")
       .columns("name", "email")
       .values("?", "?")
       .returning("id", "created_at")
   )

   result = session.execute(query, "Alice", "alice@example.com")
   new_user = result.one()
   print(f"Created user ID: {new_user['id']}")

UPDATE Queries
--------------

Basic UPDATE
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Update

   # Update with WHERE
   query = (
       Update("users")
       .set("email", "?")
       .where("id = ?")
   )
   # SQL: UPDATE users SET email = ? WHERE id = ?

   result = session.execute(query, "newemail@example.com", 1)
   print(f"Updated {result.rows_affected} rows")

Multiple Columns
^^^^^^^^^^^^^^^^

.. code-block:: python

   # Update multiple columns
   query = (
       Update("users")
       .set("name", "?")
       .set("email", "?")
       .set("updated_at", "CURRENT_TIMESTAMP")
       .where("id = ?")
   )

   session.execute(query, "New Name", "newemail@example.com", 1)

Conditional Updates
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Dynamic update builder
   def update_user(user_id, **fields):
       query = Update("users")
       params = []

       for field, value in fields.items():
           query = query.set(field, "?")
           params.append(value)

       query = query.where("id = ?")
       params.append(user_id)

       return session.execute(query, *params)

   # Usage
   update_user(1, name="Alice", email="alice@example.com", status="active")

DELETE Queries
--------------

Basic DELETE
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Delete

   # Delete with WHERE
   query = Delete("users").where("id = ?")
   # SQL: DELETE FROM users WHERE id = ?

   result = session.execute(query, 1)
   print(f"Deleted {result.rows_affected} rows")

Multiple Conditions
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Delete with multiple conditions
   query = (
       Delete("users")
       .where("status = ?")
       .where("last_login < ?")
   )

   session.execute(query, "inactive", datetime.date(2024, 1, 1))

DDL Operations
--------------

CREATE TABLE
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import CreateTable

   # Create table
   query = (
       CreateTable("users")
       .column("id", "INTEGER PRIMARY KEY")
       .column("name", "TEXT NOT NULL")
       .column("email", "TEXT UNIQUE NOT NULL")
       .column("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
   )

   session.execute(query)

DROP TABLE
^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import DropTable

   # Drop table
   query = DropTable("users")

   # Drop if exists
   query = DropTable("users").if_exists()

   session.execute(query)

CREATE INDEX
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import CreateIndex

   # Create index
   query = (
       CreateIndex("idx_users_email")
       .on("users")
       .columns("email")
   )

   # Unique index
   query = (
       CreateIndex("idx_users_email")
       .on("users")
       .columns("email")
       .unique()
   )

   session.execute(query)

Advanced Features
-----------------

Window Functions
^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Select

   query = Select(
       "id",
       "name",
       "salary",
       "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank"
   ).from_("employees")

CASE Expressions
^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.builder import Case

   case_expr = (
       Case()
       .when("status = 'active'", "'Active User'")
       .when("status = 'pending'", "'Pending Approval'")
       .else_("'Inactive'")
   )

   query = Select("id", "name", f"{case_expr} as status_label").from_("users")

Common Table Expressions (CTE)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # WITH clause
   cte = Select("user_id", "COUNT(*) as order_count").from_("orders").group_by("user_id")

   query = (
       Select("u.name", "c.order_count")
       .with_("user_orders", cte)
       .from_("users u")
       .join("user_orders c", "u.id = c.user_id")
   )

Query Composition
-----------------

Reusable Query Components
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Base query
   base_query = Select("id", "name", "email", "status").from_("users")

   # Add filters based on context
   def active_users():
       return base_query.where("status = 'active'")

   def recent_users(days=7):
       return base_query.where("created_at >= ?")

   # Use in different contexts
   active = session.execute(active_users())
   recent = session.execute(recent_users(), datetime.date.today() - datetime.timedelta(days=7))

Query Templates
^^^^^^^^^^^^^^^

.. code-block:: python

   class UserQueries:
       @staticmethod
       def by_id():
           return Select("*").from_("users").where("id = ?")

       @staticmethod
       def by_email():
           return Select("*").from_("users").where("email = ?")

       @staticmethod
       def search(filters):
           query = Select("*").from_("users")
           params = []

           if "name" in filters:
               query = query.where("name LIKE ?")
               params.append(f"%{filters['name']}%")

           if "status" in filters:
               query = query.where("status = ?")
               params.append(filters["status"])

           return query, params

   # Usage
   user = session.execute(UserQueries.by_id(), 1).one()
   query, params = UserQueries.search({"name": "Alice", "status": "active"})
   users = session.execute(query, *params).data

Best Practices
--------------

**1. Use Raw SQL for Static Queries**

.. code-block:: python

   # Prefer this for simple, static queries:
   result = session.execute("SELECT * FROM users WHERE id = ?", 1)

   # Over this:
   query = Select("*").from_("users").where("id = ?")
   result = session.execute(query, 1)

**2. Builder for Dynamic Queries**

.. code-block:: python

   # Good use case: dynamic filtering
   def search_products(category=None, min_price=None, in_stock=None):
       query = Select("*").from_("products")
       params = []

       if category:
           query = query.where("category_id = ?")
           params.append(category)

       if min_price:
           query = query.where("price >= ?")
           params.append(min_price)

       if in_stock:
           query = query.where("stock > 0")

       return session.execute(query, *params)

**3. Parameterize User Input**

.. code-block:: python

   # Always use placeholders for user input
   search_term = user_input  # From user
   query = Select("*").from_("users").where("name LIKE ?")
   result = session.execute(query, f"%{search_term}%")

**4. Type Safety with Schema Mapping**

.. code-block:: python

   from pydantic import BaseModel

   class User(BaseModel):
       id: int
       name: str
       email: str

   query = Select("id", "name", "email").from_("users")
   users: list[User] = session.execute(query, schema_type=User).to_schema()

**5. Test Generated SQL**

.. code-block:: python

   # Check generated SQL during development
   query = Select("*").from_("users").where("id = ?")
   print(query)  # Shows generated SQL

Limitations
-----------

The query builder has some limitations:

**Complex Subqueries**

For very complex subqueries, raw SQL is often clearer:

.. code-block:: python

   # This is easier to read as raw SQL:
   result = session.execute("""
       WITH ranked_users AS (
           SELECT id, name,
                  ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at DESC) as rn
           FROM users
       )
       SELECT * FROM ranked_users WHERE rn <= 5
   """)

**Database-Specific Features**

Database-specific syntax may not be supported:

.. code-block:: python

   # PostgreSQL JSON operators (use raw SQL)
   session.execute("SELECT data->>'name' FROM events WHERE data @> ?", json_filter)

**Performance**

The builder adds minimal overhead, but raw SQL is always fastest for known queries.

Migration from Raw SQL
----------------------

When migrating from raw SQL to the query builder:

.. code-block:: python

   # Before: Raw SQL
   result = session.execute("""
       SELECT u.id, u.name, COUNT(o.id) as order_count
       FROM users u
       LEFT JOIN orders o ON u.id = o.user_id
       WHERE u.status = ?
       GROUP BY u.id, u.name
       HAVING COUNT(o.id) > ?
       ORDER BY order_count DESC
       LIMIT ?
   """, "active", 5, 10)

   # After: Query Builder
   query = (
       Select("u.id", "u.name", "COUNT(o.id) as order_count")
       .from_("users u")
       .left_join("orders o", "u.id = o.user_id")
       .where("u.status = ?")
       .group_by("u.id", "u.name")
       .having("COUNT(o.id) > ?")
       .order_by("order_count DESC")
       .limit("?")
   )
   result = session.execute(query, "active", 5, 10)

Only migrate queries that benefit from dynamic construction.

Next Steps
----------

- :doc:`sql_files` - Load queries from SQL files (recommended for static queries)
- :doc:`drivers_and_querying` - Execute built queries with drivers
- :doc:`../reference/builder` - Complete query builder API reference

See Also
--------

- :doc:`data_flow` - Understanding query processing
- :doc:`configuration` - Configure statement processing
- :doc:`../examples/index` - Example queries and patterns
