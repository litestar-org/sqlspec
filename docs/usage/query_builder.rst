=============
Query Builder
=============

SQLSpec includes an experimental fluent query builder API for programmatically constructing SQL queries. While raw SQL is recommended for most use cases, the query builder is useful for dynamic query construction.

.. warning::

   The Query Builder API is **experimental** and subject to significant changes. Use raw SQL for production-critical queries where API stability is required.

Overview
--------

The query builder provides a fluent, chainable API for constructing SQL statements:

.. code-block:: python

   from sqlspec import sql

   # Build SELECT query
   query = (
       sql.select("id", "name", "email")
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

   from sqlspec import sql

   # Simple select
   query = sql.select("*").from_("users")
   # SQL: SELECT * FROM users

   # Specific columns
   query = sql.select("id", "name", "email").from_("users")
   # SQL: SELECT id, name, email FROM users

   # With table alias
   query = sql.select("u.id", "u.name").from_("users u")
   # SQL: SELECT u.id, u.name FROM users u

WHERE Clauses
^^^^^^^^^^^^^

.. code-block:: python

   # Simple WHERE
   query = sql.select("*").from_("users").where("status = ?")

   # Multiple conditions (AND)
   query = (
       sql.select("*")
       .from_("users")
       .where("status = ?")
       .where("created_at > ?")
   )
   # SQL: SELECT * FROM users WHERE status = ? AND created_at > ?

   # OR conditions
   query = (
       sql.select("*")
       .from_("users")
       .where("status = ? OR role = ?")
   )

   # IN clause
   query = sql.select("*").from_("users").where("id IN (?, ?, ?)")

Dynamic Filtering
^^^^^^^^^^^^^^^^^

Build queries conditionally based on runtime values:

.. code-block:: python

   from sqlspec import sql

   def search_users(name=None, email=None, status=None):
       query = sql.select("id", "name", "email", "status").from_("users")
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
       sql.select("u.id", "u.name", "o.total")
       .from_("users u")
       .join("orders o", "u.id = o.user_id")
   )
   # SQL: SELECT u.id, u.name, o.total FROM users u
   #      INNER JOIN orders o ON u.id = o.user_id

   # LEFT JOIN
   query = (
       sql.select("u.id", "u.name", "COUNT(o.id) as order_count")
       .from_("users u")
       .left_join("orders o", "u.id = o.user_id")
       .group_by("u.id", "u.name")
   )

   # Multiple JOINs
   query = (
       sql.select("u.name", "o.id", "p.name as product")
       .from_("users u")
       .join("orders o", "u.id = o.user_id")
       .join("order_items oi", "o.id = oi.order_id")
       .join("products p", "oi.product_id = p.id")
   )

Ordering and Limiting
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # ORDER BY
   query = sql.select("*").from_("users").order_by("created_at DESC")

   # Multiple order columns
   query = (
       sql.select("*")
       .from_("users")
       .order_by("status ASC", "created_at DESC")
   )

   # LIMIT and OFFSET
   query = sql.select("*").from_("users").limit(10).offset(20)

   # Pagination helper
   def paginate(page=1, per_page=20):
       offset = (page - 1) * per_page
       return (
           sql.select("*")
           .from_("users")
           .order_by("id")
           .limit(per_page)
           .offset(offset)
       )

Aggregations
^^^^^^^^^^^^

.. code-block:: python

   # COUNT
   query = sql.select("COUNT(*) as total").from_("users")

   # GROUP BY
   query = (
       sql.select("status", "COUNT(*) as count")
       .from_("users")
       .group_by("status")
   )

   # HAVING
   query = (
       sql.select("user_id", "COUNT(*) as order_count")
       .from_("orders")
       .group_by("user_id")
       .having("COUNT(*) > ?")
   )

   # Multiple aggregations
   query = (
       sql.select(
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

   # Subquery in WHERE
   subquery = sql.select("id").from_("orders").where("total > ?")
   query = (
       sql.select("*")
       .from_("users")
       .where(f"id IN ({subquery})")
   )

   # Subquery in FROM
   subquery = (
       sql.select("user_id", "COUNT(*) as order_count")
       .from_("orders")
       .group_by("user_id")
   )
   query = (
       sql.select("u.name", "o.order_count")
       .from_("users u")
       .join(f"({subquery}) o", "u.id = o.user_id")
   )

INSERT Queries
--------------

Basic INSERT
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import sql

   # Single row insert
   query = sql.insert("users").columns("name", "email").values("?", "?")
   # SQL: INSERT INTO users (name, email) VALUES (?, ?)

   result = session.execute(query, "Alice", "alice@example.com")

Multiple Rows
^^^^^^^^^^^^^

.. code-block:: python

   # Multiple value sets
   query = (
       sql.insert("users")
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
       sql.insert("users")
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

   from sqlspec import sql

   # Update with WHERE
   query = (
       sql.update("users")
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
       sql.update("users")
       .set("name", "?")
       .set("email", "?")
       .set("updated_at", "CURRENT_TIMESTAMP")
       .where("id = ?")
   )

   session.execute(query, "New Name", "newemail@example.com", 1)

Conditional Updates
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import sql

   # Dynamic update builder
   def update_user(user_id, **fields):
       query = sql.update("users")
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

   from sqlspec import sql

   # Delete with WHERE
   query = sql.delete("users").where("id = ?")
   # SQL: DELETE FROM users WHERE id = ?

   result = session.execute(query, 1)
   print(f"Deleted {result.rows_affected} rows")

Multiple Conditions
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Delete with multiple conditions
   query = (
       sql.delete("users")
       .where("status = ?")
       .where("last_login < ?")
   )

   session.execute(query, "inactive", datetime.date(2024, 1, 1))

DDL Operations
--------------

CREATE TABLE
^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import sql

   # Create table
   query = (
       sql.create_table("users")
       .column("id", "INTEGER PRIMARY KEY")
       .column("name", "TEXT NOT NULL")
       .column("email", "TEXT UNIQUE NOT NULL")
       .column("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
   )

   session.execute(query)

DROP TABLE
^^^^^^^^^^

.. code-block:: python

   # Drop table
   query = sql.drop_table("users")

   # Drop if exists
   query = sql.drop_table("users").if_exists()

   session.execute(query)

CREATE INDEX
^^^^^^^^^^^^

.. code-block:: python

   # Create index
   query = (
       sql.create_index("idx_users_email")
       .on("users")
       .columns("email")
   )

   # Unique index
   query = (
       sql.create_index("idx_users_email")
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

   query = sql.select(
       "id",
       "name",
       "salary",
       "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank"
   ).from_("employees")

CASE Expressions
^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import sql

   case_expr = (
       sql.case()
       .when("status = 'active'", "'Active User'")
       .when("status = 'pending'", "'Pending Approval'")
       .else_("'Inactive'")
   )

   query = sql.select("id", "name", f"{case_expr} as status_label").from_("users")

Common Table Expressions (CTE)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # WITH clause
   cte = sql.select("user_id", "COUNT(*) as order_count").from_("orders").group_by("user_id")

   query = (
       sql.select("u.name", "c.order_count")
       .with_("user_orders", cte)
       .from_("users u")
       .join("user_orders c", "u.id = c.user_id")
   )

Query Composition
-----------------

Reusable Query Components
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import sql

   # Base query
   base_query = sql.select("id", "name", "email", "status").from_("users")

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

   from sqlspec import sql

   class UserQueries:
       @staticmethod
       def by_id():
           return sql.select("*").from_("users").where("id = ?")

       @staticmethod
       def by_email():
           return sql.select("*").from_("users").where("email = ?")

       @staticmethod
       def search(filters):
           query = sql.select("*").from_("users")
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
   result = session.execute(query, *params)
   users = result.all()

Best Practices
--------------

**1. Use Raw SQL for Static Queries**

.. code-block:: python

   # Prefer this for simple, static queries:
   result = session.execute("SELECT * FROM users WHERE id = ?", 1)

   # Over this:
   query = sql.select("*").from_("users").where("id = ?")
   result = session.execute(query, 1)

**2. Builder for Dynamic Queries**

.. code-block:: python

   from sqlspec import sql

   # Good use case: dynamic filtering
   def search_products(category=None, min_price=None, in_stock=None):
       query = sql.select("*").from_("products")
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

   from sqlspec import sql

   # Always use placeholders for user input
   search_term = user_input  # From user
   query = sql.select("*").from_("users").where("name LIKE ?")
   result = session.execute(query, f"%{search_term}%")

**4. Type Safety with Schema Mapping**

.. code-block:: python

   from pydantic import BaseModel
   from sqlspec import sql

   class User(BaseModel):
       id: int
       name: str
       email: str

   query = sql.select("id", "name", "email").from_("users")
   result = session.execute(query)
   users: list[User] = result.all(schema_type=User)

**5. Test Generated SQL**

.. code-block:: python

   from sqlspec import sql

   # Check generated SQL during development
   query = sql.select("*").from_("users").where("id = ?")
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

   from sqlspec import sql

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
       sql.select("u.id", "u.name", "COUNT(o.id) as order_count")
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
