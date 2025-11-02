=======
Builder
=======

The SQL builder provides a fluent, type-safe API for constructing SQL queries programmatically. It supports SELECT, INSERT, UPDATE, DELETE, and complex operations like JOINs, CTEs, and window functions.

.. currentmodule:: sqlspec.builder

.. warning::
   The builder API is **experimental** and subject to breaking changes in future releases. Use with caution in production code.

Overview
========

The builder uses method chaining to construct SQL queries:

.. code-block:: python

   from sqlspec import sql

   query = (
       sql.select("name", "email", "department")
       .from_("users")
       .where("active = TRUE")
       .where(("salary", ">", 50000))
       .order_by("name")
       .limit(10)
   )

   # Convert to SQL statement
   stmt = query.to_statement()

   # Execute with driver
   result = await driver.execute(stmt)

Builder Factory
===============

.. autoclass:: SQLFactory
   :members:
   :undoc-members:
   :show-inheritance:

   The main entry point for building SQL queries.

   **Available as:** ``from sqlspec import sql``

   **Methods:**

   - ``select(*columns)`` - Create SELECT query
   - ``insert(table)`` - Create INSERT query
   - ``update(table)`` - Create UPDATE query
   - ``delete(table)`` - Create DELETE query
   - ``merge(dialect=None)`` - Create MERGE query (PostgreSQL 15+, Oracle, BigQuery)

SELECT Queries
==============

.. autoclass:: Select
   :members:
   :undoc-members:
   :show-inheritance:

   Builder for SELECT statements with support for:

   - Column selection
   - FROM clauses
   - JOINs (INNER, LEFT, RIGHT, FULL)
   - WHERE conditions
   - GROUP BY
   - HAVING
   - ORDER BY
   - LIMIT/OFFSET
   - CTEs (WITH)
   - Window functions
   - Subqueries

Basic SELECT
------------

.. code-block:: python

   # Simple select
   query = sql.select("*").from_("users")

   # Specific columns
   query = sql.select("id", "name", "email").from_("users")

   # Column aliases
   query = sql.select(
       "id",
       "name AS full_name",
       "COUNT(*) OVER () AS total_count"
   ).from_("users")

WHERE Conditions
----------------

Multiple ways to specify conditions:

.. code-block:: python

   # Raw SQL string
   query = sql.select("*").from_("users").where("active = TRUE")

   # Tuple format (column, operator, value)
   query = sql.select("*").from_("users").where(("age", ">=", 18))

   # Multiple conditions (AND)
   query = (
       sql.select("*")
       .from_("users")
       .where("active = TRUE")
       .where(("age", ">=", 18))
       .where(("department", "=", "Engineering"))
   )

   # OR conditions
   query = sql.select("*").from_("users").where_or(
       ("role", "=", "admin"),
       ("role", "=", "moderator")
   )

JOINs
-----

.. code-block:: python

   # INNER JOIN
   query = (
       sql.select("u.name", "o.total", "p.name as product")
       .from_("users u")
       .inner_join("orders o", "u.id = o.user_id")
       .inner_join("products p", "o.product_id = p.id")
   )

   # LEFT JOIN
   query = (
       sql.select("u.name", "COUNT(o.id) as order_count")
       .from_("users u")
       .left_join("orders o", "u.id = o.user_id")
       .group_by("u.id", "u.name")
   )

   # Multiple JOIN types
   query = (
       sql.select("*")
       .from_("users u")
       .inner_join("orders o", "u.id = o.user_id")
       .left_join("reviews r", "o.id = r.order_id")
       .right_join("products p", "o.product_id = p.id")
   )

Aggregation
-----------

.. code-block:: python

   # GROUP BY with aggregates
   query = (
       sql.select(
           "department",
           "COUNT(*) as employee_count",
           "AVG(salary) as avg_salary",
           "MAX(salary) as max_salary"
       )
       .from_("users")
       .group_by("department")
       .having("COUNT(*) > 5")
       .order_by("avg_salary DESC")
   )

CTEs (WITH Clauses)
-------------------

.. code-block:: python

   # Simple CTE
   high_earners = (
       sql.select("id", "name", "salary")
       .from_("users")
       .where(("salary", ">", 100000))
   )

   query = (
       sql.select("*")
       .with_("high_earners", high_earners)
       .from_("high_earners")
       .order_by("salary DESC")
   )

   # Multiple CTEs
   dept_stats = sql.select(
       "department",
       "AVG(salary) as avg_salary"
   ).from_("users").group_by("department")

   user_ranks = sql.select(
       "id", "name", "salary",
       "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank"
   ).from_("users")

   query = (
       sql.select("u.name", "u.salary", "d.avg_salary")
       .with_("dept_stats", dept_stats)
       .with_("user_ranks", user_ranks)
       .from_("user_ranks u")
       .inner_join("dept_stats d", "u.department = d.department")
       .where("u.rank <= 3")
   )

Subqueries
----------

.. code-block:: python

   # Subquery in WHERE
   high_value_customers = (
       sql.select("user_id")
       .from_("orders")
       .group_by("user_id")
       .having("SUM(total_amount) > 10000")
   )

   query = (
       sql.select("*")
       .from_("users")
       .where_in("id", high_value_customers)
   )

   # EXISTS subquery
   has_orders = (
       sql.select("1")
       .from_("orders")
       .where("orders.user_id = users.id")
   )

   query = (
       sql.select("*")
       .from_("users")
       .where_exists(has_orders)
   )

INSERT Queries
==============

.. autoclass:: Insert
   :members:
   :undoc-members:
   :show-inheritance:

   Builder for INSERT statements.

.. code-block:: python

   # Insert single row
   query = (
       sql.insert("users")
       .values(name="Alice", email="alice@example.com", age=30)
   )

   # Insert multiple rows
   query = (
       sql.insert("users")
       .columns("name", "email", "age")
       .values(
           ("Alice", "alice@example.com", 30),
           ("Bob", "bob@example.com", 25),
           ("Charlie", "charlie@example.com", 35)
       )
   )

   # INSERT with RETURNING
   query = (
       sql.insert("users")
       .values(name="Alice", email="alice@example.com")
       .returning("id", "created_at")
   )

   # INSERT from SELECT
   query = (
       sql.insert("archived_users")
       .columns("id", "name", "email")
       .from_select(
           sql.select("id", "name", "email")
           .from_("users")
           .where("last_login < NOW() - INTERVAL '1 year'")
       )
   )

UPDATE Queries
==============

.. autoclass:: Update
   :members:
   :undoc-members:
   :show-inheritance:

   Builder for UPDATE statements.

.. code-block:: python

   # Simple update
   query = (
       sql.update("users")
       .set(active=False)
       .where(("last_login", "<", "2024-01-01"))
   )

   # Multiple columns
   query = (
       sql.update("users")
       .set(
           active=True,
           last_login="NOW()",
           login_count="login_count + 1"
       )
       .where(("id", "=", 123))
   )

   # UPDATE with JOIN
   query = (
       sql.update("users u")
       .set(premium=True)
       .from_("orders o")
       .where("u.id = o.user_id")
       .where(("o.total_amount", ">", 10000))
   )

   # UPDATE with RETURNING
   query = (
       sql.update("users")
       .set(salary="salary * 1.1")
       .where(("department", "=", "Engineering"))
       .returning("id", "name", "salary")
   )

DELETE Queries
==============

.. autoclass:: Delete
   :members:
   :undoc-members:
   :show-inheritance:

   Builder for DELETE statements.

.. code-block:: python

   # Simple delete
   query = sql.delete("users").where(("active", "=", False))

   # Delete with multiple conditions
   query = (
       sql.delete("users")
       .where("last_login < NOW() - INTERVAL '2 years'")
       .where(("email_verified", "=", False))
   )

   # DELETE with RETURNING
   query = (
       sql.delete("users")
       .where(("id", "=", 123))
       .returning("id", "name", "email")
   )

MERGE Queries (UPSERT)
======================

.. autoclass:: Merge
   :members:
   :undoc-members:
   :show-inheritance:

   Builder for MERGE statements (INSERT or UPDATE based on condition).

   **Database Support:**

   - ✅ PostgreSQL 15+
   - ✅ Oracle 9i+
   - ✅ BigQuery
   - ❌ MySQL (use INSERT ... ON DUPLICATE KEY UPDATE)
   - ❌ SQLite (use INSERT ... ON CONFLICT)

Basic MERGE
-----------

.. code-block:: python

   # Simple upsert from dict
   query = (
       sql.merge(dialect="postgres")
       .into("products", alias="t")
       .using({"id": 1, "name": "Product A", "price": 19.99}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name", price="src.price")
       .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
   )

   # MERGE from table source
   query = (
       sql.merge(dialect="postgres")
       .into("products", alias="t")
       .using("staging_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(name="s.name", price="s.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
   )

Conditional MERGE
-----------------

.. code-block:: python

   # Update only if condition met
   query = (
       sql.merge(dialect="postgres")
       .into("products", alias="t")
       .using(staging_data, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(
           condition="t.price < src.price",
           price="src.price"
       )
       .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
   )

   # Delete matched rows conditionally
   query = (
       sql.merge(dialect="postgres")
       .into("products", alias="t")
       .using({"id": 1, "discontinued": True}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_delete(condition="src.discontinued = TRUE")
   )

SQL Server Extensions
---------------------

SQL Server supports additional WHEN NOT MATCHED BY SOURCE clauses:

.. code-block:: python

   # SQL Server: Handle rows in target not in source
   query = (
       sql.merge(dialect="tsql")
       .into("products", alias="t")
       .using(current_products, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name", price="src.price")
       .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
       .when_not_matched_by_source_then_delete()  # Delete obsolete products
   )

NULL Value Handling
-------------------

MERGE automatically handles NULL values in source data:

.. code-block:: python

   # NULL values are properly typed
   query = (
       sql.merge(dialect="postgres")
       .into("products", alias="t")
       .using({"id": 1, "name": "Updated", "price": None}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name", price="src.price")
   )
   # Sets price to NULL if matched

.. note::
   When all values for a column are NULL, PostgreSQL defaults to NUMERIC type.
   For other column types, provide at least one non-NULL value for accurate type inference.

Query Mixins
============

Base Classes
------------

.. autoclass:: QueryBase
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SelectMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: WhereMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: JoinMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: GroupByMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: OrderByMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: LimitOffsetMixin
   :members:
   :undoc-members:
   :show-inheritance:

Filter Integration
==================

The builder integrates with SQLSpec's filter system:

.. code-block:: python

   from sqlspec.core.filters import LimitOffsetFilter, SearchFilter, OrderByFilter

   # Base query
   query = sql.select("*").from_("users")

   # Apply filters
   filtered = query.append_filter(
       SearchFilter("name", "John"),
       LimitOffsetFilter(10, 0),
       OrderByFilter("created_at", "desc")
   )

   # Execute
   stmt = filtered.to_statement()

Statement Conversion
====================

Convert builder to executable SQL:

.. code-block:: python

   from sqlspec.core.statement import SQL

   # Build query
   query = sql.select("*").from_("users").limit(10)

   # Convert to SQL statement
   stmt = query.to_statement()

   # SQL object has .sql and .parameters
   print(stmt.sql)  # "SELECT * FROM users LIMIT 10"
   print(stmt.parameters)  # None or parameter dict

   # Execute with driver
   async with db.provide_session(config) as session:
       result = await session.execute(stmt)

See Also
========

- :doc:`/usage/query_builder` - Builder usage guide
- :doc:`core` - SQL statement and filter system
- :doc:`/examples/index` - Code examples including standalone demo
- :doc:`driver` - Query execution
