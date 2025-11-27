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

Vector Distance Functions
=========================

The query builder provides portable vector similarity search functions that generate dialect-specific SQL across PostgreSQL (pgvector), MySQL 9+, Oracle 23ai+, BigQuery, DuckDB, and other databases.

.. note::
   Vector functions are designed for AI/ML similarity search with embedding vectors. The SQL is generated at ``build(dialect=X)`` time, enabling portable query definitions that execute against multiple database types.

Column Methods
--------------

.. py:method:: Column.vector_distance(other_vector, metric="euclidean")

   Calculate vector distance using the specified metric.

   Generates dialect-specific SQL for vector distance operations.

   :param other_vector: Vector to compare against (list, Column reference, or SQLGlot expression)
   :type other_vector: list[float] | Column | exp.Expression
   :param metric: Distance metric to use (default: "euclidean")
   :type metric: str
   :return: FunctionColumn expression for use in SELECT, WHERE, ORDER BY
   :rtype: FunctionColumn

   **Supported Metrics:**

   - ``euclidean`` - L2 distance (default)
   - ``cosine`` - Cosine distance
   - ``inner_product`` - Negative inner product (for similarity ranking)
   - ``euclidean_squared`` - L2² distance (Oracle only)

   **Examples:**

   .. code-block:: python

      from sqlspec import sql
      from sqlspec.builder import Column

      query_vector = [0.1, 0.2, 0.3]

      # Basic distance query
      query = (
          sql.select("id", "title", Column("embedding").vector_distance(query_vector).alias("distance"))
          .from_("documents")
          .where(Column("embedding").vector_distance(query_vector) < 0.5)
          .order_by("distance")
          .limit(10)
      )

      # Using dynamic attribute access
      query = (
          sql.select("*")
          .from_("docs")
          .order_by(sql.embedding.vector_distance(query_vector, metric="cosine"))
          .limit(10)
      )

      # Compare two vector columns
      query = (
          sql.select("*")
          .from_("pairs")
          .where(Column("vec1").vector_distance(Column("vec2"), metric="euclidean") < 0.3)
      )

.. py:method:: Column.cosine_similarity(other_vector)

   Calculate cosine similarity (1 - cosine_distance).

   Convenience method that computes similarity instead of distance.
   Returns values in range [-1, 1] where 1 = identical vectors.

   :param other_vector: Vector to compare against
   :type other_vector: list[float] | Column | exp.Expression
   :return: FunctionColumn expression computing ``1 - cosine_distance(self, other_vector)``
   :rtype: FunctionColumn

   **Example:**

   .. code-block:: python

      from sqlspec import sql

      query_vector = [0.5, 0.5, 0.5]

      # Find most similar documents
      query = (
          sql.select("id", "title", sql.embedding.cosine_similarity(query_vector).alias("similarity"))
          .from_("documents")
          .order_by(sql.column("similarity").desc())
          .limit(10)
      )

Database Compatibility
----------------------

Vector functions generate dialect-specific SQL:

.. list-table::
   :header-rows: 1
   :widths: 15 25 25 35

   * - Database
     - Euclidean
     - Cosine
     - Inner Product
   * - PostgreSQL (pgvector)
     - ``<->`` operator
     - ``<=>`` operator
     - ``<#>`` operator
   * - MySQL 9+
     - ``DISTANCE(..., 'EUCLIDEAN')``
     - ``DISTANCE(..., 'COSINE')``
     - ``DISTANCE(..., 'DOT')``
   * - Oracle 23ai+
     - ``VECTOR_DISTANCE(..., EUCLIDEAN)``
     - ``VECTOR_DISTANCE(..., COSINE)``
     - ``VECTOR_DISTANCE(..., DOT)``
   * - BigQuery
     - ``EUCLIDEAN_DISTANCE(...)``
     - ``COSINE_DISTANCE(...)``
     - ``DOT_PRODUCT(...)``
   * - DuckDB (VSS extension)
     - ``array_distance(...)``
     - ``array_cosine_distance(...)``
     - ``array_negative_inner_product(...)``
   * - Generic
     - ``VECTOR_DISTANCE(..., 'EUCLIDEAN')``
     - ``VECTOR_DISTANCE(..., 'COSINE')``
     - ``VECTOR_DISTANCE(..., 'INNER_PRODUCT')``

Usage Examples
--------------

**Basic Similarity Search**

.. code-block:: python

   from sqlspec import sql

   # Find documents similar to query vector
   query_vector = [0.1, 0.2, 0.3]

   query = (
       sql.select("id", "title", sql.embedding.vector_distance(query_vector).alias("distance"))
       .from_("documents")
       .order_by("distance")
       .limit(10)
   )

   # PostgreSQL generates: SELECT id, title, embedding <-> '[0.1,0.2,0.3]' AS distance ...
   # MySQL generates: SELECT id, title, DISTANCE(embedding, STRING_TO_VECTOR('[0.1,0.2,0.3]'), 'EUCLIDEAN') AS distance ...
   # Oracle generates: SELECT id, title, VECTOR_DISTANCE(embedding, TO_VECTOR('[0.1,0.2,0.3]'), EUCLIDEAN) AS distance ...

**Threshold Filtering**

.. code-block:: python

   # Find documents within distance threshold
   query = (
       sql.select("*")
       .from_("documents")
       .where(sql.embedding.vector_distance(query_vector, metric="euclidean") < 0.5)
       .order_by(sql.embedding.vector_distance(query_vector))
   )

**Similarity Ranking**

.. code-block:: python

   # Rank by cosine similarity (higher = more similar)
   query = (
       sql.select("id", "content", sql.embedding.cosine_similarity(query_vector).alias("score"))
       .from_("articles")
       .order_by(sql.column("score").desc())
       .limit(5)
   )

**Multiple Metrics**

.. code-block:: python

   # Compare different distance metrics in single query
   query = (
       sql.select(
           "id",
           sql.embedding.vector_distance(query_vector, metric="euclidean").alias("l2_dist"),
           sql.embedding.vector_distance(query_vector, metric="cosine").alias("cos_dist"),
           sql.embedding.cosine_similarity(query_vector).alias("similarity")
       )
       .from_("documents")
       .limit(10)
   )

**Combined Filters**

.. code-block:: python

   # Vector search with additional filters
   query = (
       sql.select("*")
       .from_("products")
       .where("category = ?")
       .where("in_stock = TRUE")
       .where(sql.embedding.vector_distance(query_vector) < 0.3)
       .order_by(sql.embedding.vector_distance(query_vector))
       .limit(20)
   )

Dialect-Agnostic Construction
------------------------------

Queries are constructed once and executed against multiple databases:

.. code-block:: python

   from sqlspec import sql

   # Define query once
   query = (
       sql.select("id", "title", sql.embedding.vector_distance([0.1, 0.2, 0.3]).alias("distance"))
       .from_("documents")
       .order_by("distance")
       .limit(10)
   )

   # Execute with different adapters
   pg_result = await pg_session.execute(query)      # → PostgreSQL SQL with <-> operator
   mysql_result = await mysql_session.execute(query)  # → MySQL SQL with DISTANCE()
   oracle_result = await oracle_session.execute(query)  # → Oracle SQL with VECTOR_DISTANCE()

The dialect is selected at ``build(dialect=X)`` time based on the driver, not at query construction time.

Filter Integration
==================

The builder integrates with SQLSpec's filter system:

.. code-block:: python

   from sqlspec.core import LimitOffsetFilter, SearchFilter, OrderByFilter

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

   from sqlspec.core import SQL

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
