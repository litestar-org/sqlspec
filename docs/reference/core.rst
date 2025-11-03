====
Core
====

The core module contains the fundamental components for SQL processing, including statement handling, parameter binding, result processing, compilation, and caching.

.. currentmodule:: sqlspec.core

Overview
========

Core components:

- **SQL Statement** (``statement.py``) - SQL wrapper with metadata and parameter binding
- **Parameters** (``parameters.py``) - Parameter style conversion and binding
- **Results** (``result.py``) - Result set handling and type mapping
- **Compiler** (``compiler.py``) - SQL compilation and validation using sqlglot
- **Cache** (``cache.py``) - Statement caching for performance
- **Filters** (``filters.py``) - SQL transformation filters

SQL Statement
=============

.. currentmodule:: sqlspec.core.statement

.. autoclass:: SQL
   :members:
   :undoc-members:
   :show-inheritance:

   The main SQL statement class that wraps raw SQL with parameters.

   **Features:**

   - Parameter binding (positional and named)
   - SQL compilation and validation
   - Statement metadata
   - Cache integration
   - Filter application

   **Usage:**

   .. code-block:: python

      from sqlspec.core import SQL

      # Simple SQL
      stmt = SQL("SELECT * FROM users")

      # With positional parameters
      stmt = SQL("SELECT * FROM users WHERE id = ?", 123)

      # With named parameters
      stmt = SQL("SELECT * FROM users WHERE id = :user_id", user_id=123)

      # With keyword parameters
      stmt = SQL("SELECT * FROM users WHERE id = :id", id=123)

      # Access SQL and parameters
      print(stmt.sql)         # "SELECT * FROM users WHERE id = :id"
      print(stmt.parameters)  # {"id": 123}

.. autoclass:: StatementConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for SQL statement processing.

   **Options:**

   - ``enable_validation`` - Validate SQL syntax
   - ``enable_analysis`` - Analyze SQL for optimization hints
   - ``enable_transformations`` - Apply SQL transformations
   - ``cache_statements`` - Cache compiled statements

Parameter Handling
==================

.. currentmodule:: sqlspec.core.parameters

.. autoclass:: ParameterProcessor
   :members:
   :undoc-members:
   :show-inheritance:

   Processes SQL parameters by converting between different parameter styles.

   **Supported parameter styles:**

   - ``ParameterStyle.QMARK`` - ``?`` (SQLite/DuckDB positional)
   - ``ParameterStyle.NUMERIC`` - ``$1, $2`` (PostgreSQL positional for asyncpg, psqlpy)
   - ``ParameterStyle.POSITIONAL_PYFORMAT`` - ``%s`` (PostgreSQL/MySQL format style)
   - ``ParameterStyle.NAMED_COLON`` - ``:name`` (Named parameters for Oracle, SQLite)
   - ``ParameterStyle.NAMED_AT`` - ``@name`` (BigQuery named parameters)
   - ``ParameterStyle.NAMED_PYFORMAT`` - ``%(name)s`` (Python format style)

.. autoclass:: ParameterConverter
   :members:
   :undoc-members:
   :show-inheritance:

   Converts parameters between different styles.

.. autoclass:: ParameterValidator
   :members:
   :undoc-members:
   :show-inheritance:

   Validates parameter format and style.

.. autoclass:: ParameterStyleConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for parameter style handling.

.. autoclass:: ParameterStyle
   :members:
   :undoc-members:
   :show-inheritance:

   Enum defining supported parameter styles.

Result Processing
=================

.. currentmodule:: sqlspec.core.result

.. autoclass:: SQLResult
   :members:
   :undoc-members:
   :show-inheritance:

   Result container for executed SQL queries.

   **Attributes:**

   - ``data`` - List of result rows (dicts) - prefer using helper methods instead
   - ``rows_affected`` - Number of rows affected by INSERT/UPDATE/DELETE
   - ``columns`` - Column names
   - ``metadata`` - Query metadata

   **Recommended Helper Methods:**

   .. code-block:: python

      result = await session.execute("SELECT * FROM users")

      # Get all rows (replaces result.data)
      all_rows = result.all()

      # Get exactly one row (raises if not exactly one)
      user = result.one()

      # Get one row or None (raises if multiple)
      user = result.one_or_none()

      # Get first row without validation
      first_row = result.get_first()

      # Get scalar value (first column of first row)
      count = result.scalar()

      # Map to typed models
      from pydantic import BaseModel

      class User(BaseModel):
          id: int
          name: str
          email: str

      # Get all rows as typed models
      users: list[User] = result.all(schema_type=User)

      # Get exactly one row as typed model
      user: User = result.one(schema_type=User)

      # Get one or none as typed model
      user: User | None = result.one_or_none(schema_type=User)

**Type Mapping:**

SQLResult supports mapping to various Python type systems:

- Pydantic models
- msgspec.Struct
- attrs classes
- dataclasses
- TypedDict
- Plain dicts

Type mapping is handled directly by methods on the ``SQLResult`` class such as ``as_type()``, ``as_type_one()``, and ``as_type_one_or_none()``.

SQL Compilation
===============

.. currentmodule:: sqlspec.core.compiler

.. autoclass:: SQLProcessor
   :members:
   :undoc-members:
   :show-inheritance:

   SQL processor with compilation and caching.

   Processes SQL statements by compiling them into executable format with
   parameter substitution. Includes LRU-style caching for compilation results
   to avoid re-processing identical statements.

   **Usage:**

   .. code-block:: python

      from sqlspec.core import StatementConfig
      from sqlspec.core import SQLProcessor

      config = StatementConfig(dialect="postgres")
      processor = SQLProcessor(config)

      # Compile SQL with parameters
      compiled = processor.compile(
          "SELECT * FROM users WHERE active = ?",
          parameters=[True]
      )

.. autoclass:: CompiledSQL
   :members:
   :undoc-members:
   :show-inheritance:

   Compiled SQL result containing the compiled SQL text, processed parameters,
   operation type, and execution metadata.

.. autoclass:: OperationType
   :members:
   :undoc-members:

   Type alias for SQL operation types (SELECT, INSERT, UPDATE, DELETE, etc.).

Statement Caching
=================

.. currentmodule:: sqlspec.core.cache

.. autoclass:: UnifiedCache
   :members:
   :undoc-members:
   :show-inheritance:

   Unified LRU cache for SQL statements and compilation results.

.. autoclass:: MultiLevelCache
   :members:
   :undoc-members:
   :show-inheritance:

   Multi-level cache system for different caching strategies.

.. autoclass:: CacheConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for caching behavior.

.. autoclass:: CacheStats
   :members:
   :undoc-members:
   :show-inheritance:

   Cache statistics tracking hits, misses, and evictions.

.. autoclass:: CacheKey
   :members:
   :undoc-members:
   :show-inheritance:

   Cache key for tracking cached items.

SQL Filters
===========

.. currentmodule:: sqlspec.core.filters

Filters transform SQL statements by adding or modifying clauses.

.. autoclass:: StatementFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for SQL statement filters.

   All filters implement the ``append_to_statement()`` method to modify SQL statements.

Built-in Filters
----------------

.. autoclass:: LimitOffsetFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds LIMIT and OFFSET clauses.

   .. code-block:: python

      from sqlspec.core import LimitOffsetFilter

      filter = LimitOffsetFilter(limit=10, offset=20)
      filtered_sql = filter.append_to_statement(base_sql)
      # Adds: LIMIT 10 OFFSET 20

.. autoclass:: OrderByFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds ORDER BY clause.

   .. code-block:: python

      from sqlspec.core import OrderByFilter

      filter = OrderByFilter(field_name="created_at", sort_order="desc")
      filtered_sql = filter.append_to_statement(base_sql)
      # Adds: ORDER BY created_at DESC

.. autoclass:: SearchFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds WHERE search condition.

   .. code-block:: python

      from sqlspec.core import SearchFilter

      filter = SearchFilter(field_name="name", value="John", operator="ILIKE")
      filtered_sql = filter.append_to_statement(base_sql)
      # Adds: WHERE name ILIKE '%John%'

Filter Composition
------------------

Filters can be composed and chained:

.. code-block:: python

   from sqlspec.core import (
       LimitOffsetFilter,
       OrderByFilter,
       SearchFilter
   )
   from sqlspec.core import SQL

   base_sql = SQL("SELECT * FROM users")

   # Apply multiple filters
   filtered = base_sql
   for filter_obj in [
       SearchFilter(field_name="name", value="Alice"),
       OrderByFilter(field_name="created_at", sort_order="desc"),
       LimitOffsetFilter(limit=10, offset=0)
   ]:
       filtered = filter_obj.append_to_statement(filtered)

   # Result: SELECT * FROM users
   #         WHERE name ILIKE '%Alice%'
   #         ORDER BY created_at DESC
   #         LIMIT 10 OFFSET 0

Type Conversions
================

.. currentmodule:: sqlspec.core.type_conversion

.. autoclass:: BaseTypeConverter
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for type conversion between Python types and database types.

Advanced Features
=================

Statement Analysis
------------------

.. code-block:: python

   from sqlspec.core import SQLProcessor

   compiler = SQLProcessor(dialect="postgres")

   analysis = compiler.analyze("""
       SELECT u.name, COUNT(o.id) as order_count
       FROM users u
       LEFT JOIN orders o ON u.id = o.user_id
       WHERE u.active = TRUE
       GROUP BY u.id, u.name
       HAVING COUNT(o.id) > 5
   """)

   # Analysis includes:
   # - Tables accessed
   # - Columns referenced
   # - Join types
   # - Aggregate functions
   # - Filter conditions
   # - Potential optimizations

Custom Filters
--------------

Create custom filters for specific needs:

.. code-block:: python

   from sqlspec.core import StatementFilter
   from sqlspec.core import SQL

   class TenantFilter(StatementFilter):
       def __init__(self, tenant_id: int):
           self.tenant_id = tenant_id

       def apply(self, sql: SQL) -> SQL:
           # Add tenant condition to WHERE clause
           condition = f"tenant_id = {self.tenant_id}"
           if "WHERE" in sql.sql.upper():
               new_sql = sql.sql.replace("WHERE", f"WHERE {condition} AND")
           else:
               new_sql = sql.sql + f" WHERE {condition}"

           return SQL(new_sql, sql.parameters)

   # Usage
   filter = TenantFilter(tenant_id=123)
   filtered = filter.apply(SQL("SELECT * FROM users"))
   # Result: SELECT * FROM users WHERE tenant_id = 123

Performance Tips
================

1. **Enable Caching**

   .. code-block:: python

      config = StatementConfig(cache_statements=True)
      sql = SQL("SELECT * FROM users", config=config)

2. **Reuse Compiled Statements**

   .. code-block:: python

      # Compile once
      stmt = SQL("SELECT * FROM users WHERE id = ?")

      # Execute many times
      for user_id in user_ids:
          result = await session.execute(stmt, user_id)

3. **Use Positional Parameters**

   .. code-block:: python

      # Positional parameters
      stmt = SQL("SELECT * FROM users WHERE id = ?", 123)

      # Named parameters (requires parsing)
      stmt = SQL("SELECT * FROM users WHERE id = :id", id=123)

See Also
========

- :doc:`/usage/drivers_and_querying` - Query execution
- :doc:`builder` - SQL builder integration
- :doc:`driver` - Driver implementation
- :doc:`base` - SQLSpec configuration
