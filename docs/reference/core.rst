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

      from sqlspec.core.statement import SQL

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

.. autoclass:: ParameterBinder
   :members:
   :undoc-members:
   :show-inheritance:

   Handles parameter binding and conversion between parameter styles.

   **Supported parameter styles:**

   - ``?`` - SQLite/DuckDB positional
   - ``$1, $2`` - PostgreSQL positional (asyncpg, psqlpy)
   - ``%s`` - PostgreSQL/MySQL format style (psycopg, asyncmy)
   - ``:name`` - Named parameters (Oracle, SQLite)
   - ``@name`` - BigQuery named parameters

   **Conversion:**

   .. code-block:: python

      # Input with named parameters
      sql = "SELECT * FROM users WHERE name = :name AND age > :age"
      params = {"name": "Alice", "age": 25}

      # Convert to PostgreSQL style ($1, $2)
      binder = ParameterBinder(sql, params, target_style="$")
      converted_sql = binder.bind()
      # Result: "SELECT * FROM users WHERE name = $1 AND age > $2"
      # Parameters: ["Alice", 25]

.. autofunction:: convert_parameters
   :noindex:

.. autofunction:: bind_parameters
   :noindex:

Result Processing
=================

.. currentmodule:: sqlspec.core.result

.. autoclass:: SQLResult
   :members:
   :undoc-members:
   :show-inheritance:

   Result container for executed SQL queries.

   **Attributes:**

   - ``data`` - List of result rows (dicts)
   - ``rows_affected`` - Number of rows affected by INSERT/UPDATE/DELETE
   - ``columns`` - Column names
   - ``metadata`` - Query metadata

   **Methods:**

   .. code-block:: python

      result = await session.execute("SELECT * FROM users")

      # Access data
      all_rows = result.data
      first_row = result.get_first()
      first_row_or_none = result.get_first_or_none()

      # Map to types
      from pydantic import BaseModel

      class User(BaseModel):
          id: int
          name: str
          email: str

      users = result.as_type(User)  # List[User]
      user = result.as_type_one(User)  # User
      user_or_none = result.as_type_one_or_none(User)  # User | None

      # Get scalar value
      count = result.scalar()  # First column of first row

.. autoclass:: ResultMapper
   :members:
   :undoc-members:
   :show-inheritance:

   Maps raw database results to Python types.

   **Supported type systems:**

   - Pydantic models
   - msgspec.Struct
   - attrs classes
   - dataclasses
   - TypedDict
   - Plain dicts

   **Example:**

   .. code-block:: python

      from msgspec import Struct

      class User(Struct):
          id: int
          name: str
          email: str

      mapper = ResultMapper(User)
      users = mapper.map_all(result.data)

.. autoclass:: ResultColumn
   :members:
   :undoc-members:
   :show-inheritance:

   Metadata about a result column.

   **Attributes:**

   - ``name`` - Column name
   - ``type`` - Database type
   - ``nullable`` - Whether NULL is allowed
   - ``table`` - Source table name
   - ``schema`` - Source schema name

SQL Compilation
===============

.. currentmodule:: sqlspec.core.compiler

.. autoclass:: SQLCompiler
   :members:
   :undoc-members:
   :show-inheritance:

   Compiles and validates SQL using sqlglot.

   **Features:**

   - Syntax validation
   - Dialect-specific compilation
   - Query analysis
   - Optimization hints
   - Error reporting

   **Usage:**

   .. code-block:: python

      compiler = SQLCompiler(dialect="postgres")

      # Compile SQL
      compiled = compiler.compile("SELECT * FROM users WHERE active = TRUE")

      # Validate SQL
      is_valid = compiler.validate("SELECT * FROM users")

      # Analyze SQL
      analysis = compiler.analyze("""
          SELECT u.name, COUNT(o.id)
          FROM users u
          LEFT JOIN orders o ON u.id = o.user_id
          GROUP BY u.id
      """)

.. autoclass:: CompilerConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for SQL compilation.

   **Options:**

   - ``dialect`` - SQL dialect (postgres, mysql, sqlite, etc.)
   - ``validate`` - Enable validation
   - ``pretty`` - Pretty-print compiled SQL
   - ``optimize`` - Apply optimizations

.. autofunction:: compile_sql
   :noindex:

.. autofunction:: validate_sql
   :noindex:

Statement Caching
=================

.. currentmodule:: sqlspec.core.cache

.. autoclass:: StatementCache
   :members:
   :undoc-members:
   :show-inheritance:

   LRU cache for compiled SQL statements.

   **Features:**

   - Configurable size
   - Thread-safe
   - Automatic eviction
   - Cache statistics

   **Usage:**

   .. code-block:: python

      cache = StatementCache(max_size=1000)

      # Store statement
      cache.set("SELECT * FROM users", compiled_stmt)

      # Retrieve statement
      stmt = cache.get("SELECT * FROM users")

      # Clear cache
      cache.clear()

      # Get statistics
      stats = cache.stats()
      print(f"Hits: {stats.hits}, Misses: {stats.misses}")

.. autoclass:: CacheConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for statement caching.

   **Options:**

   - ``max_size`` - Maximum number of cached statements
   - ``ttl`` - Time to live in seconds
   - ``enabled`` - Enable/disable caching

SQL Filters
===========

.. currentmodule:: sqlspec.core.filters

Filters transform SQL statements by adding or modifying clauses.

.. autoclass:: SQLFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for SQL filters.

   **Protocol:**

   .. code-block:: python

      from sqlspec.protocols import SQLFilterProtocol

      class CustomFilter(SQLFilterProtocol):
          def apply(self, sql: SQL) -> SQL:
              # Transform SQL
              return modified_sql

Built-in Filters
----------------

.. autoclass:: LimitOffsetFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds LIMIT and OFFSET clauses.

   .. code-block:: python

      from sqlspec.core.filters import LimitOffsetFilter

      filter = LimitOffsetFilter(limit=10, offset=20)
      filtered_sql = filter.apply(base_sql)
      # Adds: LIMIT 10 OFFSET 20

.. autoclass:: OrderByFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds ORDER BY clause.

   .. code-block:: python

      from sqlspec.core.filters import OrderByFilter

      filter = OrderByFilter("created_at", "desc")
      filtered_sql = filter.apply(base_sql)
      # Adds: ORDER BY created_at DESC

.. autoclass:: SearchFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds WHERE search condition.

   .. code-block:: python

      from sqlspec.core.filters import SearchFilter

      filter = SearchFilter("name", "John", operator="ILIKE")
      filtered_sql = filter.apply(base_sql)
      # Adds: WHERE name ILIKE '%John%'

.. autoclass:: WhereFilter
   :members:
   :undoc-members:
   :show-inheritance:

   Adds WHERE condition.

   .. code-block:: python

      from sqlspec.core.filters import WhereFilter

      filter = WhereFilter("active = TRUE")
      filtered_sql = filter.apply(base_sql)
      # Adds: WHERE active = TRUE

Filter Composition
------------------

Filters can be composed and chained:

.. code-block:: python

   from sqlspec.core.filters import (
       LimitOffsetFilter,
       OrderByFilter,
       SearchFilter
   )
   from sqlspec.core.statement import SQL

   base_sql = SQL("SELECT * FROM users")

   # Apply multiple filters
   filtered = base_sql.copy()
   for filter in [
       SearchFilter("name", "Alice"),
       OrderByFilter("created_at", "desc"),
       LimitOffsetFilter(10, 0)
   ]:
       filtered = filter.apply(filtered)

   # Result: SELECT * FROM users
   #         WHERE name ILIKE '%Alice%'
   #         ORDER BY created_at DESC
   #         LIMIT 10 OFFSET 0

Type Conversions
================

.. currentmodule:: sqlspec.core.converters

.. autoclass:: TypeConverter
   :members:
   :undoc-members:
   :show-inheritance:

   Converts between Python types and database types.

   **Supported conversions:**

   - Python datetime <-> Database timestamp
   - Python Decimal <-> Database numeric
   - Python UUID <-> Database UUID/text
   - Python Enum <-> Database text/int
   - Python bool <-> Database boolean/int

.. autofunction:: python_to_db
   :noindex:

.. autofunction:: db_to_python
   :noindex:

Advanced Features
=================

Statement Analysis
------------------

.. code-block:: python

   from sqlspec.core.compiler import SQLCompiler

   compiler = SQLCompiler(dialect="postgres")

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

   from sqlspec.core.filters import SQLFilter
   from sqlspec.core.statement import SQL

   class TenantFilter(SQLFilter):
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

      # Faster
      stmt = SQL("SELECT * FROM users WHERE id = ?", 123)

      # Slower (requires parameter parsing)
      stmt = SQL("SELECT * FROM users WHERE id = :id", id=123)

See Also
========

- :doc:`/usage/drivers_and_querying` - Query execution
- :doc:`builder` - SQL builder integration
- :doc:`driver` - Driver implementation
- :doc:`base` - SQLSpec configuration
