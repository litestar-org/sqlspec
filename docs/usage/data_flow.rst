==========================
The SQLSpec Execution Flow
==========================

Understanding how SQLSpec processes a query from input to result is essential for using the library effectively and debugging issues. SQLSpec employs a sophisticated "parse once, transform once, validate once" pipeline that ensures both performance and security.

This guide provides a comprehensive overview of the execution flow, from the moment you create a SQL statement to receiving typed Python objects as results.

Execution Pipeline Overview
----------------------------

SQLSpec's execution flow can be visualized as a series of well-defined stages that transform user input into database results. The architecture is designed around **single-pass processing** with **multi-tier caching** for optimal performance.

High-Level Flow Diagram
^^^^^^^^^^^^^^^^^^^^^^^^

.. mermaid::

   graph TD
      subgraph "1. User Input"
         A[SQL String or QueryBuilder] --> B[SQL Object Creation];
      end

      subgraph "2. SQLSpec Core Pipeline"
         B --> C[Parameter Extraction];
         C --> D[AST Generation via SQLGlot];
         D --> E{Validation};
         E --> F{Transformation};
         F --> G[SQL Compilation];
      end

      subgraph "3. Driver & Database"
         G --> H[Driver Execution];
         H --> I[DBAPI Connection];
         I --> J[(Database)];
         J --> K[Raw Results];
      end

      subgraph "4. Result Handling"
         K --> L[SQLResult Object];
         L --> M{Schema Mapping};
         M --> N[Typed Python Objects];
      end

      style E fill:#f9f,stroke:#333,stroke-width:2px
      style F fill:#9f9,stroke:#333,stroke-width:2px
      style M fill:#9ff,stroke:#333,stroke-width:2px

Detailed Execution Stages
--------------------------

Stage 1: SQL Object Creation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The execution flow begins when you create a SQL object. SQLSpec accepts multiple input formats:

**Direct SQL Creation**

.. code-block:: python

   from sqlspec.core.statement import SQL

   # Raw SQL string with positional parameters
   sql = SQL("SELECT * FROM users WHERE id = ?", 1)

   # Named parameters
   sql = SQL("SELECT * FROM users WHERE email = :email", email="user@example.com")

**Using the Query Builder**

.. code-block:: python

   from sqlspec.builder import Select

   # Build SQL programmatically
   query = Select("id", "name", "email").from_("users").where("status = ?")
   sql = SQL(query, "active")

**From SQL Files**

.. code-block:: python

   from sqlspec.loader import SQLFileLoader

   loader = SQLFileLoader()
   loader.load_sql("queries/users.sql")
   sql = loader.get_sql("get_user_by_id", user_id=123)

During initialization, the SQL object:

1. Stores the statement (string, QueryBuilder, or sqlglot expression)
2. Captures positional and named parameters with type information
3. Initializes lazy processing flags for deferred compilation
4. Prepares for pipeline execution

Stage 2: The Core Processing Pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the SQL object is compiled for execution, it passes through a sophisticated processing pipeline. This is where SQLSpec's "parse once, transform once, validate once" philosophy is implemented.

**Step 1: Parameter Extraction**

The first step extracts and preserves parameter information before any SQL modifications:

.. code-block:: python

   # SQLSpec identifies parameter placeholders
   # Input:  "SELECT * FROM users WHERE id = ? AND status = ?"
   # Params: [1, 'active']
   #
   # Result: Positional parameter mapping created
   #         Position 0 ' value: 1
   #         Position 1 ' value: 'active'

This step uses ``ParameterValidator`` to ensure parameters are properly formatted and positions are tracked.

**Step 2: AST Generation with SQLGlot**

The SQL string is parsed into an Abstract Syntax Tree (AST) using SQLGlot:

.. code-block:: python

   import sqlglot

   # Parse SQL into structured AST
   expression = sqlglot.parse_one(
       "SELECT * FROM users WHERE id = ?",
       dialect="sqlite"
   )

The AST represents your query as a tree structure that can be analyzed and modified programmatically. This is crucial for the validation and transformation steps.

**Why AST Processing?**

Instead of treating SQL as plain text, SQLSpec uses the AST to:

- Understand the query structure (SELECT, WHERE, JOIN clauses)
- Identify security risks (SQL injection patterns)
- Detect performance issues (missing JOINs, unbounded queries)
- Transform queries safely (add filters, parameterize literals)

**Step 3: Validation**

The AST is passed through multiple validators to check for potential issues:

.. code-block:: python

   from sqlspec.core.validation import (
       SecurityValidator,
       PerformanceValidator,
       DMLSafetyValidator
   )

**SecurityValidator**

Detects SQL injection patterns and dangerous constructs:

- Tautologies (``OR 1=1``, ``OR 'x'='x'``)
- Comment-based injection attempts
- Dangerous keywords (``EXEC``, ``xp_cmdshell``)
- Union-based injection patterns

.. code-block:: python

   # BLOCKED: Suspicious tautology pattern
   SQL("SELECT * FROM users WHERE id = 1 OR 1=1")

   # BLOCKED: Comment injection attempt
   SQL("SELECT * FROM users WHERE name = 'admin' --'")

**PerformanceValidator**

Identifies potential performance bottlenecks:

- Cartesian products (missing JOIN conditions)
- Queries without WHERE clauses on large tables
- SELECT * on tables with many columns
- Missing LIMIT on potentially large result sets

.. code-block:: python

   # WARNING: Query without WHERE clause
   SQL("SELECT * FROM large_table")

   # WARNING: Cartesian product detected
   SQL("SELECT * FROM users, orders")  # Missing JOIN condition

**DMLSafetyValidator**

Enforces safe data modification practices:

- UPDATE without WHERE clause
- DELETE without WHERE clause
- Truncation safety checks

.. code-block:: python

   # BLOCKED: DELETE without WHERE
   SQL("DELETE FROM users")  # Would delete all users!

   # ALLOWED: DELETE with WHERE
   SQL("DELETE FROM users WHERE inactive = true")

**Step 4: Transformation**

After validation, the AST can be programmatically transformed. SQLSpec includes several built-in transformers:

**Literal Parameterization**

The ``ParameterizeLiterals`` transformer converts hardcoded values into bound parameters:

.. code-block:: python

   # Before transformation:
   SQL("SELECT * FROM users WHERE status = 'active'")

   # After transformation:
   # SQL: "SELECT * FROM users WHERE status = ?"
   # Parameters: ['active']

This improves security and enables database query plan caching.

**Custom Transformations**

Drivers can inject their own transformers. For example, you could:

- Auto-append soft-delete filters (``WHERE deleted_at IS NULL``)
- Add row-level security filters based on user context
- Transform DELETE into UPDATE for audit trails
- Inject tenant isolation filters for multi-tenant applications

.. code-block:: python

   # Example custom transformer
   class SoftDeleteTransformer:
       def transform(self, ast):
           # Add "WHERE deleted_at IS NULL" to all SELECT queries
           if ast.find(exp.Select):
               # Inject soft delete filter
               pass

**Step 5: SQL Compilation**

The final, validated, and transformed AST is compiled back to a SQL string in the target database dialect:

.. code-block:: python

   # Compile for PostgreSQL
   sql_string = expression.sql(dialect="postgres")
   # Result: "SELECT * FROM users WHERE id = $1"

   # Compile for SQLite
   sql_string = expression.sql(dialect="sqlite")
   # Result: "SELECT * FROM users WHERE id = ?"

Parameters are prepared in the appropriate style for the database driver (``?``, ``$1``, ``:name``, ``%s``, etc.).

Stage 3: Driver Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the SQL is compiled, it's sent to the database-specific driver for execution:

.. code-block:: python

   # Driver receives compiled SQL and parameters
   with spec.provide_session(config) as session:
       result = session.execute(compiled_sql, prepared_params)

**Template Method Pattern**

SQLSpec drivers use the Template Method pattern for consistent execution:

1. **Special Handling Check**: Try database-specific optimizations (PostgreSQL COPY, bulk operations)
2. **Execution Routing**: Route to appropriate method based on query type:

   - ``_execute_statement``: Single statement execution
   - ``_execute_many``: Batch execution (executemany)
   - ``_execute_script``: Multi-statement scripts

3. **Database Interaction**: Execute via DBAPI connection
4. **Result Building**: Package raw results into SQLResult

**Example: SQLite Driver Execution**

.. code-block:: python

   class SqliteDriver(SyncDriverAdapterBase):
       def _execute_statement(self, cursor, statement):
           sql, params = self._get_compiled_sql(statement)
           cursor.execute(sql, params or ())
           return self.create_execution_result(cursor)

       def _execute_many(self, cursor, statement):
           sql, params = self._get_compiled_sql(statement)
           cursor.executemany(sql, params)
           return self.create_execution_result(cursor)

Stage 4: Result Handling
^^^^^^^^^^^^^^^^^^^^^^^^^

After database execution, raw results are transformed into typed Python objects.

**SQLResult Object**

All query results are wrapped in a ``SQLResult`` object:

.. code-block:: python

   result = session.execute("SELECT * FROM users")

   # Access result data
   result.data              # List of dictionaries
   result.rows_affected     # Number of rows modified (INSERT/UPDATE/DELETE)
   result.column_names      # Column names for SELECT
   result.operation_type    # "SELECT", "INSERT", "UPDATE", "DELETE", "SCRIPT"

**Convenience Methods**

.. code-block:: python

   # Get exactly one row (raises if not exactly one)
   user = result.one()

   # Get one or None
   user = result.one_or_none()

   # Get scalar value (first column of first row)
   count = result.scalar()

**Schema Mapping**

SQLSpec can automatically map results to typed objects:

.. code-block:: python

   from pydantic import BaseModel
   from typing import Optional

   class User(BaseModel):
       id: int
       name: str
       email: str
       is_active: Optional[bool] = True

   # Execute with schema type
   result = session.execute(
       "SELECT id, name, email, is_active FROM users",
       schema_type=User
   )

   # Results are typed User instances
   users: list[User] = result.to_schema()
   user: User = result.one()  # Type-safe!

**Supported Schema Types**

- Pydantic models (v1 and v2)
- msgspec Structs
- attrs classes
- dataclasses
- TypedDict

Performance Optimizations
--------------------------

SQLSpec's pipeline includes several performance optimizations:

Multi-Tier Caching
^^^^^^^^^^^^^^^^^^

SQLSpec implements caching at multiple levels:

.. code-block:: python

   # Cache types and their benefits:
   sql_cache: dict[str, str]              # Compiled SQL strings (12x+ speedup)
   optimized_cache: dict[str, Expression] # Post-optimization AST
   builder_cache: dict[str, bytes]        # QueryBuilder serialization
   file_cache: dict[str, CachedSQLFile]   # File loading with checksums
   analysis_cache: dict[str, Any]         # Pipeline step results

**Cache Benefits**

- File operations: 12x+ performance improvement
- Repeated queries: Near-instant compilation
- AST processing: Cached validation and transformation results
- Parameter conversion: Reuse for identical patterns

Single-Pass Processing
^^^^^^^^^^^^^^^^^^^^^^

Each SQL statement is processed exactly once through the pipeline:

1. Parse once ' AST generation happens once
2. Transform once ' Modifications applied once to AST
3. Validate once ' Security and performance checks run once
4. Compile once ' SQL generation happens once per dialect

This eliminates redundant work and ensures consistent results.

Configuration-Driven Processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``StatementConfig`` controls pipeline behavior:

.. code-block:: python

   from sqlspec.core.statement import StatementConfig
   from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig

   config = StatementConfig(
       dialect="postgres",
       enable_parsing=True,      # AST generation
       enable_validation=True,   # Security/performance checks
       enable_transformations=True,  # AST transformations
       enable_caching=True,      # Multi-tier caching
       parameter_config=ParameterStyleConfig(
           default_parameter_style=ParameterStyle.NUMERIC,
           has_native_list_expansion=False,
       )
   )

Disable features you don't need for maximum performance.

Understanding the Flow Benefits
--------------------------------

By understanding this execution flow, you can:

**Debug Issues Effectively**

- Know where to look when queries fail
- Understand validation errors
- Trace parameter binding issues

**Optimize Performance**

- Leverage caching appropriately
- Understand when AST processing occurs
- Choose the right statement configuration

**Extend SQLSpec**

- Write custom transformers
- Create new validators
- Implement custom drivers

**Write Better Queries**

- Understand how parameterization works
- Know what triggers validation errors
- Use the right query patterns for your database

Next Steps
----------

Now that you understand the execution flow, learn how to:

- :doc:`configuration` - Configure database connections and statement processing
- :doc:`drivers_and_querying` - Execute queries with different database drivers
- :doc:`query_builder` - Build queries programmatically with the fluent API

See Also
--------

- :doc:`../reference/core` - Core module API reference
- :doc:`../reference/driver` - Driver implementation details
- :doc:`../contributing/creating_adapters` - Creating custom database adapters
