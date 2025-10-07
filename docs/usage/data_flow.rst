==============
Execution Flow
==============

Understanding how SQLSpec processes a query from input to result is essential for using the library effectively and debugging issues. SQLSpec employs a sophisticated "parse once, transform once, validate once" pipeline that ensures both performance and security.

This guide provides a comprehensive overview of the execution flow, from the moment you create a SQL statement to receiving typed Python objects as results.

Execution Pipeline Overview
----------------------------

SQLSpec's execution flow can be visualized as a series of well-defined stages that transform user input into database results. The architecture is designed around **single-pass processing** with **multi-tier caching** for optimal performance.

High-Level Flow Diagram
^^^^^^^^^^^^^^^^^^^^^^^^

.. mermaid::

   sequenceDiagram
       autonumber
       actor User
       participant SQL as SQL Object
       participant Core as SQLSpec Core
       participant Driver as Database Driver
       participant DB as Database
       participant Result as SQLResult

       Note over User,SQL: Stage 1: SQL Creation
       User->>SQL: Create SQL statement<br/>(string/builder/file)
       SQL->>SQL: Store parameters<br/>Initialize lazy flags

       Note over SQL,Core: Stage 2: Core Processing Pipeline
       SQL->>Core: Trigger compilation
       Core->>Core: Extract parameters
       Core->>Core: Parse to AST (SQLGlot)
       Core->>Core: Validate SQL
       Core->>Core: Transform AST
       Core->>Core: Compile to dialect

       Note over Core,DB: Stage 3: Database Execution
       Core->>Driver: Pass compiled SQL + params
       Driver->>Driver: Convert parameter style
       Driver->>DB: Execute query
       DB-->>Driver: Return raw results

       Note over Driver,Result: Stage 4: Result Processing
       Driver->>Result: Create SQLResult object
       Result->>Result: Map to schema types
       Result-->>User: Return typed Python objects

       Note right of Result: Supports: Pydantic,<br/>msgspec, attrs,<br/>dataclasses, TypedDict

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

   from sqlspec import sql

   # Build SQL programmatically
   query = sql.select("id", "name", "email").from_("users").where("status = ?", "active")


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
   #         Position 0 → value: 1
   #         Position 1 → value: 'active'

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

**Step 3: Compilation**

The AST is compiled into the target SQL dialect:

.. code-block:: python

   import sqlglot

   # Compile AST to target dialect
   compiled_sql = expression.sql(dialect="postgres")
   # Result: "SELECT * FROM users WHERE id = $1"




**Step 4: Parameter Processing**

Parameters are converted to the appropriate style for the target database:

.. code-block:: python

   # Input parameters: [1, 'active']
   # Target style: PostgreSQL numeric ($1, $2)
   # Result: Parameters ready for execution

This ensures compatibility across different database drivers.

**Step 5: Statement Execution**

The compiled SQL and processed parameters are sent to the database:

.. code-block:: python

   # Driver executes compiled SQL with parameters
   cursor.execute(compiled_sql, parameters)
   results = cursor.fetchall()

The driver handles database-specific execution patterns and result retrieval.

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

   # Cache types and their purposes:
   sql_cache: dict[str, str]              # Compiled SQL strings
   optimized_cache: dict[str, Expression] # Post-optimization AST
   builder_cache: dict[str, bytes]        # QueryBuilder serialization
   file_cache: dict[str, CachedSQLFile]   # File loading with checksums
   analysis_cache: dict[str, Any]         # Pipeline step results

**Cache Benefits**

- Avoids recompiling identical SQL statements
- Skips redundant AST processing for repeated queries
- Caches validation and transformation results
- Reuses parameter conversion for identical patterns

Single-Pass Processing
^^^^^^^^^^^^^^^^^^^^^^

Each SQL statement is processed exactly once through the pipeline:

1. Parse once → AST generation happens once
2. Transform once → Modifications applied once to AST
3. Validate once → Security and performance checks run once
4. Compile once → SQL generation happens once per dialect

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
