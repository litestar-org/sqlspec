# SQLSpec Data Flow: From Input to Result

This document provides a detailed, in-depth analysis of how SQL statements and parameters flow through the `sqlspec` library, from user input to the final result set. Understanding this flow is crucial for debugging, extending the library, and using it effectively.

## High-Level Overview

The core of `sqlspec` is designed around a single-pass processing pipeline. A user's SQL input, whether a raw string or a `QueryBuilder` object, is converted into a `SQL` object. This object then flows through a series of transformations and validations before being executed by a database-specific driver. The result is then packaged into a standardized `SQLResult` object.

Here is a high-level Mermaid diagram illustrating the flow:

```mermaid
graph TD
    A[User Input: SQL("SELECT ..."), sql.select(), etc.] --> B{SQL Object};
    B --> C{Driver.execute()};
    C --> D[Parameter & SQL Processing Pipeline];
    D --> E{Driver-Specific Execution};
    E --> F[SQLResult];
    F --> G[User Code];
```

## Detailed Step-by-Step Flow

Let's break down each stage of the process in detail.

### 1. User Input and `SQL` Object Initialization

The journey begins when a user creates a `SQL` object. This can be done in several ways:

- **Directly:** `SQL("SELECT * FROM users WHERE id = ?", 1)`
- **Via the `sql` factory:** `sql("SELECT * FROM users WHERE id = ?", 1)`
- **Using a `QueryBuilder`:** `sql.select().from_("users").where("id = 1")`

When a `SQL` object is initialized, the following happens:

1. **Statement Normalization**: The input (string, `QueryBuilder`, or `sqlglot` expression) is converted into a `sqlglot` expression tree. This is the Abstract Syntax Tree (AST) that represents the SQL query. This is handled by the `_to_expression` method in the `SQL` class.
2. **Parameter and Filter Processing**: Any parameters and filters provided to the `SQL` object are processed and stored internally. Positional arguments become `_positional_params`, and keyword arguments become `_named_params`. `StatementFilter` objects are stored in `_filters`.
3. **Configuration**: A `StatementConfig` object is associated with the `SQL` object, which controls aspects like dialect, validation, and transformation.

### 2. The `Driver.execute()` Method

The user then calls the `execute()` method on a driver instance (e.g., `sqlite_driver.execute(sql_obj)`). This is the main entry point for the execution flow.

The `AsyncDriverAdapterBase` and `SyncDriverAdapterBase` classes define the `execute` method. This method is responsible for:

1. **Preparing the SQL**: It calls `_prepare_sql` to ensure the `SQL` object is ready for execution. This includes applying any filters to the `sqlglot` expression.
2. **Dispatching Execution**: It then calls `_dispatch_execution`, which is the central orchestrator of the execution flow.

### 3. The `_dispatch_execution` Method

This method, present in both sync and async base drivers, follows the Template Method Pattern. It manages the overall execution flow, delegating database-specific logic to abstract methods that concrete drivers must implement.

The key steps are:

1. **Context Management**: It sets the current driver in a context variable using `set_current_driver(self)`. This allows other parts of the system (like the parameter processor) to be aware of the current driver's capabilities.
2. **Cursor Management**: It acquires a database cursor using `with self.with_cursor(connection) as cursor:`. The `with_cursor` method is an abstract method that each driver implements to provide a cursor.
3. **Performing Execution**: It calls `self._perform_execute(cursor, statement)`, which is where the actual database interaction happens.
4. **Building the Result**: It calls `self._build_result(cursor, statement)` to create a `SQLResult` object from the cursor's state.
5. **Context Cleanup**: It clears the driver context using `set_current_driver(None)`.

### 4. The `_prepare_sql` and Parameter Processing

Before execution, the `SQL` object needs to be compiled into a final SQL string and a set of parameters that the database driver can understand. This is where the `ParameterProcessor` comes in.

The `SQL.compile()` method is where the magic happens:

1. **Pipeline Execution**: The `_ensure_processed` method is called, which runs the SQL processing pipeline. This pipeline is a series of functions that transform the `SQL` object's expression and parameters. The steps can include:
    - `parameterize_literals_step`: Replaces literal values in the SQL (e.g., `1`, `'abc'`) with parameter placeholders.
    - `optimize_step`: Applies `sqlglot`'s optimizer to simplify the expression.
    - `validate_step`: Performs security and safety checks.
2. **Parameter Style Conversion**: The `ParameterProcessor` converts the SQL string and parameters to the style required by the driver (e.g., `qmark` for SQLite, `numeric` for asyncpg, `pyformat` for psycopg). This is a critical step for cross-dialect compatibility.
3. **Type Coercion**: The `ParameterProcessor` also applies any driver-specific type coercions. For example, it might convert a `datetime` object to an ISO string for a driver that doesn't handle `datetime` objects natively.

### 5. The `_perform_execute` Method

This is an **abstract method** that each concrete driver adapter must implement. Its responsibility is to take the compiled SQL string and parameters and execute them against the database using the provided cursor.

For example, the `SqliteDriver`'s `_perform_execute` method looks like this:

```python
def _perform_execute(self, cursor: "sqlite3.Cursor", statement: "SQL") -> None:
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
    # ... logic to call cursor.execute or cursor.executemany ...
```

This is where the library interfaces with the underlying DB-API driver (e.g., `sqlite3`, `psycopg`, `asyncpg`).

### 6. The `_build_result` Method

After `_perform_execute` is done, `_build_result` is called. This method inspects the cursor to determine the outcome of the operation and packages it into a `SQLResult` object.

It uses two other abstract methods that drivers must implement:

- `_extract_select_data(cursor)`: For `SELECT` statements, this method should return a tuple containing the data rows, column names, and row count.
- `_extract_execute_rowcount(cursor)`: For `INSERT`, `UPDATE`, and `DELETE` statements, this should return the number of affected rows.

The `_build_result` method then uses this information to create a `SQLResult` object with the appropriate `operation_type`.

### 7. The `SQLResult` Object

The `SQLResult` object is the final product of the execution flow. It's a standardized container for the results of any SQL operation. It provides a consistent API for accessing:

- **Data**: `result.data` (a list of dictionaries)
- **Rows Affected**: `result.rows_affected`
- **Column Names**: `result.column_names`
- **Operation Type**: `result.operation_type`

It also includes convenience methods like `one()`, `one_or_none()`, and `scalar()` for easily accessing the data.

## Driver-Specific Variations

While the core flow is the same for all drivers, there are some variations based on the capabilities of the underlying database and driver.

### SQLite (`sqlspec.adapters.sqlite`)

- **Parameter Style**: `qmark` (`?`).
- **Implementation**: This is the simplest implementation, closely following the base driver protocols. It's a good reference for understanding the core architecture.

### Psycopg (`sqlspec.adapters.psycopg`)

- **Parameter Style**: `pyformat` (`%s`).
- **Key Feature**: It uses the `psycopg` library, which is the standard for PostgreSQL in Python. The driver adapter ensures that `sqlspec`'s internal parameter handling is correctly translated to `pyformat`.

### ADBC (`sqlspec.adapters.adbc`)

- **Parameter Style**: Varies by the underlying ADBC driver (e.g., `numeric` for PostgreSQL, `qmark` for SQLite).
- **Key Feature**: The ADBC driver is designed to work with the Arrow Database Connectivity (ADBC) standard. This allows it to return data in the form of Apache Arrow tables, which is highly efficient for data analysis and is handled by the `ArrowResult` class. The `AdbcDriver` has more complex parameter preparation to handle Arrow's type system.

### Async Drivers (`aiosqlite`, `asyncpg`, `asyncmy`)

- **Core Difference**: The `execute`, `_dispatch_execution`, `_perform_execute`, and `_build_result` methods are all `async` methods.
- **Flow**: The overall flow is identical to the sync drivers, but it uses `async/await` at each step. The `AsyncDriverAdapterBase` provides the async version of the template methods.

## Extending with a New Driver

To add support for a new database, you would need to:

1. Create a new driver adapter class that inherits from `SyncDriverAdapterBase` or `AsyncDriverAdapterBase`.
2. Implement the abstract methods:
    - `with_cursor`
    - `begin`, `commit`, `rollback`
    - `_perform_execute`
    - `_extract_select_data`
    - `_extract_execute_rowcount`
3. Define the `dialect` and `parameter_config` for the driver.
4. Create a `DatabaseConfig` class for your new driver.

By following this pattern, you can leverage the entire `sqlspec` processing pipeline and get features like automatic parameter conversion, validation, and standardized results for free.
