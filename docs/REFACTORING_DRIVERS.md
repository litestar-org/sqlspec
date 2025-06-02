# Refactoring Guide: SQLSpec Driver Adapters (`_execute_impl`)

This guide outlines the steps to refactor existing SQLSpec driver adapters to align with the changes to the `_execute_impl` method signature and the handling of batch (`is_many`) and script (`is_script`) execution modes.

**Core Principle**: The `_execute_impl` method in driver adapters now has a simplified signature. Information about whether an operation is a batch execution (`is_many`) or a script execution (`is_script`) is now part of the `SQL` object itself (accessible via `statement.is_many` and `statement.is_script`). The `parameters` and `config` are also primarily sourced from the `SQL` object.

## General Steps for Refactoring `_execute_impl`

1. **Update `_execute_impl` Signature**:
    * The new signature for both synchronous and asynchronous drivers is:

        ```python
        # For async drivers
        async def _execute_impl(
            self,
            statement: SQL, # The fully prepared SQL object
            connection: Optional[YourDriverConnectionType] = None,
            **kwargs: Any, # For any remaining driver-specific execution options
        ) -> Any: # Raw result from the database driver
        ```

        ```python
        # For sync drivers
        def _execute_impl(
            self,
            statement: SQL, # The fully prepared SQL object
            connection: Optional[YourDriverConnectionType] = None,
            **kwargs: Any, # For any remaining driver-specific execution options
        ) -> Any: # Raw result from the database driver
        ```

    * Remove `parameters`, `config`, `is_many`, and `is_script` from the method parameters.

2. **Access Execution Mode from `SQL` Object**:
    * Inside `_execute_impl`, determine the execution mode using the `SQL` object's properties:
        * `if statement.is_script:`
        * `if statement.is_many:`

3. **Access SQL String and Parameters from `SQL` Object**:
    * **SQL String**: Get the final SQL string to execute using `statement.to_sql(placeholder_style=self._get_placeholder_style())`.
        * For scripts (`statement.is_script`), use `statement.to_sql(placeholder_style=ParameterStyle.STATIC)` to get the raw SQL as scripts usually don't use dynamic placeholders in the same way.
    * **Parameters**: Access the processed and merged parameters directly from `statement.parameters` or `statement._merged_parameters` (the latter is often more reliable for the final list/dict after processing).
        * For `is_many=True`, `statement.parameters` (or `statement._merged_parameters`) will typically be a sequence of parameter sets (e.g., a list of tuples or list of dicts).
        * For single execution, it will be a single parameter set (e.g., a tuple or dict) or `None`.

4. **Handle `is_script` Logic**:
    * If `statement.is_script` is true:
        * Generate SQL using `ParameterStyle.STATIC`.
        * Use the database driver's method for executing scripts (e.g., `cursor.executescript()` for SQLite, `connection.execute()` for asyncpg for simple scripts).
        * Parameters are usually not passed separately for scripts; they should be embedded if the script syntax supports it, or `ParameterStyle.STATIC` should have rendered them in.

5. **Handle `is_many` Logic (Batch Execution)**:
    * If `statement.is_many` is true:
        * Ensure `statement.parameters` is treated as a sequence of parameter sets.
        * Use the database driver's batch execution method (e.g., `cursor.executemany()`).

6. **Handle Single Execution Logic**:
    * If not `is_script` and not `is_many`:
        * Use the database driver's standard single statement execution method (e.g., `cursor.execute()`, `connection.execute()`, `connection.fetch()`).
        * Pass the `statement.parameters` (appropriately formatted as a tuple or dict if needed by the driver API) along with the SQL string.

7. **Configuration**: The `SQL` object's `statement.config` is available if any specific configuration is needed at this level, but generally, the SQL string and parameters from the `statement` object should be pre-configured.

8. **Remove `config` Parameter Usage**: If `_execute_impl` was previously using the `config` parameter to re-copy or re-configure the statement, this logic should now be unnecessary as the input `statement: SQL` object is assumed to be fully prepared by the base driver protocol before `_execute_impl` is called.

9. **Update Calls in `select_to_arrow` (if applicable)**:
    * If your driver implements an Arrow-specific method like `select_to_arrow` that internally calls helper methods which construct SQL or use parameters, ensure those helpers also now rely on the `SQL` object for parameters rather than taking them as separate arguments if they were refactored similarly.
    * Typically, `select_to_arrow` would construct its own `SQL` object or receive one, and then pass the necessary SQL string and parameters to the underlying DB-API call. Ensure it uses `stmt_obj.to_sql(...)` and `stmt_obj.parameters` correctly.

## Example: Refactoring `_execute_impl` for an Async Driver

**Old Signature Example**:

```python
# async def _execute_impl(
#     self,
#     statement: SQL,
#     parameters: Optional[SQLParameterType] = None,
#     connection: Optional[YourConnectionType] = None,
#     config: Optional[SQLConfig] = None, # Old
#     is_many: bool = False, # Old
#     is_script: bool = False, # Old
#     **kwargs: Any,
# ) -> Any:
#     conn = self._connection(connection)
#     final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
#     if is_script:
#         # ... script logic with final_sql ...
#     elif is_many:
#         # ... executemany logic with final_sql and parameters ...
#     else:
#         # ... execute logic with final_sql and parameters ...
```

**New Signature and Logic Example**:

```python
from sqlspec.statement.sql import SQL
from sqlspec.statement.parameters import ParameterStyle # For ParameterStyle.STATIC
from typing import Optional, Any

# Assuming YourConnectionType is defined

async def _execute_impl(
    self,
    statement: SQL, # SQL object now carries all necessary info
    connection: Optional[YourConnectionType] = None,
    **kwargs: Any,
) -> Any:
    conn = self._connection(connection)

    if statement.is_script:
        final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
        # Example: return await conn.execute_script_raw(final_sql) # Hypothetical driver method
        # For asyncpg, it might just be: return await conn.execute(final_sql)
        # Ensure no separate parameters are passed if the script is self-contained.
        return await conn.execute(final_sql) # Example for asyncpg

    final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
    params_to_execute = statement.parameters # This should be the final list/dict

    if statement.is_many:
        # Ensure params_to_execute is a sequence of sequences/dicts
        # Example: return await conn.executemany(final_sql, params_to_execute)
        pass # Replace with actual driver call
    else:
        # Ensure params_to_execute is a single sequence/dict or None
        # Example: return await conn.execute(final_sql, *params_to_execute) # If driver expects *args
        # Example: return await conn.execute(final_sql, params_to_execute) # If driver expects a list/tuple or dict
        pass # Replace with actual driver call

    # Remember to handle return values appropriate for your driver (cursors, status strings, etc.)
```

**Key Points**:

* The `SQL` object passed to `_execute_impl` is the single source of truth for the SQL string, its parameters, and execution mode (script/many).
* The base driver adapter (`SyncDriverAdapterProtocol` / `AsyncDriverAdapterProtocol`) methods (`execute`, `execute_many`, `execute_script`) are responsible for preparing the `SQL` object correctly (e.g., calling `sql_obj.as_many()` or `sql_obj.as_script()`) before calling `_execute_impl`.
* Individual drivers no longer need to interpret `is_many` or `is_script` bools passed as parameters.

This refactor simplifies the `_execute_impl` interface and centralizes query information within the `SQL` object, leading to cleaner and more maintainable driver adapters.
