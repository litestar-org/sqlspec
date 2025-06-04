## Plan: Refactor Arrow and Parquet Mixins for Consistency with Driver Execute Flow (Revised)

**Date:** 2024-07-27

**Objective:**
To refactor the `ArrowMixin` (`select_to_arrow`) and `ParquetMixin` (`to_parquet`) methods to align them closely with the redesigned driver execution flow. This involves leveraging instrumentation wrappers, providing default `list[RowT]` to Arrow/Parquet conversion logic within the mixins, and allowing drivers to override these for native performance.

**Guiding Principles:**

1. **Maintain Mixin Structure:** Functionalities remain as mixins (`SyncArrowMixin`, `AsyncArrowMixin`, `SyncParquetMixin`, `AsyncParquetMixin`).
2. **Consistent Public API:** Public methods in mixins (`select_to_arrow`, `to_parquet`) provide a consistent user interface.
3. **Instrumentation via Wrappers:** Public methods delegate to instrumentation wrappers (`instrument_operation` / `instrument_operation_async`).
4. **Standard Data Fetch First:** The instrumentation wrappers will call an intermediate private method (e.g., `_execute_and_fetch_for_arrow`). This method first fetches data using the driver's standard execution path (`_execute` + `_wrap_select_result`) to obtain `SQLResult[RowT]`.
5. **Overridable Conversion Methods:** The intermediate method then calls a new set of protected, overridable conversion methods, passing the fetched `list[RowT]` and the original `SQL` object:
    * `_convert_rows_to_arrow(self, rows: list[RowT], statement: SQL, **kwargs: Any) -> ArrowTable`
    * `_write_rows_to_parquet(self, rows: list[RowT], statement: SQL, path: Path, **kwargs: Any) -> None`
6. **Default Mixin Implementations:**
    * The mixins will provide default implementations for `_convert_rows_to_arrow` and `_write_rows_to_parquet`.
    * These defaults will convert `list[RowT]` to Arrow/Parquet, using `PYARROW_INSTALLED` from `sqlspec.typing` to check for `pyarrow` availability and raising `MissingDependencyError` if not found. Type stubs like `ArrowTable` from `sqlspec.typing` will be used.
    * Default `_write_rows_to_parquet` will use `_convert_rows_to_arrow`.
7. **Driver Specialization:** Drivers with native Arrow/Parquet capabilities will override these conversion methods. Their overrides will typically ignore the `rows` argument and use the `statement` argument for direct, optimized fetching.
8. **Robust Kwargs Handling:** Ensure `**kwargs` are correctly propagated through the call chain.

---

### Phase 1: Define New Protected Conversion Methods and Default Mixin Implementations

**1.1. Update `sqlspec.typing` (if necessary)**

* Ensure `ArrowTable` and any other `pyarrow` related types are correctly defined using `PYARROW_INSTALLED` for conditional import/stubbing.
    * (Already done in `sqlspec.typing` via `sqlspec._typing`)

**1.2. Implement Default `_convert_rows_to_arrow` in `SyncArrowMixin` and `AsyncArrowMixin`**

* **`SyncArrowMixin._convert_rows_to_arrow`:**

    ```python
    # In sqlspec.statement.mixins (SyncArrowMixin)
    from sqlspec.typing import PYARROW_INSTALLED, ArrowTable, RowT # Assuming RowT is typically dict[str, Any]
    from sqlspec.exceptions import MissingDependencyError
    # from sqlspec.statement.sql import SQL # Already in scope likely

    def _convert_rows_to_arrow(self: 'ExporterMixinProtocol', rows: list[RowT], statement: 'SQL', **kwargs: Any) -> 'ArrowTable':
        """Default implementation to convert a list of row data to an Arrow Table."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError("pyarrow", "pyarrow")
        import pyarrow as pa

        if not rows:
            # Attempt to get column names from statement.analysis_result if available and useful
            # For simplicity here, if no rows, create an empty table with no schema or try to infer from SQL object if possible
            # A more robust way would be to get schema from SQL.analysis_result.columns if populated
            return pa.Table.from_arrays([], names=[]) # Or try to get names from statement.analysis_result

        # Assuming RowT is typically list[dict[str, Any]]
        # Extract column names from the first row, assuming all rows have the same structure.
        column_names = list(rows[0].keys()) if isinstance(rows[0], dict) else []
        if not column_names:
            # Fallback if rows are not dicts or first row is empty dict
            # This part might need more robust schema inference or rely on statement analysis
            raise ValueError("Cannot determine column names for Arrow conversion from non-dict rows or empty first row.")

        # Convert list of dicts to list of PyArrow arrays (one per column)
        # This is a common pattern: transpose the list of dicts.
        columns_data = {col_name: [row.get(col_name) for row in rows] for col_name in column_names} # type: ignore

        # Create PyArrow arrays for each column
        pa_arrays = []
        for col_name in column_names:
            try:
                pa_arrays.append(pa.array(columns_data[col_name]))
            except Exception as e: # Broad exception for various pyarrow conversion errors
                raise SQLConversionError(f"Error converting column '{col_name}' to Arrow: {e}") from e

        return pa.Table.from_arrays(pa_arrays, names=column_names)
    ```

* **`AsyncArrowMixin._convert_rows_to_arrow`:**
    * The default conversion logic from `list[RowT]` to `ArrowTable` is synchronous. If `_convert_rows_to_arrow` itself needs to be async for some reason (e.g., async-specific `pyarrow` features, which are rare for basic table creation), it would need an `async def`. Otherwise, it can be a sync method even in an async mixin, called via `await self._convert_rows_to_arrow(...)` if the method itself becomes `async`, or just `self._convert_rows_to_arrow(...)` if it remains sync.
    * For this default, the conversion logic itself is CPU-bound, so a synchronous method is appropriate. The `AsyncArrowMixin` will just have the same synchronous default implementation.

**1.3. Implement Default `_write_rows_to_parquet` in `SyncParquetMixin` and `AsyncParquetMixin`**

* **`SyncParquetMixin._write_rows_to_parquet`:**

    ```python
    # In sqlspec.statement.mixins (SyncParquetMixin)
    # from pathlib import Path # Already in scope likely
    # from sqlspec.typing import PYARROW_INSTALLED, RowT # Already in scope likely
    # from sqlspec.exceptions import MissingDependencyError, SQLConversionError # Already in scope likely

    def _write_rows_to_parquet(self: 'ExporterMixinProtocol', rows: list[RowT], statement: 'SQL', path: Path, **kwargs: Any) -> None:
        """Default implementation to write a list of row data to a Parquet file."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError("pyarrow", "pyarrow")
        import pyarrow.parquet as pq

        # kwargs might contain specific options for Arrow conversion vs Parquet writing.
        # Separate them if necessary, e.g. arrow_kwargs = kwargs.pop("arrow_kwargs", {})
        arrow_table = self._convert_rows_to_arrow(rows, statement, **kwargs) # Pass all kwargs for now
        try:
            pq.write_table(arrow_table, path, **kwargs) # Pass remaining/relevant kwargs to Parquet writer
        except Exception as e:
            raise SQLConversionError(f"Error writing Arrow table to Parquet file '{path}': {e}") from e
    ```

* **`AsyncParquetMixin._write_rows_to_parquet`:**
    * Similar to Arrow, the Parquet writing itself using `pyarrow` is typically synchronous. If the `_write_rows_to_parquet` method in `AsyncParquetMixin` is `async def`, it would need to run the synchronous `pq.write_table` in a thread (e.g., `await anyio.to_thread.run_sync(pq.write_table, arrow_table, path, **kwargs)`).
    * The `_convert_rows_to_arrow` call would be `await self._convert_rows_to_arrow(...)` if that method is async, or `self._convert_rows_to_arrow(...)` if it's sync.
    * For simplicity, let's assume the default `_convert_rows_to_arrow` is sync. So, `AsyncParquetMixin`'s default would be:

    ```python
    # In sqlspec.statement.mixins (AsyncParquetMixin)
    async def _write_rows_to_parquet(self: 'ExporterMixinProtocol', rows: list[RowT], statement: 'SQL', path: Path, **kwargs: Any) -> None:
        if not PYARROW_INSTALLED:
            raise MissingDependencyError("pyarrow", "pyarrow")
        import pyarrow.parquet as pq
        from sqlspec.utils.sync_tools import async_ # Helper to run sync in thread

        arrow_table = self._convert_rows_to_arrow(rows, statement, **kwargs) # Assuming _convert_rows_to_arrow is sync
        try:
            await async_(pq.write_table)(arrow_table, path, **kwargs)
        except Exception as e:
            raise SQLConversionError(f"Error writing Arrow table to Parquet file '{path}': {e}") from e
    ```

### Phase 2: Refactor Public Mixin Methods and Instrumentation Flow

**2.1. Sync Arrow/Parquet Mixins**

* **Define Intermediate Protected Methods (Sync):**
    These methods are called by the instrumentation wrapper.

    ```python
    # In sqlspec.statement.mixins (e.g., part of SyncArrowMixin)
    # Needs access to driver's _execute and _wrap_select_result

    def _execute_and_fetch_for_arrow(self: 'SyncDriverAdapterProtocol', statement: 'SQL', connection: 'ConnectionT', **kwargs: Any) -> 'ArrowTable':
        # 1. Execute query using driver's standard path to get rows
        # This assumes _execute and _wrap_select_result are available on self (the driver)
        raw_driver_result = self._execute(statement.to_sql(placeholder_style=self.parameter_style), statement.parameters, statement, connection, **kwargs)
        sql_result: SQLResult[RowT] = self._wrap_select_result(statement, raw_driver_result, schema_type=None) # Get as RowT

        # 2. Call the overridable conversion method
        return self._convert_rows_to_arrow(sql_result.data if sql_result.data else [], statement, **kwargs)

    def _execute_and_write_to_parquet(self: 'SyncDriverAdapterProtocol', statement: 'SQL', connection: 'ConnectionT', path: Path, **kwargs: Any) -> None:
        raw_driver_result = self._execute(statement.to_sql(placeholder_style=self.parameter_style), statement.parameters, statement, connection, **kwargs)
        sql_result: SQLResult[RowT] = self._wrap_select_result(statement, raw_driver_result, schema_type=None)

        self._write_rows_to_parquet(sql_result.data if sql_result.data else [], statement, path, **kwargs)
    ```

* **Refactor Public `select_to_arrow` (Sync):**

    ```python
    # In sqlspec.statement.mixins (SyncArrowMixin)
    def select_to_arrow(self: 'SyncDriverAdapterProtocol', statement_like: 'SQLStatementLike', parameters: 'Optional[Parameters]' = None, *filters: 'StatementFilter', connection: 'Optional[ConnectionT]' = None, config: 'Optional[SQLConfig]' = None, **kwargs: 'Any') -> 'ArrowTable':
        stmt_obj: 'SQL' = self._build_statement(statement_like, parameters, filters=list(filters), config=config or self.config)
        if not self.returns_rows(stmt_obj.expression):
            raise TypeError("select_to_arrow can only be used with SELECT statements.")

        # The instrument_operation decorator/context manager needs to be able to call _execute_and_fetch_for_arrow
        # with the correct `self` (the driver instance).
        # The `instrument_operation` expects the function to be executed as its last positional args,
        # and `original_self` as the instance to call it on.
        return instrument_operation(
            self, # driver_obj for instrumentation context
            "select_to_arrow", # operation_name
            "database_export", # operation_type
            {}, # custom_tags_from_decorator
            SyncArrowMixin._execute_and_fetch_for_arrow, # func_to_execute (the intermediate method)
            self, # original_self (the driver instance on which _execute_and_fetch_for_arrow will be called)
            stmt_obj, # args for _execute_and_fetch_for_arrow
            self._connection(connection), # args for _execute_and_fetch_for_arrow
            **kwargs # kwargs for _execute_and_fetch_for_arrow
        ) # type: ignore
    ```

* **Refactor Public `to_parquet` (Sync):**

    ```python
    # In sqlspec.statement.mixins (SyncParquetMixin)
    def to_parquet(self: 'SyncDriverAdapterProtocol', statement_like: 'SQLStatementLike', path: Path, parameters: 'Optional[Parameters]' = None, *filters: 'StatementFilter', connection: 'Optional[ConnectionT]' = None, config: 'Optional[SQLConfig]' = None, **kwargs: 'Any') -> None:
        stmt_obj: 'SQL' = self._build_statement(statement_like, parameters, filters=list(filters), config=config or self.config)
        if not self.returns_rows(stmt_obj.expression):
            raise TypeError("to_parquet can only be used with SELECT statements.")

        instrument_operation(
            self,
            "to_parquet",
            "database_export",
            {},
            SyncParquetMixin._execute_and_write_to_parquet,
            self,
            stmt_obj,
            self._connection(connection),
            path=path, # path is a specific arg for _execute_and_write_to_parquet
            **kwargs
        )
    ```

**2.2. Async Arrow/Parquet Mixins**

* **Define Intermediate Protected Methods (Async):**

    ```python
    # In sqlspec.statement.mixins (e.g., part of AsyncArrowMixin)
    async def _execute_and_fetch_for_arrow(self: 'AsyncDriverAdapterProtocol', statement: 'SQL', connection: 'ConnectionT', **kwargs: Any) -> 'ArrowTable':
        raw_driver_result = await self._execute(statement.to_sql(placeholder_style=self.parameter_style), statement.parameters, statement, connection, **kwargs)
        sql_result: SQLResult[RowT] = await self._wrap_select_result(statement, raw_driver_result, schema_type=None)

        # _convert_rows_to_arrow is sync by default
        return self._convert_rows_to_arrow(sql_result.data if sql_result.data else [], statement, **kwargs)

    async def _execute_and_write_to_parquet(self: 'AsyncDriverAdapterProtocol', statement: 'SQL', connection: 'ConnectionT', path: Path, **kwargs: Any) -> None:
        raw_driver_result = await self._execute(statement.to_sql(placeholder_style=self.parameter_style), statement.parameters, statement, connection, **kwargs)
        sql_result: SQLResult[RowT] = await self._wrap_select_result(statement, raw_driver_result, schema_type=None)

        # _write_rows_to_parquet default implementation is async
        await self._write_rows_to_parquet(sql_result.data if sql_result.data else [], statement, path, **kwargs)
    ```

* **Refactor Public `select_to_arrow` (Async):**

    ```python
    # In sqlspec.statement.mixins (AsyncArrowMixin)
    async def select_to_arrow(self: 'AsyncDriverAdapterProtocol', statement_like: 'SQLStatementLike', parameters: 'Optional[Parameters]' = None, *filters: 'StatementFilter', connection: 'Optional[ConnectionT]' = None, config: 'Optional[SQLConfig]' = None, **kwargs: 'Any') -> 'ArrowTable':
        stmt_obj: 'SQL' = self._build_statement(statement_like, parameters, filters=list(filters), config=config or self.config)
        if not self.returns_rows(stmt_obj.expression):
            raise TypeError("select_to_arrow can only be used with SELECT statements.")

        return await instrument_operation_async(
            self,
            "select_to_arrow",
            "database_export",
            {},
            AsyncArrowMixin._execute_and_fetch_for_arrow,
            self,
            stmt_obj,
            self._connection(connection),
            **kwargs
        ) # type: ignore
    ```

* **Refactor Public `to_parquet` (Async):**

    ```python
    # In sqlspec.statement.mixins (AsyncParquetMixin)
    async def to_parquet(self: 'AsyncDriverAdapterProtocol', statement_like: 'SQLStatementLike', path: Path, parameters: 'Optional[Parameters]' = None, *filters: 'StatementFilter', connection: 'Optional[ConnectionT]' = None, config: 'Optional[SQLConfig]' = None, **kwargs: 'Any') -> None:
        stmt_obj: 'SQL' = self._build_statement(statement_like, parameters, filters=list(filters), config=config or self.config)
        if not self.returns_rows(stmt_obj.expression):
            raise TypeError("to_parquet can only be used with SELECT statements.")

        await instrument_operation_async(
            self,
            "to_parquet",
            "database_export",
            {},
            AsyncParquetMixin._execute_and_write_to_parquet,
            self,
            stmt_obj,
            self._connection(connection),
            path=path,
            **kwargs
        )
    ```

**Note on `instrument_operation` usage:** The `func_to_execute` argument for `instrument_operation` and `instrument_operation_async` should be the intermediate methods (`_execute_and_fetch_for_arrow`, etc.). These intermediate methods must be defined as methods of the *Mixin class itself* (or a base class they both inherit from, if they are identical for sync/async logic aside from `await`). If they are instance methods, they need to be passed correctly. Given the structure, defining them as staticmethods within the mixin or helper functions might be cleaner if they don't need `self` of the mixin, but they do need `self` of the *driver* to call `_execute`, `_wrap_select_result`, `_convert_rows_to_arrow`. So, they should be methods of the mixin, and `original_self` for `instrument_operation` will be the driver instance.

The `SyncArrowMixin._execute_and_fetch_for_arrow` (and its async counterpart) seems correct as an instance method of the mixin that will be bound to the driver instance at runtime. The `instrument_operation` calls seem to pass `self` (the driver instance) as `original_self` and then the unbound mixin method as `func_to_execute` - this should work as `instrument_operation` will then call `func_to_execute(original_self, *args, **kwargs)`.

### Phase 3: Driver Modifications

* **Identify Drivers:**
    * **Native Arrow Support (Override `_convert_rows_to_arrow`):**
        * `AdbcDriver`: Will rename/adapt its existing `_select_to_arrow_impl`.
        * `DuckDBDriver` (Sync/Async if separate): Will use its native `relation.arrow()`.
        * `OracleSyncDriver` / `OracleAsyncDriver`: Investigate `fetch_df_all()` or other native Arrow capabilities.
        * `BigQueryDriver`: Use `query_job.to_arrow()`.
    * **No Native Arrow Support (Will use default `_convert_rows_to_arrow`):**
        * `AiosqliteDriver`: Default implementation likely based on its current manual conversion.
        * `AsyncmyDriver`: Default implementation likely based on its current manual conversion.
        * `AsyncpgDriver`: Default implementation likely based on its current manual conversion.
        * `PsqlpyDriver`: Will use the new default.
        * `PsycopgSyncDriver` / `PsycopgAsyncDriver`: Will use the new default (will gain Arrow export).
        * `SqliteDriver`: Will use the new default (will gain Arrow export).
* **Native Parquet Support (Override `_write_rows_to_parquet`):**
    * `DuckDBDriver`: May have native `COPY TO PARQUET`.
    * `AdbcDriver`: May support for some underlying DBs.
    * Others will likely use the default which relies on `_convert_rows_to_arrow`.

* **Implementation for Overrides:**
    * Drivers overriding `_convert_rows_to_arrow` will typically ignore the `rows: list[RowT]` argument and use the `statement: SQL` object to perform their native Arrow fetching mechanism (e.g., `connection.execute(statement.to_sql()).fetch_arrow()`).
    * Similar logic for `_write_rows_to_parquet` overrides.

### Phase 4: Testing Updates

* **Unit Tests:**
    * Test public mixin methods (`select_to_arrow`, `to_parquet`) to ensure they call the instrumentation wrapper correctly.
    * Test the intermediate methods (`_execute_and_fetch_for_arrow`, etc.) to ensure they correctly call driver's `_execute`, `_wrap_select_result`, and then the appropriate `_convert_rows_to_arrow` or `_write_rows_to_parquet`.
    * Test the default implementations of `_convert_rows_to_arrow` and `_write_rows_to_parquet` for correct `list[RowT]` to Arrow/Parquet conversion and `pyarrow` dependency handling.
    * For drivers overriding conversion methods, test their specific native implementations.
* **Integration Tests:**
    * Verify end-to-end functionality for all drivers, testing both default and native paths.
    * Test `**kwargs` pass-through to underlying Arrow/Parquet libraries.

### Phase 5: Documentation

* Update docstrings for public mixin methods and the new protected conversion methods (`_convert_rows_to_arrow`, `_write_rows_to_parquet`).
* Document the default conversion behavior and how drivers can override it for native support.
* Clearly state the `pyarrow` dependency for default implementations.

This revised plan provides a more robust and flexible architecture for Arrow/Parquet exports, promoting code reuse while allowing for driver-specific optimizations.

---

## Plan: DDL (Data Definition Language) Builders

**Date:** 2024-07-27

**Objective:**
To create a suite of DDL builders for SQLSpec, enabling programmatic construction of `CREATE`, `ALTER`, `DROP`, and other DDL statements in a manner consistent with existing DML builders, leveraging `sqlglot` for AST manipulation.

**Guiding Principles:**

1. **Separate Builder Hierarchy:** DDL builders will likely have their own base class (e.g., `DDLBuilder`) distinct from `QueryBuilder`, as DDL operations have different characteristics (e.g., usually no runtime parameters except for CTAS, different result types).
2. **SQLGlot Expression Core:** Each builder will internally manage a corresponding `sqlglot` expression (e.g., `exp.Create`, `exp.AlterTable`, `exp.Drop`).
3. **Fluent Interface:** Methods will allow chaining to construct the DDL statement progressively.
4. **`build()` and `to_statement()`:**
    * `build()`: Generates the raw SQL string for the DDL statement.
    * `to_statement()`: Wraps the generated SQL string into an `SQL` object. The `SQLConfig` for these `SQL` objects might default to disabling parameter validation and other query-specific processing, as DDL statements are typically static.
5. **Dialect Awareness:** Builders should support dialect-specific syntax where necessary, leveraging `sqlglot`'s dialect handling.
6. **Error Handling:** Use `SQLBuilderError` for invalid builder usage or unsupported DDL constructs.
7. **Arrow/Parquet Interaction:** DDL primarily defines schema. Direct interaction with Arrow/Parquet data is minimal but can occur in table storage definitions (e.g., `STORED AS PARQUET`) or CTAS sources.

---

### Feasibility and Prioritization of DDL Builders

#### Tier 1: Easiest (Core DDL Operations - Highest Priority)

These involve relatively straightforward `sqlglot` expressions and common DDL commands.

1. **`DropTableBuilder`**
    * **SQL:** `DROP TABLE [IF EXISTS] table_name [CASCADE | RESTRICT]`
    * **SQLGlot:** `sqlglot.exp.Drop(kind='TABLE', this=exp.to_table(name), exists=True/False, cascade=True/False)`
    * **Builder Methods:**
        * `table(name: str) -> Self`: Sets the table name.
        * `if_exists() -> Self`: Adds `IF EXISTS`.
        * `cascade() -> Self`: Adds `CASCADE`.
        * `restrict() -> Self`: Adds `RESTRICT`.
    * **Notes:** Simple to implement. `cascade` and `restrict` are mutually exclusive; builder should enforce this.

2. **`DropIndexBuilder`**
    * **SQL:** `DROP INDEX [IF EXISTS] index_name [ON table_name] [CASCADE | RESTRICT]`
    * **SQLGlot:** `sqlglot.exp.Drop(kind='INDEX', this=exp.to_identifier(name), exists=True/False, table=exp.to_table(table_name), cascade=True/False)`
    * **Builder Methods:**
        * `name(index_name: str) -> Self`
        * `on_table(table_name: str) -> Self` (Optional, dialect-dependent)
        * `if_exists() -> Self`
        * `cascade() -> Self`
        * `restrict() -> Self`

3. **`DropViewBuilder`**
    * **SQL:** `DROP VIEW [IF EXISTS] view_name [CASCADE | RESTRICT]`
    * **SQLGlot:** `sqlglot.exp.Drop(kind='VIEW', this=exp.to_identifier(name), exists=True/False, cascade=True/False)`
    * **Builder Methods:** Similar to `DropTableBuilder`.

4. **`DropSchemaBuilder`**
    * **SQL:** `DROP SCHEMA [IF EXISTS] schema_name [CASCADE | RESTRICT]`
    * **SQLGlot:** `sqlglot.exp.Drop(kind='SCHEMA', this=exp.to_identifier(name), exists=True/False, cascade=True/False)`
    * **Builder Methods:** Similar to `DropTableBuilder`.

5. **`CreateIndexBuilder`**
    * **SQL:** `CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column1 [ASC|DESC] [NULLS FIRST|LAST], ...) [USING method] [WHERE predicate]`
    * **SQLGlot:** `sqlglot.exp.CreateIndex(this=exp.to_identifier(index_name), table=exp.to_table(table_name), expressions=[exp.Ordered(this=exp.column(col), desc=True/False, nulls_first=True/False), ...], unique=True/False, exists=True/False, using=exp.to_identifier(method), where=exp.condition(predicate))`
    * **Builder Methods:**
        * `name(index_name: str) -> Self`
        * `on_table(table_name: str) -> Self`
        * `columns(*cols: Union[str, exp.Ordered]) -> Self`: Accepts column names or `exp.column("col").desc()` for ordering.
        * `expressions(*exprs: Union[str, exp.Expression]) -> Self`: For expression-based indexes.
        * `unique() -> Self`
        * `if_not_exists() -> Self`
        * `using(method: str) -> Self`: (e.g., BTREE, HASH, GIN, GIST)
        * `where(condition: str) -> Self`

6. **`TruncateTableBuilder`**
    * **SQL:** `TRUNCATE TABLE table_name [CASCADE | RESTRICT] [RESTART IDENTITY | CONTINUE IDENTITY]`
    * **SQLGlot:** `sqlglot.exp.TruncateTable(this=exp.to_table(name), cascade=True/False, identity=exp.Var(this="RESTART"/"CONTINUE"))`
    * **Builder Methods:**
        * `table(name: str) -> Self`
        * `cascade() -> Self` / `restrict() -> Self`
        * `restart_identity() -> Self` / `continue_identity() -> Self`

#### Tier 2: Medium Complexity (Basic Table/Schema Creation, Simple Alterations)

These involve more structured `sqlglot` expressions, particularly `exp.Create` and `exp.AlterTable` with common actions.

1. **`CreateSchemaBuilder`**
    * **SQL:** `CREATE SCHEMA [IF NOT EXISTS] schema_name [AUTHORIZATION user_name]`
    * **SQLGlot:** `sqlglot.exp.Create(kind='SCHEMA', this=exp.to_identifier(schema_name), exists=True/False, expressions=[exp.Property(this=exp.to_identifier("AUTHORIZATION"), value=exp.to_identifier(user_name))])` (Authorization might need specific property handling or be part of `expressions` depending on `sqlglot` version/dialect target for `CREATE SCHEMA`).
    * **Builder Methods:**
        * `name(schema_name: str) -> Self`
        * `if_not_exists() -> Self`
        * `authorization(user_name: str) -> Self`

2. **`CreateTableBuilder` (Basic Columns & Constraints)**
    * **SQL:** `CREATE TABLE [IF NOT EXISTS] table_name ( column1 datatype [CONSTRAINT_DEF], ... [TABLE_CONSTRAINT_DEF] )`
    * **SQLGlot:** `sqlglot.exp.Create(kind='TABLE', this=exp.to_table(table_name), schema=exp.Schema(expressions=[exp.ColumnDef(...), exp.Constraint(...) ]), exists=True/False)`
    * **Builder Methods:**
        * `name(table_name: str) -> Self`
        * `if_not_exists() -> Self`
        * `column(name: str, dtype: str, default: Optional[Any]=None, not_null: bool=False, primary_key: bool=False, unique: bool=False, check: Optional[str]=None, references_table: Optional[str]=None, references_col: Optional[str]=None, collate: Optional[str]=None) -> Self`: Adds `exp.ColumnDef`. Constraints here are column constraints.
        * `primary_key_constraint(cols: Union[str, list[str]], name: Optional[str]=None) -> Self`: Adds `exp.PrimaryKey` constraint to table schema.
        * `unique_constraint(cols: Union[str, list[str]], name: Optional[str]=None) -> Self`: Adds `exp.Unique` constraint.
        * `foreign_key_constraint(cols: Union[str, list[str]], references_table: str, references_cols: Union[str, list[str]], name: Optional[str]=None, on_delete: Optional[str]=None, on_update: Optional[str]=None) -> Self`: Adds `exp.ForeignKey`.
        * `check_constraint(expression: str, name: Optional[str]=None) -> Self`: Adds `exp.Check`.
    * **Notes:** Column definition needs to handle various constraint types (`exp.NotNullColumnConstraint`, `exp.DefaultColumnConstraint`, `exp.PrimaryKeyColumnConstraint`, `exp.UniqueColumnConstraint`, `exp.ReferenceColumnConstraint`, `exp.CheckColumnConstraint`).

3. **`CreateTableAsSelectBuilder` (CTAS)**
    * **SQL:** `CREATE TABLE [IF NOT EXISTS] table_name [(col1, col2, ...)] AS SELECT ...`
    * **SQLGlot:** `sqlglot.exp.Create(kind='TABLE', this=exp.to_table(table_name), expression=exp.Select(...), exists=True/False, properties=[exp.Schema(expressions=[exp.column(c) for c in columns])])` (Column list is optional).
    * **Builder Methods:**
        * `name(table_name: str) -> Self`
        * `if_not_exists() -> Self`
        * `columns(*cols: str) -> Self` (Optional: for explicit column naming in CTAS)
        * `as_select(select_query: Union[SQL, 'SelectBuilder', str]) -> Self`: Takes an existing `SQL` object, `SelectBuilder`, or raw SELECT string.
            * Parameters from `select_query` must be extracted and managed by the CTAS builder if the `SELECT` part is parameterized. The final `SQL` object from `CTASBuilder.to_statement()` will contain these parameters.
    * **Arrow/Parquet Interaction:** If `select_query` is an `SQL` object that was (hypothetically) derived from an Arrow/Parquet source, its parameters would be incorporated. The CTAS itself is a DDL operation.

4. **`AlterTableBuilder` (Simple Column/Constraint Operations)**
    * **SQL:** `ALTER TABLE table_name ADD COLUMN ...`, `DROP COLUMN ...`, `RENAME COLUMN ...`, `ALTER COLUMN ... TYPE ...`, `ADD CONSTRAINT ...`, `DROP CONSTRAINT ...`
    * **SQLGlot:** `sqlglot.exp.AlterTable(this=exp.to_table(name), actions=[exp.AddColumn(...), exp.DropColumn(...), exp.AlterColumn(...), exp.AddConstraint(...)])`.
    * **Builder Methods (each action creates a new `AlterTable` or appends to `actions`):**
        * `table(name: str) -> Self`
        * `add_column(name: str, dtype: str, default: Optional[Any]=None, not_null: bool=False, ...) -> Self`
        * `drop_column(name: str, if_exists: bool=False, cascade: bool=False) -> Self`
        * `rename_column(old_name: str, new_name: str) -> Self`
        * `alter_column_type(col_name: str, new_dtype: str, using: Optional[str]=None) -> Self`
        * `set_column_default(col_name: str, default_value: Any) -> Self`
        * `drop_column_default(col_name: str) -> Self`
        * `set_column_not_null(col_name: str, set_not_null: bool = True) -> Self` (handles SET and DROP NOT NULL)
        * `add_primary_key_constraint(cols: Union[str, list[str]], name: Optional[str]=None) -> Self`
        * `add_unique_constraint(cols: Union[str, list[str]], name: Optional[str]=None) -> Self`
        * `add_foreign_key_constraint(...) -> Self`
        * `add_check_constraint(expression: str, name: Optional[str]=None) -> Self`
        * `drop_constraint(name: str, if_exists: bool=False, cascade: bool=False) -> Self`
    * **Notes:** `AlterTable` can have multiple actions. Builder methods might either build a single-action `AlterTable` or accumulate actions. Accumulating is more flexible but complex.

#### Tier 3: Harder Complexity (Advanced Table Features, Dialect-Specifics)

These require deeper `sqlglot` knowledge, handling of dialect-specific properties, or potentially extending `sqlglot` if features are not fully represented.

1. **`CreateTableBuilder` (Advanced Features)**
    * **SQL:** `... PARTITION BY ...`, `... WITH (storage_parameter = value, ...)`, `... STORED AS PARQUET`, `... INHERITS (parent_table)`
    * **SQLGlot:** `props` argument in `exp.Create` can take `exp.Property` (e.g., `exp.FileFormatProperty`, `exp.PartitionedByProperty`). Inheritance might need `exp.Inherits`.
    * **Builder Methods:**
        * `partition_by_range(column: str, ...)` / `partition_by_list(column: str, ...)`
        * `storage_parameters(**params) -> Self`: For `WITH (...)` clauses.
        * `stored_as(file_format: str) -> Self`: e.g., `PARQUET`, `ORC`.
        * `inherits(parent_table_name: str) -> Self`
    * **Arrow/Parquet Interaction:** `stored_as("PARQUET")` directly relates to Parquet. The database handles the actual storage format.

2. **`AlterTableBuilder` (Advanced Operations)**
    * **SQL:** `... ATTACH/DETACH PARTITION ...`, `... RENAME TO new_name`, `... SET SCHEMA new_schema`, `... SET TABLESPACE ...`
    * **SQLGlot:** These might be generic `exp.AlterTable` actions with specific string commands if `sqlglot` doesn't have dedicated expression types, or might require custom expression node generation if not supported. `RENAME TO` is often `exp.RenameTable`.
    * **Builder Methods:**
        * `attach_partition(...) -> Self`
        * `detach_partition(...) -> Self`
        * `rename_to(new_table_name: str) -> Self`
        * `set_schema(new_schema_name: str) -> Self`
        * `set_tablespace(tablespace_name: str) -> Self`

3. **Dialect-Specific DDL Builders (e.g., `CreateTypeBuilder`, `CreateFunctionBuilder`)**
    * **SQL:** `CREATE TYPE name AS ...`, `CREATE FUNCTION name (...) RETURNS ... AS ... LANGUAGE ...`
    * **SQLGlot:** Often `exp.Create(kind='TYPE', ...)` or `exp.Create(kind='FUNCTION', ...)`. Body of functions/procedures might be `exp.LiteralString`.
    * **Builder Methods:** Highly specific to the DDL type (e.g., `CreateTypeBuilder.as_enum(*values)`, `CreateFunctionBuilder.returns(dtype).language(lang).body(sql_body)`).
    * **Notes:** This is a large area. Start with the most commonly needed types/functions.

4. **`AlterIndexBuilder`, `AlterViewBuilder`, `AlterSchemaBuilder`**
    * Generally involve `RENAME TO` or other dialect-specific alterations.
    * `sqlglot.exp.RenameObject` might be relevant for renames.
    * **Notes:** Lower priority unless specific ALTER operations for these object types are frequently needed.

---

**General Implementation Approach for DDL Builders:**

1. **Base `DDLBuilder` Class:**
    * Similar to `QueryBuilder` but simpler; likely won't manage parameters directly (except for CTAS).
    * Holds a `dialect: Optional[DialectType]` and `_expression: Optional[exp.Expression]`.
    * `build() -> str`: Returns `self._expression.sql(dialect=self.dialect_name)`.
    * `to_statement() -> SQL`: Creates an `SQL` object. `SQLConfig` for DDL might disable validation/transformation by default.

2. **Builder-Specific Logic:**
    * Each builder (e.g., `CreateTableBuilder`) initializes `_expression` with the correct `sqlglot` root node (e.g., `exp.Create(kind='TABLE', ...)`).
    * Methods like `column()`, `if_not_exists()`, `on_conflict_do_nothing()` manipulate this AST node.
        * Example for `CreateTableBuilder.column(name, dtype)`: `self._expression.args.get("schema").append("expressions", exp.ColumnDef(this=exp.to_identifier(name), kind=exp.DataType.build(dtype)))`

3. **Leveraging `sqlglot.exp`:**
    * Utilize `sqlglot.exp` module extensively for creating parts of the DDL (e.g., `exp.ColumnDef`, `exp.Constraint`, `exp.PrimaryKey`, `exp.FileFormatProperty`).
    * Use `exp.to_identifier()` for names, `exp.to_table()` for table references, `exp.condition()` for parsing string conditions.

4. **Dialect Considerations:**
    * Many DDL features are dialect-specific (e.g., storage parameters, partitioning details). `sqlglot` handles some of this through its dialect-aware SQL generation from the AST.
    * For features `sqlglot` doesn't abstract perfectly, the builder might need to construct slightly different ASTs based on the target dialect or use `exp.Anonymous` or `exp.Command` for highly specific syntax.

This detailed breakdown should provide a solid foundation for implementing DDL builders within SQLSpec, progressively tackling features from common and simple to more complex and dialect-specific ones.

---

## Plan: Advanced Features – Query Hints and Materialized Views

**Date:** 2024-07-28

### 1. `with_hint` Feature for SelectBuilder and Other Builders

**Objective:**
Enable users to inject optimizer hints or dialect-specific query hints into SELECT (and other relevant) statements via the builder API, supporting both standard SQL hints (e.g., Oracle, SQL Server, PostgreSQL, MySQL) and vendor-specific syntaxes.

**Rationale:**

* Power users and DBAs often require fine-grained control over query plans via hints (e.g., `/*+ INDEX(...) */`, `OPTION (RECOMPILE)`, `FORCE INDEX`, etc.).
* Hints are highly dialect-specific but must be supported in a composable, safe, and testable way.
* The builder API should allow hints to be attached at the statement level and, where appropriate, at the table or join level.

**API Design:**

* Add a `.with_hint(hint: str, *, location: str = "statement", table: Optional[str] = None, dialect: Optional[str] = None) -> Self` method to `SelectBuilder` (and potentially `UpdateBuilder`, `DeleteBuilder`).
    * `hint`: The raw hint string (e.g., `INDEX(users idx_users_name)` or `NOLOCK`).
    * `location`: Where to apply the hint (`"statement"`, `"table"`, `"join"`).
    * `table`: If the hint is for a specific table, provide the table name.
    * `dialect`: Optionally restrict the hint to a specific dialect.
* Hints should be stored in a `_hints` attribute (list of dicts or objects) on the builder.
* On build, hints are injected into the SQL AST using `sqlglot`'s support for `exp.Hint`, `exp.TableHint`, or by manipulating the raw SQL string if necessary.
* Hints should be included in the `SafeQuery` and `SQL` objects for downstream driver/adapter awareness.

**Implementation Plan:**

* Extend `SelectBuilder` (and other relevant builders) with a `_hints` attribute and a `.with_hint()` method as described.
* On `build()`, inject hints into the AST:
    * For statement-level hints: add `exp.Hint(this=...)` as the first child of the statement.
    * For table/join hints: attach `exp.TableHint` or similar to the relevant table node.
    * If `sqlglot` lacks support for a specific hint, fallback to prepending/appending the hint as a comment in the SQL string.
* Ensure hints are preserved in the `SQL` object for driver-level processing if needed.
* Add tests for major dialects (Oracle, SQL Server, PostgreSQL, MySQL, DuckDB, etc.).

**Example Usage:**

```python
query = (
    SelectBuilder()
    .select("id", "name")
    .from_("users")
    .with_hint("INDEX(users idx_users_name)", location="table", table="users", dialect="oracle")
    .with_hint("NOLOCK", location="table", table="users", dialect="sqlserver")
    .with_hint("/*+ PARALLEL(4) */", location="statement", dialect="oracle")
)
```

**Integration Notes:**

* Update the DML builder enhancement section to reference the new `with_hint` feature.
* Ensure mixins that manipulate FROM/JOIN clauses are compatible with table/join-level hints.

---

### 2. `CreateMaterializedViewBuilder` for DDL

**Objective:**
Provide a builder for `CREATE MATERIALIZED VIEW` statements, supporting all major options (e.g., `AS SELECT`, `REFRESH` options, `WITH DATA/NO DATA`, storage parameters, etc.), with dialect awareness.

**Rationale:**

* Materialized views are a key feature in many databases (PostgreSQL, Oracle, SQL Server, MySQL, DuckDB, etc.).
* Syntax and options vary by dialect, but a unified builder API can cover the common core and allow for dialect-specific extensions.

**API Design:**

* New class: `CreateMaterializedViewBuilder(DDLBuilder)`
* Methods:
    * `.name(view_name: str) -> Self`
    * `.if_not_exists() -> Self`
    * `.columns(*cols: str) -> Self` (optional column list)
    * `.as_select(select_query: Union[SQL, SelectBuilder, str]) -> Self`
    * `.with_data() -> Self` / `.no_data() -> Self` (Postgres, DuckDB)
    * `.refresh_mode(mode: str) -> Self` (e.g., `ON COMMIT`, `ON DEMAND`, `FAST`, `COMPLETE`, `IMMEDIATE`)
    * `.storage_parameter(key: str, value: Any) -> Self` (for dialects supporting storage options)
    * `.tablespace(name: str) -> Self` (Postgres, Oracle)
    * `.using_index(index_name: str) -> Self` (Oracle, etc.)
    * `.with_hint(hint: str, ...) -> Self` (see above)
* On `build()`, generates the appropriate `exp.Create(kind="MATERIALIZED_VIEW", ...)` AST, with dialect-specific properties as needed.
* On `to_statement()`, merges parameters from the SELECT source if present.

**Implementation Plan:**

* Implement `CreateMaterializedViewBuilder` in `sqlspec.statement.builder.ddl`.
* Add to `__all__` in `sqlspec/statement/builder/__init__.py`.
* Ensure parameter merging from the SELECT source, with preservation of user-supplied parameter names.
* Add dialect-specific logic for `WITH DATA`, `NO DATA`, `REFRESH`, etc.
* Add tests for major dialects (Postgres, Oracle, DuckDB, etc.).

**Example Usage:**

```python
view_builder = (
    CreateMaterializedViewBuilder()
    .name("active_users_mv")
    .if_not_exists()
    .columns("id", "name")
    .as_select(select_builder)
    .with_data()
    .refresh_mode("ON COMMIT")
    .tablespace("fastspace")
    .with_hint("/*+ PARALLEL(2) */", location="statement", dialect="oracle")
)
```

**Integration Notes:**

* Reference this builder in the DDL builder plan (Tier 2 or 3, depending on dialect support).
* Ensure the builder is compatible with the parameter name preservation and configuration architecture.

---

### 3. Cross-References and Section Updates

* **DML Builder Enhancements:**
    * Add `with_hint` as a required feature for `SelectBuilder` and recommend for other DML builders where supported by the dialect.
    * Ensure parameter merging and name preservation logic is compatible with hints (e.g., hints referencing parameterized columns).
* **DDL Builder Plan:**
    * Add `CreateMaterializedViewBuilder` to the list of Tier 2 DDL builders, with a note on dialect-specific support and options.
* **Mixin Architecture:**
    * Ensure mixins for FROM, JOIN, and table clauses are extensible to support table/join-level hints.
* **Testing:**
    * Add tests for hint injection and materialized view creation for all supported dialects.
* **Documentation:**
    * Document the new `with_hint` and `CreateMaterializedViewBuilder` features in the usage and reference docs.

---

**These additions complete the actionable plan for rapid implementation of all requested features, with sufficient detail for direct engineering execution.**

---
