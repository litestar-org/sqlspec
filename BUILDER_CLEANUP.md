## Refactoring Plan: SQL Builder Mixins

**Goal:** Decompose the large builder classes (`sqlspec/statement/builder/_select.py`, `sqlspec/statement/builder/_insert.py`, etc.) into smaller, reusable mixin classes, each responsible for a specific set of SQL clauses or functionalities. This will improve code organization, reduce file sizes, and make the system more extensible.

**Core Idea:** Identify common sets of methods that can be grouped logically. For example, methods related to `WHERE` clauses, `JOIN` clauses, `ORDER BY` clauses, etc., can each become a mixin. The main builder classes will then inherit from these mixins.

---

### 1. Detailed Breakdown and Mixin Design

We'll analyze each builder and identify potential mixins. The `_base.py` will likely remain the core, and other builders will compose their functionality from it and the new mixins.

#### A. General Mixins (Potentially applicable to multiple builders)

1. **`WhereClauseMixin`**:
    * **Responsibility:** Handling `WHERE` conditions.
    * **Methods:** `where()`, `and_where()`, `or_where()`.
    * **Potential Users:** `SelectBuilder`, `UpdateBuilder`, `DeleteBuilder`.
    * **File:** `sqlspec/statement/builder/mixins/_where.py`

2. **`OrderByClauseMixin`**:
    * **Responsibility:** Handling `ORDER BY` clauses.
    * **Methods:** `order_by()`.
    * **Potential Users:** `SelectBuilder`.
    * **File:** `sqlspec/statement/builder/mixins/_order_by.py`

3. **`LimitOffsetClauseMixin`**:
    * **Responsibility:** Handling `LIMIT` and `OFFSET` clauses.
    * **Methods:** `limit()`, `offset()`.
    * **Potential Users:** `SelectBuilder`.
    * **File:** `sqlspec/statement/builder/mixins/_limit_offset.py`

4. **`ReturningClauseMixin`**:
    * **Responsibility:** Handling `RETURNING` clauses.
    * **Methods:** `returning()`.
    * **Potential Users:** `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`.
    * **File:** `sqlspec/statement/builder/mixins/_returning.py`

5. **`CommonTableExpressionMixin` (CTE Mixin)**:
    * **Responsibility:** Handling `WITH` clauses (Common Table Expressions).
    * **Methods:** `with_()` (consider renaming to `cte()` if `with_` is problematic), `with_recursive()`.
    * **Potential Users:** `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`.
    * **File:** `sqlspec/statement/builder/mixins/_cte.py`

#### B. `SelectBuilder` Specific Mixins (from `_select.py`)

1. **`SelectColumnsMixin`**:
    * **Responsibility:** Managing the `SELECT` column list.
    * **Methods:** `select()`, `add_select()`, potentially `distinct()`.
    * **File:** `sqlspec/statement/builder/mixins/_select_columns.py`

2. **`FromClauseMixin`**:
    * **Responsibility:** Managing the `FROM` clause.
    * **Methods:** `from_()`.
    * **File:** `sqlspec/statement/builder/mixins/_from.py`

3. **`JoinClauseMixin`**:
    * **Responsibility:** Handling all types of `JOIN` clauses.
    * **Methods:** `join()`, `left_join()`, `right_join()`, `inner_join()`, `full_join()`, `cross_join()`, `on()`.
    * **File:** `sqlspec/statement/builder/mixins/_join.py`

4. **`GroupByClauseMixin`**:
    * **Responsibility:** Handling `GROUP BY` clauses.
    * **Methods:** `group_by()`.
    * **File:** `sqlspec/statement/builder/mixins/_group_by.py`

5. **`HavingClauseMixin`**:
    * **Responsibility:** Handling `HAVING` conditions.
    * **Methods:** `having()`, `and_having()`, `or_having()`.
    * **File:** `sqlspec/statement/builder/mixins/_having.py`

6. **`SetOperationMixin` (for UNION, INTERSECT, EXCEPT)**:
    * **Responsibility:** Handling set operations.
    * **Methods:** `union()`, `union_all()`, `intersect()`, `except_()`.
    * **File:** `sqlspec/statement/builder/mixins/_set_ops.py`

#### C. `InsertBuilder` Specific Mixins (from `_insert.py`)

1. **`InsertValuesMixin`**:
    * **Responsibility:** Managing `VALUES` clauses and column lists for insert.
    * **Methods:** `columns()`, `values()`, `add_values()`.
    * **File:** `sqlspec/statement/builder/mixins/_insert_values.py`

2. **`InsertFromSelectMixin`**:
    * **Responsibility:** Handling `INSERT INTO ... SELECT ...` statements.
    * **Methods:** `from_select()`.
    * **File:** `sqlspec/statement/builder/mixins/_insert_from_select.py`

#### D. `UpdateBuilder` Specific Mixins (from `_update.py`)

1. **`UpdateSetClauseMixin`**:
    * **Responsibility:** Managing `SET` clauses for updates.
    * **Methods:** `set()`.
    * **File:** `sqlspec/statement/builder/mixins/_update_set.py`

2. **`UpdateFromClauseMixin` (if applicable)**:
    * **Responsibility:** Handling `FROM` clauses in `UPDATE` statements.
    * **Methods:** `from_()`.
    * **File:** `sqlspec/statement/builder/mixins/_update_from.py`

#### E. `DeleteBuilder` Specific Mixins (from `_delete.py`)

Likely simpler, primarily using general mixins.

#### F. `MergeBuilder` Specific Mixins (from `_merge.py`)

All `MergeBuilder`-specific mixins will be defined in a single file for better cohesion, as their functionalities are tightly coupled to the `MERGE` statement.

* **File:** `sqlspec/statement/builder/mixins/_merge_clauses.py`
    * **`MergeUsingClauseMixin`** (defined in `_merge_clauses.py`):
        * **Responsibility:** Managing the `USING` clause.
        * **Methods:** `using()`.
    * **`MergeOnClauseMixin`** (defined in `_merge_clauses.py`):
        * **Responsibility:** Managing the `ON` condition for the merge.
        * **Methods:** `on()`.
    * **`MergeMatchedClauseMixin`** (defined in `_merge_clauses.py`):
        * **Responsibility:** Handling `WHEN MATCHED THEN ...` clauses.
        * **Methods:** `when_matched_then_update()`, `when_matched_then_delete()`, etc.
    * **`MergeNotMatchedClauseMixin`** (defined in `_merge_clauses.py`):
        * **Responsibility:** Handling `WHEN NOT MATCHED THEN ...` clauses.
        * **Methods:** `when_not_matched_then_insert()`, etc.

---

### 2. File Structure Changes

* **New Directory:** `sqlspec/statement/builder/mixins/`
    * Will contain all new mixin files (e.g., `_where.py`, `_merge_clauses.py`).
    * Add `__init__.py` to this directory to export all mixin classes for easy consolidated import by builder modules.

        ```python
        # sqlspec/statement/builder/mixins/__init__.py
        from ._where import WhereClauseMixin
        from ._join import JoinClauseMixin
        from ._merge_clauses import (
            MergeUsingClauseMixin,
            MergeOnClauseMixin,
            MergeMatchedClauseMixin,
            MergeNotMatchedClauseMixin,
        )
        # ... import and list in __all__ for all defined mixins ...

        __all__ = [
            "WhereClauseMixin",
            "JoinClauseMixin",
            "MergeUsingClauseMixin",
            "MergeOnClauseMixin",
            "MergeMatchedClauseMixin",
            "MergeNotMatchedClauseMixin",
            # ... etc. ...
        ]
        ```

* **Original Builder Definition Files (e.g., `_select.py`, `_insert.py`, `_merge.py`):**
    * These files will be significantly slimmed down. Their primary role will be to define the main builder class by inheriting from `BaseBuilder` and the relevant new mixins.
    * The `_merge.py` (or its renamed version like `merge.py`) would import the four merge-related mixins from `mixins._merge_clauses`.
    * **Consider renaming these files** after refactoring (e.g., `_select.py` to `select.py` or `select_builder.py`) to better reflect their new role as lean composer modules. The leading underscore might no longer be appropriate.
    * Example `merge.py` (formerly `_merge.py`):

        ```python
        from ._base import BaseBuilder
        from .mixins import (
            CommonTableExpressionMixin, # If applicable
            # Import other general mixins if used by MERGE
        )
        from .mixins._merge_clauses import (
            MergeUsingClauseMixin,
            MergeOnClauseMixin,
            MergeMatchedClauseMixin,
            MergeNotMatchedClauseMixin,
        )

        class MergeBuilder(
            MergeUsingClauseMixin,
            MergeOnClauseMixin,
            MergeMatchedClauseMixin,
            MergeNotMatchedClauseMixin,
            CommonTableExpressionMixin, # Example
            BaseBuilder,
        ):
            def __init__(self, dialect=None, expression=None):
                super().__init__(dialect=dialect, expression=expression)
            # Minimal, highly specific MergeBuilder logic remaining, if any.
        ```

* **`sqlspec/statement/builder/__init__.py`:**
    * This file is critical for maintaining the external API.
    * It must continue to export the main builder classes (e.g., `SelectBuilder`, `MergeBuilder`) under their established names. If the internal filenames for builder definitions are changed (e.g., `_merge.py` to `merge.py`), this `__init__.py` must be updated to import from the new filenames, ensuring no breaking change for users of the library.

* **File Removal/Renaming Summary:**
    * Original builder definition files (e.g., `_select.py`) *may be renamed* (e.g., to `select.py`) after their content is refactored. These files will likely persist to define the actual builder classes, even if very lean.
    * Any *other auxiliary files* that were part of the original builder structure and become genuinely obsolete or entirely empty after all their logic is moved to mixins or `BaseBuilder` *can be removed*.

---

### 3. Renaming/Trimming for Slimming Code

* **Method Movement:** This is the primary mechanism for slimming. Methods are moved from large builder classes into their respective, focused mixin classes.
* **Module Slimming & Renaming:** The original builder modules (e.g., `_select.py`) will become significantly smaller. This reduction in size and increased focus on class composition is a key benefit and can justify renaming these modules (e.g., removing the leading underscore).
* **Instance Variables:** Core attributes like `self._expression` and `self._dialect` should ideally be managed or provided by `BaseBuilder`. Mixins will operate on these attributes.
* **Helper/Private Methods:** These should be moved along with their public counterparts if they are exclusively used within a single mixin. If a helper is used by methods that end up in different mixins or remain in `BaseBuilder`, it must be placed appropriately (e.g., in `BaseBuilder`).
* **No Breaking Functional Changes (External API):** The public API of the builder classes (`SelectBuilder`, `MergeBuilder`, etc.)—meaning the methods available to users and their behavior—must remain identical. The refactoring is internal.
* **SQLFactory (`sqlspec/_sql.py`):** This class should not require changes, as it interacts with the public API of the builder classes, which will be preserved through `sqlspec/statement/builder/__init__.py`.

---

### 4. Implementation Steps (High-Level Workflow)

1. **Create `sqlspec/statement/builder/mixins/` directory and its `__init__.py` file.** Populate the `__init__.py` to export mixin classes as they are created.
2. **Iteratively Refactor:** Start with one logical group of functionality (e.g., `WHERE` clauses) to create the first mixin (e.g., `WhereClauseMixin`). For `MergeBuilder`, create `mixins/_merge_clauses.py` and define all four merge-related mixins (`MergeUsingClauseMixin`, `MergeOnClauseMixin`, `MergeMatchedClauseMixin`, `MergeNotMatchedClauseMixin`) within it.
    * Create the mixin file(s).
    * Define the mixin class(es).
    * Move the relevant methods (and any exclusively used helper methods) from the original builder classes into these mixins.
    * Update the original builder classes to inherit from the new mixins.
    * Add the mixins to `sqlspec/statement/builder/mixins/__init__.py` (importing from `_merge_clauses.py` for the merge mixins).
3. **Test Thoroughly:** After integrating each mixin (or group of mixins like for merge), run existing tests to confirm that functionality remains unchanged. Address any issues before proceeding.
4. **Repeat for all identified mixins.** Tackle `_select.py` methodically, as it is the largest.
5. **Address Shared State:** Ensure all mixins correctly access and modify shared state (like `self._expression`) provided by the base builder class.
6. **Review Method Resolution Order (MRO):** Be mindful of the MRO, especially if mixins begin to have their own `__init__` methods or override methods from `BaseBuilder` or other mixins. `super()` calls must be correct.
7. **Consider Renaming/Removing Files:**
    * Once a builder definition file (e.g., `_select.py`) has been significantly slimmed down, decide whether to rename it (e.g., to `select.py`).
    * If any auxiliary files become entirely redundant after their functionality is moved, consider their removal.
    * Ensure `sqlspec/statement/builder/__init__.py` is updated to reflect any renames or structural changes to maintain consistent external imports for builder classes.
8. **Final Review and Testing:** After all mixins are created and integrated, and renames/removals (if any) are done, conduct a full review and run all tests.

---

### 5. SQLFactory Function Review and TODOs (for `sqlspec/_sql.py`)

While the builder refactoring focuses on the structure of `SelectBuilder`, `InsertBuilder`, etc., the `SQLFactory` (`sql` object in `sqlspec/_sql.py`) also contains methods for generating SQL expression components. Some of these could be improved for better dialect compatibility and semantic accuracy by leveraging specific `sqlglot.exp` types instead of generic `exp.Anonymous` or potentially problematic function names.

**TODOs for `SQLFactory` (`sqlspec/_sql.py`):**

* **`sql.to_date(date_string, format_mask)`:**
    * **Review:** Currently uses `exp.Anonymous(this="TO_DATE")`.
    * **Action:** Investigate using `sqlglot.exp.StrToDate(this=date_expr, format=format_expr)` when `format_mask` is present. If no mask, consider `sqlglot.exp.Cast(this=date_expr, to=exp.DataType.build("DATE"))` for ISO-formatted strings, or `sqlglot.exp.TsOrDsToDate`. This will improve dialect transpilation as `TO_DATE` is not universal (e.g., MySQL uses `STR_TO_DATE`, SQL Server uses `CONVERT` or `CAST`).

* **`sql.to_char(column, format_mask)`:**
    * **Review:** Currently uses `exp.Anonymous(this="TO_CHAR")`.
    * **Action:** `TO_CHAR` is dialect-specific. Investigate using `sqlglot.exp.DateToStr` or `sqlglot.exp.FormatTime(this=col_expr, format=format_expr)` for date/time types. For other types, `CAST(column AS VARCHAR)` (i.e., `exp.Cast(this=col_expr, to=exp.DataType.build("VARCHAR"))`) is often more portable.

* **`sql.to_number(column, format_mask)`:**
    * **Review:** Currently uses `exp.Anonymous(this="TO_NUMBER")`.
    * **Action:** `TO_NUMBER` is dialect-specific. Investigate using `sqlglot.exp.StrToNum(this=col_expr, format=format_expr)` for better portability.

* **JSON Functions (`sql.to_json`, `sql.from_json`, `sql.json_extract`, `sql.json_value`):**
    * **Review:** Currently use `exp.Anonymous` for functions like `JSON_EXTRACT`, `TO_JSON`, etc.
    * **Action:** Refactor to use specific SQLGlot JSON expression types where available (e.g., `sqlglot.exp.JSONExtract`, `sqlglot.exp.JSONExtractScalar`, `sqlglot.exp.JSONFormat`, `sqlglot.exp.JSONParse`). This allows SQLGlot to handle dialect-specific syntax and path notations more effectively.

* **Window Functions (`sql.row_number`, `sql.rank`, `sql.dense_rank`):**
    * **Review:** The helper `_create_window_function` uses `exp.Anonymous(this=func_name, ...)` for the core window function (e.g., `ROW_NUMBER`).
    * **Action:** Modify to use specific SQLGlot types like `sqlglot.exp.RowNumber()`, `sqlglot.exp.Rank()`, `sqlglot.exp.DenseRank()` as the `this` argument when constructing the `exp.Window` object. This provides better semantic information to SQLGlot.

* **General Principle for `SQLFactory` functions:**
    * **Action:** Systematically review all functions in `SQLFactory` that produce `sqlglot.exp.Expression` objects. Prioritize using specific, existing `sqlglot.exp` subclasses over `exp.Anonymous` or direct string function names whenever SQLGlot provides a relevant typed expression. This enhances SQLGlot's ability to parse, transpile, and analyze the generated SQL accurately across different dialects.

---

### 6. SQLFactory Callable Enhancement (for `sqlspec/_sql.py`)

**Goal:** Make the `SQLFactory` (`sql` object) callable so users can write `sql("SELECT * FROM users WHERE id = :id", id=1)` and get back the appropriate builder (`SelectBuilder`, `InsertBuilder`, etc.) with intelligent type detection and validation.

#### A. Core Enhancement: `__call__` Method

**Implementation Requirements:**

* **Method Signature:** Match the driver execution patterns but adapted for builder creation. `schema_type` is removed, and return type is now `SelectBuilder`.

    ```python
    def __call__(
        self,
        statement: str,
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        config: "Optional[SQLConfig]" = None,
        dialect: "Optional[DialectType]" = None,
        **kwargs: Any,
    ) -> "SelectBuilder":
    ```

* **SQL Type Detection Strategy:**
    1. **Fast Regex Pre-filtering:** Use regex patterns to quickly identify the statement type (SELECT, INSERT, UPDATE, DELETE, MERGE, WITH), ignoring comments, hints, and whitespace.
    2. **AST Validation:** Use SQLGlot parsing to confirm the actual expression type.
    3. **Builder Dispatch (SELECT only):**
        * If the detected and parsed type is `SELECT` (or `WITH` followed by `SELECT`), instantiate and return a `SelectBuilder` with the parsed expression.
        * If any other type (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) is detected, raise a `SQLProgrammingError` (or a more specific custom exception) guiding the user to use `sql.insert()`, `sql.update()`, etc.

* **Regex Patterns for Detection:** (These remain useful for initial classification and error messaging)

    ```python
    SQL_TYPE_PATTERNS = {
        'SELECT': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*SELECT\b', re.IGNORECASE | re.DOTALL),
        'INSERT': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*INSERT\b', re.IGNORECASE | re.DOTALL),
        'UPDATE': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*UPDATE\b', re.IGNORECASE | re.DOTALL),
        'DELETE': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*DELETE\b', re.IGNORECASE | re.DOTALL),
        'MERGE': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*MERGE\b', re.IGNORECASE | re.DOTALL),
        'WITH': re.compile(r'^\s*(?:/\*.*?\*/\s*)*(?:--.*?\n\s*)*WITH\b', re.IGNORECASE | re.DOTALL),
    }
    ```

* **Builder Dispatch Logic:**

    ```python
    def __call__(self, statement: str, ...):
        # 1. Fast regex detection
        detected_type = self._detect_sql_type(statement)

        # 2. Parse with SQLGlot for validation and expression
        try:
            parsed_expr = sqlglot.parse_one(statement, read=dialect or self.dialect)
        except Exception as e:
            raise SQLValidationError(f"Failed to parse SQL: {e}")

        # 3. Validate detected type matches parsed type and handle dispatch
        actual_type_str = self._get_expression_type(parsed_expr)

        if detected_type.upper() != actual_type_str.upper():
            logger.warning(f"Regex detected {detected_type} but AST shows {actual_type_str}")
            # Potentially trust actual_type_str more, or require consistency

        if actual_type_str == 'SELECT' or (actual_type_str == 'WITH' and isinstance(parsed_expr, exp.Select)):
            builder = self._create_builder_for_type('SELECT', dialect)
            builder._expression = parsed_expr
        elif actual_type_str in ['INSERT', 'UPDATE', 'DELETE', 'MERGE']:
            raise SQLProgrammingError(
                f"Cannot create a {actual_type_str} statement using sql(...). "
                f"Please use sql.{actual_type_str.lower()}() instead."
            )
        else:
            raise SQLProgrammingError(
                f"Unsupported SQL statement type '{actual_type_str}' for sql(...). "
                "Only SELECT statements are supported via the callable interface."
            )

        # 4. Handle parameters if provided (for SelectBuilder)
        if parameters or kwargs:
            # Convert to SQL object first for parameter processing
            sql_obj = SQL(statement, parameters, *filters, dialect=dialect, config=config, **kwargs)
            # Transfer processed parameters to builder's internal SQL object
            builder._sql_object = sql_obj

        return builder
    ```

#### B. Type Safety and RowT Integration

**Challenge:** `RowT` is typically determined at the driver level. The `sql(...)` factory, now returning only `SelectBuilder`, will rely on the `as_schema()` method on the builder instance for type association.

* **Generic `SelectBuilder` Option (Advanced):**
    The `SelectBuilder` itself could become generic to carry the `RowT` type information.

    ```python
    # SQLFactory itself might not need to be generic if __call__ only returns SelectBuilder
    # class SQLFactory(Generic[RowT]): ...

    # SelectBuilder could be generic
    class SelectBuilder(Generic[RowT]):
        def as_schema(self, schema_type: type[RowT]) -> "SelectBuilder[RowT]":
            # Logic to store and utilize schema_type
            self._schema_type = schema_type
            return self # self is now conceptually SelectBuilder[RowT]
    ```

* **`as_schema` as the Standard Strategy:**
    Users will call `as_schema(UserModel)` on the `SelectBuilder` instance returned by `sql(...)` to associate it with a specific data model or schema.

    ```python
    # User provides schema_type via the .as_schema() method on the builder
    query: SelectBuilder[UserModel] = sql("SELECT * FROM users WHERE active = :active", active=True).as_schema(UserModel)
    # The .as_schema(UserModel) method on the builder associates UserModel
    # with this query, returning a builder typed (conceptually or explicitly) with UserModel.
    ```

#### C. Enhanced Validation for Existing Builder Methods

**Current Issue:** Methods like `sql.insert("SELECT ...")` don't validate that the SQL matches the expected statement type.

**Enhanced Validation Implementation:**

* **Pre-validation in Builder Factory Methods:**

    ```python
    def insert(self, table_or_sql: Optional[str] = None, dialect: Optional[DialectType] = None) -> "InsertBuilder":
        builder_dialect = dialect or self.dialect
        builder = InsertBuilder(dialect=builder_dialect)

        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                # NEW: Validate SQL type before proceeding
                detected_type = self._detect_sql_type(table_or_sql)
                if detected_type not in ['INSERT', 'SELECT']:  # SELECT allowed for INSERT FROM SELECT
                    raise SQLValidationError(
                        f"sql.insert() expects INSERT or SELECT statements, got {detected_type}. "
                        f"Use sql.{detected_type.lower()}() instead."
                    )
                return self._populate_insert_from_sql(builder, table_or_sql)
            return builder.into(table_or_sql)
        return builder
    ```

* **Similar validation for all builder methods:**
    * `sql.select()` - validate SELECT or WITH statements
    * `sql.update()` - validate UPDATE statements
    * `sql.delete()` - validate DELETE statements
    * `sql.merge()` - validate MERGE statements

#### D. Integration with Existing Architecture

**Maintains Compatibility:**

* Existing builder usage (`sql.select().from_()...`) remains unchanged
* New callable interface is additive
* All validation and pipeline logic is preserved
* Parameters flow through existing `SQL` object processing

**Builder Enhancement Requirements:**

* Builders need to handle pre-populated `_expression` from `__call__`
* Parameter integration needs to work with pre-parsed expressions
* `to_statement()` method should use the pre-populated expression if available
* Builders need to implement an `as_schema(self, schema_type: type) -> Self` method to associate a data schema/type with the query, potentially making the builder instance generic or storing the type for later use.

#### E. Implementation TODOs and Improvements

**Immediate TODOs for SQLFactory Callable:**

1. **Regex Pattern Refinement:**
    * **TODO:** Test regex patterns against real-world SQL with various comment styles, hints (e.g., `/*+ HINT */`), and edge cases
    * **TODO:** Add support for compound statements (e.g., `WITH ... SELECT`, `WITH ... INSERT`)
    * **TODO:** Handle CTEs that end with different statement types (WITH leading to INSERT/UPDATE/DELETE)

2. **Error Handling and User Experience:**
    * **TODO:** Provide helpful error messages when SQL type detection fails
    * **TODO:** Suggest correct method when user calls wrong builder method (e.g., `sql.insert("SELECT ...")` → "Did you mean sql.select()?"
    * **TODO:** Add warning/info logging for type mismatches between regex and AST detection

3. **Performance Optimization:**
    * **TODO:** Cache compiled regex patterns as class-level constants
    * **TODO:** Benchmark regex vs direct SQLGlot parsing for small SQL strings
    * **TODO:** Consider LRU cache for frequently parsed SQL patterns

4. **Type Safety Enhancement:**
    * **TODO:** Investigate making `SQLFactory` generic with `RowT` parameter
    * **TODO:** Add overloads for `__call__` method based on detected SQL type for better static type checking
    * **TODO:** Research if TypedDict or Literal types can improve schema_type integration
    * **TODO:** Implement `as_schema` method on all relevant builders.
    * **TODO:** Investigate making builder classes generic (e.g., `SelectBuilder[RowT]`) and how `as_schema` would affect this.

5. **Builder Integration:**
    * **TODO:** Ensure all builders can handle pre-populated expressions from `__call__`
    * **TODO:** Add `_sql_object` attribute to builders for parameter management
    * **TODO:** Modify `to_statement()` methods to prefer pre-populated expression over builder-constructed one
    * **TODO:** Ensure `as_schema` method correctly stores/utilizes the schema type information within the builder.

6. **Advanced SQL Support:**
    * **TODO:** Handle stored procedure calls (`CALL`, `EXEC`)
    * **TODO:** Support for PostgreSQL-specific syntax (`COPY`, `TRUNCATE`)
    * **TODO:** Handle database-specific extensions (e.g., MySQL's `REPLACE`, SQL Server's `BULK INSERT`)

**Broader Architecture TODOs:**

7. **Builder Mixin Integration:**
    * **TODO:** Ensure the callable interface works seamlessly with the planned mixin refactoring
    * **TODO:** Consider if any validation logic should be moved to mixins vs staying in factory

8. **Driver Integration:**
    * **TODO:** Test that builders created via `sql("...")` work identically to those created via `sql.select()` when passed to driver adapters
    * **TODO:** Ensure parameter binding works correctly for both creation paths

9. **Documentation and Examples:**
    * **TODO:** Create comprehensive examples showing both old and new syntax side by side
    * **TODO:** Document performance implications of different creation methods
    * **TODO:** Add type annotation examples for schema_type usage

10. **Testing Strategy:**
    * **TODO:** Unit tests for all SQL type detection edge cases
    * **TODO:** Integration tests with actual driver adapters
    * **TODO:** Performance benchmarks comparing creation methods
    * **TODO:** Type checking tests with mypy/pyright

**Implementation Priority:**

1. **Phase 1:** Basic `__call__` method with regex detection and simple builder dispatch
2. **Phase 2:** Enhanced validation for existing builder methods (`sql.insert()`, etc.)
3. **Phase 3:** Type safety improvements and schema_type integration
4. **Phase 4:** Performance optimizations and advanced SQL support

This enhancement would provide a significantly more intuitive API while maintaining all existing functionality and validation. The intelligent type detection combined with the validation improvements would catch many user errors early and provide a much smoother developer experience.
