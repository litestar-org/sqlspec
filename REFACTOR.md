# Architectural Refactor: Script Execution and Placeholder Styles

## 1. Problem Statement

The `execute_script` method, crucial for executing multi-statement SQL scripts, fails when a database driver (e.g., SQLite) requests a specific `placeholder_style` during the `to_sql()` conversion. The core issue lies in `sqlspec.statement.sql.SQL.to_sql()`, where the logic to transform SQL for a given `placeholder_style` inadvertently bypasses or conflicts with the special serialization needed for `exp.Command(this="SCRIPT")` expressions. This results in errors like `sqlite3.OperationalError: near "SCRIPT": syntax error` because the script is not rendered as a plain multi-statement SQL string before placeholder transformation attempts to process it.

**Current Behavior:**

- `SQL.to_expression()` correctly identifies and parses multi-statement scripts into an `exp.Command(this="SCRIPT", expressions=[...])` object.
- `SQL.to_sql()` contains logic to iterate through these expressions and generate a semicolon-separated SQL string for scripts.
- However, if `placeholder_style` is provided (e.g., `ParameterStyle.STATIC` by the SQLite adapter), the placeholder transformation logic is entered *before* the script serialization logic can properly render the multi-statement string, leading to the "SCRIPT" command itself being treated as SQL.

## 2. Architectural Considerations

### 2.1. `exp.Command(this="SCRIPT")` Approach

Using `sqlglot.exp.Command(this="SCRIPT", expressions=[...])` to represent a multi-statement script within a single `sqlglot.exp.Expression` object is a valid and minimally invasive approach. It allows the `sqlspec` processing pipeline (validation, transformation, analysis) to potentially access and process individual statements within the script if the pipeline components are designed to look inside `exp.Command` expressions with `this="SCRIPT"`.

This approach requires that methods consuming these expressions, particularly `to_sql()`, are explicitly aware of this custom "SCRIPT" command type and prioritize its specific serialization logic.

### 2.2. Project Principles

- **All SQL must go through validation pipeline:** This principle necessitates parsing all statements in a script (`sqlglot.parse()`) rather than just the first (`sqlglot.parse_one()`). The current script parsing logic adheres to this.
- **Driver Compatibility:** `sqlspec` aims to provide a consistent interface over various database drivers, which may have different placeholder requirements. The `to_sql(placeholder_style=...)` mechanism is key to this.

The conflict arises from the interaction of these two valid architectural choices.

## 3. Proposed Solution

The primary solution is to adjust the order of operations within the `SQL.to_sql()` method to ensure that script serialization always occurs first if the expression is a "SCRIPT" command, regardless of whether a `placeholder_style` is also requested.

### 3.1. Modification to `SQL.to_sql()`

**File:** `sqlspec/statement/sql.py`

**Logic:**

1. Check if `self.expression` is an `exp.Command` with `this="SCRIPT"`.
2. If it is, serialize it into a multi-statement SQL string. This string becomes the new "base" SQL to work with.
3. *Then*, if a `placeholder_style` is specified, apply the placeholder transformation (`_transform_sql_placeholders`) to this resulting multi-statement string (or, if more appropriate and feasible, to each sub-statement before joining, though transforming the combined string is simpler initially).
4. If it's not a "SCRIPT" command, proceed with the existing logic: if `placeholder_style` is given, transform; otherwise, use `expression.sql()`.

This ensures that "SCRIPT" commands are always turned into their proper multi-statement string form before any dialect-specific or placeholder-specific transformations are attempted on the overall structure.

## 4. Implementation and Validation Plan

### Step 1: Implement `SQL.to_sql()` Modification

- **Action:** Refactor `sqlspec/statement/sql.py` as described in section 3.1.
- **Verification:** Code review, static analysis.

### Step 2: Unit Test `SQL.to_sql()`

- **Action:** Create new unit tests specifically for `SQL.to_sql()`:
    - Test case 1: `SQL` object with `exp.Command(this="SCRIPT")`, `placeholder_style=None`. Assert correct multi-statement SQL output.
    - Test case 2: `SQL` object with `exp.Command(this="SCRIPT")`, `placeholder_style=ParameterStyle.STATIC`. Assert correct multi-statement SQL output (placeholders should ideally be handled per statement or on the final script string).
    - Test case 3: `SQL` object with `exp.Command(this="SCRIPT")`, `placeholder_style=ParameterStyle.QMARK`. Assert correct multi-statement SQL output.
    - Test case 4: Regular single-statement `SQL` object with various `placeholder_style` values. Assert correct behavior is maintained.
- **Verification:** All unit tests pass.

### Step 3: Verify Driver Integration (`test_sqlite_execute_script`)

- **Action:** Review and run the existing integration test: `tests/integration/test_adapters/test_sqlite/test_driver.py::test_sqlite_execute_script`.
- **Enhancements:**
    - Add explicit assertions for `SQLResult` attributes: `operation_type == "SCRIPT"`, `total_statements`, `successful_statements`, `errors`.
    - Confirm the database state reflects that all statements in the script were executed (e.g., correct number of rows inserted/updated).
- **Verification:** SQLite integration tests pass with enhanced assertions.

### Step 4: Broader Integration Testing (Conceptual)

- **Action (Future):** If other drivers are added that use `execute_script` and specify a `placeholder_style`, similar integration tests should be created for them.
- **Verification:** Relevant driver tests pass.

### Step 5: Edge Case Testing for Script Execution

- **Action:** Add new integration tests for `execute_script`:
    - Script with leading/trailing whitespace and comments.
    - Script containing only a single statement.
    - Empty string passed to `execute_script`.
    - Script where an intermediate statement causes an SQL error (verify error reporting).
- **Verification:** All new edge case tests pass.

### Step 6: Code Cleanup

- **Action:** Remove any commented-out or obsolete code in `sqlspec/statement/sql.py` related to previous attempts to fix this issue once the new solution is stable.
- **Verification:** Code review confirms no dead code remains.

## 5. Impact and Benefits

- **Correctness:** Fixes the critical bug preventing `execute_script` from working with drivers that specify `placeholder_style`.
- **Architectural Integrity:** Maintains the "SCRIPT" command approach while ensuring it coexists correctly with placeholder transformation.
- **Reliability:** Improves the robustness of script execution across different database adapters.
- **Adherence to Principles:** Ensures multi-statement scripts can still have their constituent parts (notionally) pass through `sqlspec`'s processing stages.

## 6. Future Considerations (Optional)

- **Granular Placeholder Transformation for Scripts:** For very complex scenarios, one might consider if `_transform_sql_placeholders` should be capable of operating on a list of `exp.Expression` (the sub-statements of a script) and then joining them. However, the current proposal of transforming the already-concatenated script string is simpler and likely sufficient.
- **Pipeline Awareness of Scripts:** Evaluate if pipeline components (validators, analyzers) should be made explicitly aware of `exp.Command(this="SCRIPT")` and iterate through its `expressions` if deeper analysis of individual script statements is required. Currently, they would see the `Command` object itself.

Here's the initial starting point for troubleshoointg, this can probably be cleaned up though:

```py
// ... existing code ...
    def to_sql(
        self,
        placeholder_style: "Optional[Union[str, ParameterStyle]]" = None,
        dialect: "Optional[DialectType]" = None,
        statement_separator: str = ";",
        include_statement_separator: bool = False,
    ) -> str:
        """Get SQL string with specified placeholder style.

        Args:
            placeholder_style: The target placeholder style.
                Can be a string ('qmark', 'named', 'pyformat_named', etc.) or ParameterStyle enum.
                If None, uses dialect-appropriate default or existing SQL if parsing disabled.
            statement_separator: The statement separator to use.
            include_statement_separator: Whether to include the statement separator.
            dialect: The SQL dialect to use for SQL generation.

        Returns:
            SQL string with placeholders in the requested style.

        Example:
            >>> stmt = SQLStatement(
            ...     "SELECT * FROM users WHERE id = ?", [123]
            ... )
            >>> stmt.get_sql()
            'SELECT * FROM users WHERE id = ?'
            >>> stmt.get_sql(placeholder_style="named")
            'SELECT * FROM users WHERE id = :param_0'
        """
        target_dialect = dialect if dialect is not None else self._dialect
        sql: str

        if not self._config.enable_parsing and self.expression is None:
            sql = str(self._sql)
            if include_statement_separator and not sql.rstrip().endswith(statement_separator):
                sql = sql.rstrip() + statement_separator
            return sql

        current_expression = self.expression

        if current_expression is not None:
            # Step 1: Prioritize rendering "SCRIPT" commands to a multi-statement string first.
            if (
                isinstance(current_expression, exp.Command)
                and hasattr(current_expression, "this")
                and str(current_expression.this) == "SCRIPT"
                and hasattr(current_expression, "expressions")
            ):
                script_parts = []
                for stmt_expr in current_expression.expressions:
                    if stmt_expr is not None:
                        script_parts.append(stmt_expr.sql(dialect=target_dialect))
                sql = ";\n".join(script_parts)
                if sql and not sql.rstrip().endswith(";"):  # Ensure trailing semicolon for scripts
                    sql += ";"
                # The 'sql' variable now holds the rendered multi-statement script string.
                # If placeholder_style is provided, it will be applied to this string.
                # If placeholder_style is None, this 'sql' string is the final result.
                print(f"[DEBUG] Generated script SQL (prioritized): {sql!r}")
                print(f"[DEBUG] Script parts count: {len(script_parts)}")

                # If a placeholder_style is specified, we now apply it to the generated script string.
                # This requires parsing the script string again to apply transformations.
                # Note: This path means _transform_sql_placeholders will parse the script string.
                if placeholder_style is not None:
                    # We need an expression to pass to _transform_sql_placeholders.
                    # Re-parse the generated script string to get a (potentially single) expression.
                    # This is a bit of a workaround; ideally, _transform_sql_placeholders
                    # could take a string directly if it's already rendered.
                    # For now, we'll parse it as a single statement, as placeholder transformation
                    # operates on the structure.
                    # If the script was complex, this might not be ideal, but for typical placeholder needs it should work.
                    try:
                        # Treat the entire script as one "statement" for placeholder transformation purposes
                        # This means placeholders inside the script will be transformed.
                        # We pass the already rendered script string as the expression to _transform_sql_placeholders
                        # by re-parsing it.
                        # We'll use the original 'current_expression' (the Command SCRIPT object)
                        # for transformation, and _transform_sql_placeholders will handle its structure.
                        # This relies on _transform_sql_placeholders being able to handle
                        # Command(this="SCRIPT") or its logic correctly applying to the already rendered `sql` string.

                        # The _transform_sql_placeholders method takes an expression.
                        # If we give it the Command SCRIPT expression, and it internally calls .sql()
                        # without placeholder style, it will get the script string.
                        # Then it calls _convert_placeholder_style on that string.
                        # This seems like the most direct way.

                        sql = self._transform_sql_placeholders(placeholder_style, current_expression, target_dialect)
                        print(f"[DEBUG] Script SQL after placeholder transformation: {sql!r}")

                # If placeholder_style was None, 'sql' is already correctly set from script rendering.

            # Step 2: Handle non-script expressions or scripts after placeholder transformation (if any)
            elif placeholder_style is not None:
                sql = self._transform_sql_placeholders(placeholder_style, current_expression, target_dialect)
                print(f"[DEBUG] Generated regular SQL with placeholder_style: {sql!r}")
            else:
                # Default: No specific placeholder style requested, and not a script handled above.
                sql = current_expression.sql(dialect=target_dialect)
                print(f"[DEBUG] Generated regular SQL (default): {sql!r}")
        else:
            sql = str(self._sql)
            print(f"[DEBUG] Using original SQL string as expression is None: {sql!r}")


        if include_statement_separator and not sql.rstrip().endswith(statement_separator):
            sql = sql.rstrip() + statement_separator

        return sql
// ... existing code ...
    def _transform_sql_placeholders(
        self,
        target_style: "Union[str, ParameterStyle]",
        expression_to_render: "exp.Expression",
        dialect: "Optional[DialectType]" = None,
    ) -> str:
        target_dialect = dialect if dialect is not None else self._dialect
        target_style_enum: ParameterStyle

        if isinstance(target_style, str):
            style_map = {
                "qmark": ParameterStyle.QMARK,
                "named": ParameterStyle.NAMED_COLON,
                "named_colon": ParameterStyle.NAMED_COLON,
                "named_at": ParameterStyle.NAMED_AT,
                "named_dollar": ParameterStyle.NAMED_DOLLAR,
                "numeric": ParameterStyle.NUMERIC,
                "pyformat_named": ParameterStyle.PYFORMAT_NAMED,
                "pyformat_positional": ParameterStyle.PYFORMAT_POSITIONAL,
                "static": ParameterStyle.STATIC,
            }
            try:
                target_style_enum = style_map[target_style.lower()]
            except KeyError:
                logger.warning("Unknown placeholder_style '%s', defaulting to qmark.", target_style)
                target_style_enum = ParameterStyle.QMARK
        elif isinstance(target_style, ParameterStyle):
            target_style_enum = target_style
        else:
            # Should not happen with proper typing, but as a fallback:
            logger.error(f"Invalid target_style type: {type(target_style)}. Defaulting to qmark.")
            target_style_enum = ParameterStyle.QMARK


        if target_style_enum == ParameterStyle.STATIC:
            # For static rendering, we need to handle scripts by rendering sub-expressions
            if isinstance(expression_to_render, exp.Command) and str(getattr(expression_to_render, 'this', '')) == "SCRIPT":
                script_parts = []
                for stmt_expr in getattr(expression_to_render, 'expressions', []):
                    if stmt_expr is not None:
                        # Recursively call _render_static_sql for each sub-statement
                        # This ensures parameters within each script part are substituted
                        script_parts.append(self._render_static_sql(stmt_expr))
                rendered_script = ";\n".join(script_parts)
                if rendered_script and not rendered_script.rstrip().endswith(";"):
                     rendered_script += ";"
                return rendered_script
            return self._render_static_sql(expression_to_render)

        # For other placeholder styles, first get the SQL string of the expression.
        # If it's a SCRIPT command, this will correctly render the multi-statement string.
        # The _convert_placeholder_style method will then operate on this rendered string.
        current_sql_str: str
        if (
            isinstance(expression_to_render, exp.Command)
            and hasattr(expression_to_render, "this")
            and str(expression_to_render.this) == "SCRIPT"
            and hasattr(expression_to_render, "expressions")
        ):
            script_parts = []
            for stmt_expr in expression_to_render.expressions:
                if stmt_expr is not None:
                    script_parts.append(stmt_expr.sql(dialect=target_dialect))
            current_sql_str = ";\n".join(script_parts)
            if current_sql_str and not current_sql_str.rstrip().endswith(";"):
                current_sql_str += ";"
        else:
            current_sql_str = expression_to_render.sql(dialect=target_dialect)

        return self._convert_placeholder_style(current_sql_str, target_style_enum)

    def _convert_placeholder_style(self, sql: str, target_style: "ParameterStyle") -> str:
// ... existing code ...
```
