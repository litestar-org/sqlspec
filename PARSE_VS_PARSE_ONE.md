# Script Execution Issue: `parse()` vs `parse_one()` Debug Summary

## Problem Statement

The `execute_script` method in both SQLite and aiosqlite drivers was only executing the first statement from multi-statement scripts, instead of executing all statements as expected.

**Test Case**:

```sql
INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
```

**Expected Result**: 2 rows (`script_test1` and `script_test2`)
**Actual Result**: 1 row (only `script_test1`)

## Root Cause Analysis

### The Core Issue: `parse_one()` vs `parse()`

The fundamental problem was in SQLGlot parsing behavior:

- **`sqlglot.parse_one()`**: Returns only the first parsed statement
- **`sqlglot.parse()`**: Returns a list of all parsed statements

**Verification**:

```python
script = "INSERT INTO test_table (name, value) VALUES ('script_test1', 999); INSERT INTO test_table (name, value) VALUES ('script_test2', 888); UPDATE test_table SET value = 1000 WHERE name = 'script_test1';"

# Using parse_one - only returns first statement
result = sqlglot.parse_one(script, read='sqlite')
# Result: INSERT INTO test_table (name, value) VALUES ("script_test1", 999)

# Using parse - returns all statements
results = sqlglot.parse(script, read='sqlite')
# Count: 3 statements
```

### Problem Location

The issue was in `sqlspec/statement/sql.py` in the `to_expression()` method:

```python
# Line 525 - Always used parse_one regardless of script context
return sqlglot.parse_one(sql_str, read=dialect)
```

## Attempted Solutions

### 1. Driver-Level Override (❌ Rejected)

**Approach**: Override `execute_script` in each driver to handle raw script strings directly.

**Why Rejected**:

- Violated DRY principles - would need to implement in every driver
- Code duplication and maintenance burden
- Not scalable architecture

### 2. Base Class Raw Script Methods (❌ Rejected)

**Approach**: Add protected methods `_execute_raw_script` and `_execute_raw_script_async` to base classes.

**Why Rejected**:

- Bypassed all SQL validation, parsing, and transformation
- Violated project rules requiring SQL validation
- Created unsafe execution path

### 3. Parsing Layer Fix (✅ Current Approach)

**Approach**: Fix the root cause in the SQL parsing logic to properly handle multi-statement scripts.

## Current Implementation

### Auto-Detection Logic

Added auto-detection of multi-statement scripts in `to_expression()`:

```python
# Auto-detect scripts by checking for multiple statements (semicolons)
auto_detect_script = False
if not is_script and isinstance(sql_str, str):
    clean_sql = sql_str.strip()
    semicolon_positions = [i for i, c in enumerate(clean_sql) if c == ';']
    if semicolon_positions:
        # Check if there's meaningful content after any semicolon
        for pos in semicolon_positions[:-1]:
            remaining = clean_sql[pos+1:].strip()
            if remaining:  # There's content after this semicolon
                auto_detect_script = True
                break
```

### Script Parsing Implementation

```python
if is_script or auto_detect_script:
    # For scripts, use parse to get all statements
    parsed_statements = sqlglot.parse(sql_str, read=dialect)
    if len(parsed_statements) > 1:
        # Create a compound expression to represent the script
        valid_statements = [stmt for stmt in parsed_statements if stmt is not None]
        return exp.Command(this="SCRIPT", expressions=valid_statements)
```

### Script SQL Generation

Added special handling in `to_sql()` method:

```python
# Special handling for script expressions
if (isinstance(self.expression, exp.Command) and
    hasattr(self.expression, 'this') and
    str(self.expression.this) == "SCRIPT" and
    hasattr(self.expression, 'expressions')):
    # Convert each statement individually and join with semicolons
    script_parts = []
    for stmt in self.expression.expressions:
        if stmt is not None:
            script_parts.append(stmt.sql(dialect=target_dialect))
    sql = ";\n".join(script_parts)
    if sql and not sql.rstrip().endswith(";"):
        sql += ";"
```

## Current Status

### ✅ Working

- **Auto-detection**: Scripts are correctly identified by semicolon analysis
- **Multi-statement parsing**: All 3 statements are parsed using `sqlglot.parse()`
- **Command expression creation**: Scripts are wrapped in `exp.Command` with `this="SCRIPT"`

**Debug Evidence**:

```
[DEBUG] Auto-detected script due to content after semicolon
[DEBUG] Parsed 3 statements
[DEBUG] Creating Command with 3 statements
```

### ❌ Current Issue

**Problem**: `to_sql()` method not handling script expressions when placeholder styles are specified.

**Error**: `sqlite3.OperationalError: near "SCRIPT": syntax error`

**Root Cause**: When `placeholder_style=ParameterStyle.STATIC` is specified (as in SQLite driver line 80), the script conversion logic is bypassed and goes to `_transform_sql_placeholders()` instead.

**Code Location**:

```python
# sqlspec/adapters/sqlite/driver.py:80
statement.to_sql(placeholder_style=ParameterStyle.STATIC)
```

## Next Steps

### Immediate Fix Required

Update the `to_sql()` method to handle script expressions even when `placeholder_style` is specified:

```python
def to_sql(self, placeholder_style=None, ...):
    if self.expression is not None:
        # Check for script expressions first, regardless of placeholder_style
        if (isinstance(self.expression, exp.Command) and
            str(self.expression.this) == "SCRIPT"):
            # Handle script conversion
            return convert_script_to_sql()

        # Then handle regular placeholder style conversion
        if placeholder_style is None:
            return self.expression.sql(dialect=target_dialect)
        else:
            return self._transform_sql_placeholders(...)
```

### Testing Strategy

1. **Unit Tests**: Verify `sqlglot.parse()` vs `sqlglot.parse_one()` behavior
2. **Integration Tests**: Test both SQLite and aiosqlite drivers
3. **Edge Cases**: Single statements, empty scripts, syntax errors
4. **Parameter Validation**: Ensure all SQL validation still works for scripts

### Architecture Validation

This approach maintains:

- ✅ **DRY Principles**: Single implementation in base SQL class
- ✅ **Validation Requirements**: All SQL goes through validation pipeline
- ✅ **Driver Compatibility**: Works with all existing drivers
- ✅ **Backwards Compatibility**: Single statements continue working as before

## Debug Flow Analysis

### Execution Flow

1. **`execute_script(script)`** called with multi-statement script
2. **SQL object creation**: `SQL(script).as_script()`
3. **`to_expression()` called**: `is_script=False` initially (auto-detection needed)
4. **Auto-detection**: Semicolon analysis identifies script
5. **`sqlglot.parse()`**: Parses all 3 statements correctly
6. **Command creation**: Wraps statements in `exp.Command`
7. **`to_sql()` called**: With `placeholder_style=ParameterStyle.STATIC`
8. **❌ Issue**: Script conversion bypassed due to placeholder_style logic

### Debug Output Sequence

```
[DEBUG] to_expression called: is_script=False, statement_type=<class 'str'>
[DEBUG] Auto-detected script due to content after semicolon
[DEBUG] Parsing as script: "\n        INSERT INTO test_table..."
[DEBUG] Parsed 3 statements
[DEBUG] Creating Command with 3 statements
[DEBUG] Expression type: <class 'sqlglot.expressions.Command'>, is_script: True
```

## Files Modified

- `sqlspec/statement/sql.py`: Core parsing and SQL generation logic
- `tests/integration/test_adapters/test_sqlite/test_driver.py`: Enhanced error checking
- `tests/integration/test_adapters/test_aiosqlite/test_driver.py`: Updated test expectations

## Test Results

**Before Fix**: ❌ 1 row inserted (only first statement)
**After Auto-detection**: ✅ Scripts detected and parsed correctly
**Current**: ❌ SQL generation issue with placeholder styles

**Target**: ✅ 2 rows inserted (all statements executed)

## Key Learnings

1. **SQLGlot Behavior**: Understanding the difference between `parse()` and `parse_one()` is crucial for multi-statement handling
2. **Auto-detection**: Simple semicolon analysis can reliably identify multi-statement scripts
3. **Placeholder Style Impact**: The `to_sql()` method needs to prioritize script handling over placeholder style conversion
4. **Architecture Decision**: Fixing at the parsing layer maintains all existing validation and security features
5. **Debug Strategy**: Comprehensive logging at each step revealed the exact point of failure

## References

- [SQLGlot Documentation](https://sqlglot.com/sqlglot.html)
- [SQLite executescript() Documentation](https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executescript)
- Project Rules: All SQL must go through validation pipeline
