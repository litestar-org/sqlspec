# SQL Processing Consistency Implementation Plan

## Overview

This document outlines the implementation plan for fixing SQL processing consistency issues in SQLSpec. The core issues are:

1. **Parameter handling inconsistency** between execute and execute_many methods
2. **Security validation disabled** for scripts
3. **Analyzer never instantiated** despite being implemented  
4. **Incorrect terminology** using "normalization" for parameter style conversion

## Phase 1: Enable Analyzer (Low Risk - 1 Day)

### Changes Required

**File: `sqlspec/statement/sql.py`**

Fix the bug where analyzer is set to empty list (lines 174-176):

```python
# Current (BUG):
elif self.enable_analysis:
    analyzers = []

# Fixed:
elif self.enable_analysis:
    from sqlspec.statement.pipelines.analyzers import StatementAnalyzer
    analyzers = [StatementAnalyzer()]
```

Add analyzer output configuration to SQLConfig class (~line 106):

```python
# Add to SQLConfig dataclass
analyzer_output_handler: Optional[Callable[[StatementAnalysis], None]] = None

# Add after SQLConfig class definition
def default_analysis_handler(analysis: StatementAnalysis) -> None:
    """Default handler that logs analysis to debug."""
    logger.debug("SQL Analysis: %s", analysis)
```

### Tests Required

```python
def test_analyzer_stats_collection():
    """Test that analyzer collects statistics when enabled."""
    captured = []
    
    def capture_stats(analysis):
        captured.append(analysis)
    
    config = SQLConfig(
        enable_analysis=True,
        analyzer_output_handler=capture_stats
    )
    
    driver.execute("SELECT * FROM users WHERE id = 1", _config=config)
    
    assert len(captured) == 1
    assert captured[0].statement_type == "Select"
    assert captured[0].complexity_score > 0
    assert captured[0].tables == ["users"]
```

## Phase 2: Unify Execute_many Parameters (Medium Risk - 2-3 Days)

### Changes Required

**File: `sqlspec/driver/_sync.py`**

Replace execute_many method (lines 199-215):

```python
def execute_many(
    self,
    statement: "Union[Statement, QueryBuilder[Any]]",
    /,
    param_sequence: "Optional[list[Any]]" = None,
    *filters: "StatementFilter",
    _connection: "Optional[ConnectionT]" = None,
    _config: "Optional[SQLConfig]" = None,
    **kwargs: Any,
) -> "SQLResult[RowT]":
    """Execute statement multiple times with different parameters.
    
    Now passes first parameter set through pipeline to enable
    literal extraction and consistent parameter processing.
    """
    # Process first parameter set through pipeline for literal extraction
    first_params = param_sequence[0] if param_sequence else None
    
    # Build statement with first params to trigger pipeline processing
    sql_statement = self._build_statement(
        statement, first_params, *filters, _config=_config or self.config, **kwargs
    )
    
    # Mark as many with full sequence
    sql_statement = sql_statement.as_many(param_sequence)
    
    return self._execute_statement(
        statement=sql_statement, 
        connection=self._connection(_connection), 
        **kwargs
    )
```

**File: `sqlspec/driver/_async.py`**

Apply same changes to async version of execute_many.

**File: `sqlspec/statement/sql.py`**

Simplify `_compile_execute_many` method since parameters now go through pipeline:

```python
def _compile_execute_many(self, placeholder_style: "Optional[str]") -> "tuple[str, Any]":
    """Compile for execute_many operations.
    
    Parameters already include extracted literals from pipeline processing,
    so no manual merging is required.
    """
    sql = self.sql
    self._ensure_processed()
    
    # Use merged parameters from pipeline processing
    params = self._processed_state.merged_parameters if self._processed_state else self._original_parameters
    
    if placeholder_style:
        sql, params = self._convert_placeholder_style(sql, params, placeholder_style)
    
    return sql, params
```

### Tests Required

```python
async def test_execute_many_with_literals():
    """Test execute_many with SQL containing literals to be parameterized."""
    # SQL with embedded literal that should be parameterized
    sql = "INSERT INTO users (name, status) VALUES ($1, 'active')"
    params = [("Alice",), ("Bob",), ("Charlie",)]
    
    result = await driver.execute_many(sql, params)
    
    # The literal 'active' should be extracted and added to each parameter set
    # Resulting in: VALUES ($1, $2) with [("Alice", "active"), ("Bob", "active"), ...]
    assert result.rows_affected == 3
    
    # Verify data
    users = await driver.execute("SELECT name, status FROM users")
    assert all(user["status"] == "active" for user in users)

def test_execute_many_edge_cases():
    """Test execute_many with edge cases."""
    # Empty parameter list
    result = driver.execute_many("INSERT INTO test (col) VALUES ($1)", [])
    assert result.rows_affected == 0
    
    # Single parameter set
    result = driver.execute_many("INSERT INTO test (col) VALUES ($1)", [("value",)])
    assert result.rows_affected == 1
```

## Phase 3: Script Security Enhancement (Medium Risk - 3-4 Days)

### Changes Required

**File: `sqlspec/driver/_sync.py`**

Update execute_script to maintain validation by default (lines 217-232):

```python
def execute_script(
    self,
    statement: "Union[str, SQL]",
    /,
    *parameters: "Union[StatementParameters, StatementFilter]",
    _connection: "Optional[ConnectionT]" = None,
    _config: "Optional[SQLConfig]" = None,
    _suppress_warnings: bool = False,  # New parameter for migrations
    **kwargs: Any,
) -> "SQLResult[RowT]":
    """Execute a multi-statement script.
    
    By default, validates each statement and logs warnings for dangerous
    operations. Use _suppress_warnings=True for migrations and admin scripts.
    """
    script_config = _config or self.config
    
    # Keep validation enabled by default
    # Validators will log warnings for dangerous operations
    
    sql_statement = self._build_statement(
        statement, *parameters, _config=script_config, **kwargs
    ).as_script()
    
    # Pass suppress warnings flag to execution
    if _suppress_warnings:
        kwargs["_suppress_warnings"] = True
    
    return self._execute_statement(
        statement=sql_statement, 
        connection=self._connection(_connection), 
        **kwargs
    )
```

**Update all adapter _execute_script methods to use splitter**

Example for SQLite adapter:

```python
from sqlspec.statement.splitter import split_sql_script
def _execute_script(
    self, script: str, connection: Optional[SqliteConnection] = None, 
    statement: Optional[SQL] = None, **kwargs: Any
) -> SQLResult[RowT]:
    """Execute script using splitter for per-statement validation."""
    
    
    conn = connection or self._connection(None)
    statements = split_sql_script(script, dialect="sqlite")
    
    total_rows = 0
    successful = 0
    suppress_warnings = kwargs.get("_suppress_warnings", False)
    
    with self._get_cursor(conn) as cursor:
        for stmt in statements:
            try:
                # Validate each statement unless warnings suppressed
                if not suppress_warnings and statement:
                    # Run validation through pipeline
                    temp_sql = SQL(stmt, config=statement._config)
                    temp_sql._ensure_processed()
                    # Validation errors are logged as warnings by default
                
                cursor.execute(stmt)
                successful += 1
                total_rows += cursor.rowcount or 0
            except Exception as e:
                if not kwargs.get("continue_on_error", False):
                    raise
                logger.warning("Script statement failed: %s", e)
    
    conn.commit()
    
    return SQLResult(
        statement=statement or SQL(script).as_script(),
        data=[],
        rows_affected=total_rows,
        operation_type="SCRIPT",
        total_statements=len(statements),
        successful_statements=successful,
    )
```

### Tests Required

```python
def test_script_security_validation():
    """Test script validation behavior."""
    unsafe_script = "DROP TABLE users; SELECT * FROM accounts;"
    
    # By default, dangerous operations log warnings but don't fail
    with capture_logs(level=logging.WARNING) as logs:
        result = driver.execute_script(unsafe_script)
        assert result.operation_type == "SCRIPT"
        # Should see warning about DDL operation
        assert any("DDL operation 'DROP' is not allowed" in log.message for log in logs)
    
    # Can suppress warnings for migrations
    with capture_logs(level=logging.WARNING) as logs:
        result = driver.execute_script(unsafe_script, _suppress_warnings=True)
        assert result.operation_type == "SCRIPT"
        # No warnings logged
        assert not any("DDL operation" in log.message for log in logs)

def test_script_sql_injection_protection():
    """Test that SQL injection patterns are detected."""
    # This would need to be implemented in SecurityValidator
    injection_script = "SELECT * FROM users WHERE id = 1 OR '1'='1'; DROP TABLE users; --"
    
    # SQL injection patterns should be detected and logged
    with capture_logs(level=logging.WARNING) as logs:
        result = driver.execute_script(injection_script)
        assert any("SQL injection" in log.message for log in logs)
```

## Phase 4: Terminology Standardization (Low Risk - 2 Days)

### Global Replacements

Perform these replacements across ~24 files:

1. `normalize_parameter` → `convert_parameter_style`
2. `normalization` → `parameter_style_conversion`
3. `default_parameter_style` → `default_parameter_style`

### Key Files to Update

- `sqlspec/statement/parameters.py`
- `sqlspec/driver/parameters.py`
- `sqlspec/driver/_common.py`
- All adapter driver files
- Related test files

### Example Changes

```python
# Before
def normalize_parameters_to_style(params, style):
    """Normalize parameters to target style."""
    pass

default_parameter_style = "qmark"

# After  
def convert_parameters_to_style(params, style):
    """Convert parameters to specified style."""
    pass

default_parameter_style = "qmark"
```

## Migration Timeline

- **Week 1**: Phase 1 (Analyzer) - Immediate value, low risk
- **Week 2**: Phase 2 (Execute_many) - Thorough testing with all adapters
- **Week 3-4**: Phase 3 (Script Security) - Gradual rollout, test with real scripts
- **Week 5**: Phase 4 (Terminology) - Mechanical changes, update docs

## Key Design Decisions

### Security Validation Behavior

Based on the requirement "as secure as possible but 100% usable by default":

1. **Warnings by Default**: Dangerous operations (DROP TABLE, DELETE without WHERE) log warnings but don't fail
2. **True Security Issues**: SQL injection patterns could optionally fail (future enhancement)
3. **Opt-out Available**: `_suppress_warnings=True` for migrations and admin scripts
4. **Per-Statement Validation**: Scripts are split and each statement validated

This approach balances security awareness with practical usability.

### Parameter Processing

1. **Unified Flow**: All execution methods pass parameters through `_build_statement`
2. **Pipeline Benefits**: Automatic literal extraction, consistent merging
3. **Edge Case Handling**: Empty lists and single-item lists work correctly
4. **Performance**: Minimal overhead compared to manual merging complexity

## Success Metrics

- Zero parameter count mismatch errors in execute_many
- Security validators active by default for scripts (as warnings)
- Analyzer provides actionable statistics  
- All tests pass across 10 adapters
- No breaking changes to public API
- Improved developer experience with clearer terminology

## Risk Mitigation

1. **Backward Compatibility**: Optional parameters preserve existing behavior
2. **Gradual Rollout**: Phased approach allows testing at each stage
3. **Warning-First**: Security starts with warnings, not hard failures
4. **Comprehensive Testing**: New tests cover all edge cases
5. **Documentation**: Update all docstrings and examples
