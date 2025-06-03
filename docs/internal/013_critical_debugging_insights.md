## [REF-013] Critical Debugging Insights: SQL Class and Adapter Testing

**DECISION**: Document critical bugs discovered during SQLite adapter debugging and establish patterns to prevent similar issues across all adapters.

**IMPLEMENTATION**:

### Critical Bug Fixes Applied

#### 1. SQL Class Property Implementation Bug (CRITICAL)

- **Issue**: Both `is_many` and `is_script` properties were incorrectly returning `processed.input_had_placeholders` instead of actual instance flags
- **Impact**: Prevented proper execution path dispatch in drivers, causing all statements to follow single execution path
- **Fix**: Properties must return actual instance variables:

    ```python
    @property
    def is_many(self) -> bool:
        return self._is_many  # NOT processed.input_had_placeholders

    @property
    def is_script(self) -> bool:
        return self._is_script  # NOT processed.input_had_placeholders
    ```

- **Prevention**: All property implementations must be covered by unit tests

#### 2. Parameter Handling in as_many() Method

- **Issue**: Using `self.parameters` triggered validation before no-validation config took effect
- **Fix**: Use `self._raw_parameters` to avoid validation pipeline
- **Pattern**: For operations that need to bypass validation, use raw parameters not processed ones

#### 3. Script Parameter Substitution

- **Issue**: `as_script()` method wasn't preserving transformed expressions and extracted parameters
- **Fix**: Must call `_ensure_processed()` to preserve pipeline state and pass it to new instance
- **Requirement**: Scripts must convert all parameters to literals using `ParameterStyle.STATIC`

#### 4. Multi-Statement Script Parsing

- **Issue**: Using `sqlglot.parse_one()` only returned first statement of multi-statement scripts
- **Fix**: Use `sqlglot.parse()` for scripts to get all statements
- **Implementation**: Auto-detect scripts by analyzing semicolon positions and remaining content

### Testing Infrastructure Fixes

#### 1. Mock Context Manager Support

- **Issue**: Using basic `Mock()` for cursor objects that need context manager protocol
- **Fix**: Use `MagicMock()` for any object that needs `__enter__`/`__exit__` support
- **Critical Pattern**:

    ```python
    mock_cursor = MagicMock()  # Not Mock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    ```

#### 2. Assertion Target Mismatch

- **Issue**: Tests asserting on locally created mocks instead of the actual mock used by the driver
- **Fix**: Assert on the actual cursor used: `mock_connection.cursor.return_value.__enter__.return_value`
- **Pattern**: Always trace the exact mock path the driver code uses

### Architecture Integration Insights

#### 1. SQL Object as Single Source of Truth

- **Principle**: SQL object must be authoritative for SQL string, parameters, and execution mode
- **Implementation**: No separate parameter passing to `_execute_statement`
- **Dispatch Logic**: Use `statement.is_many`, `statement.is_script`, `statement.to_sql()`, `statement.parameters`

#### 2. Execution Method Structure (Mandatory)

- **Required Methods** (in order):
    1. `_execute_statement` - Main dispatch
    2. `_execute` - Single execution
    3. `_execute_many` - Batch execution
    4. `_execute_script` - Script execution
- **SQLite Reference**: SQLite adapter serves as canonical reference implementation

#### 3. Parameter Style Handling

- **Regular Execution**: Use adapter-specific placeholder style
- **Script Execution**: Always use `ParameterStyle.STATIC` to convert parameters to literals
- **Reason**: Most database drivers don't support parameter binding in script execution

### Integration with Recent Refactors

#### 1. BUILDER_CLEANUP.md Integration

- **Mixin Architecture**: Decomposed large builder classes into focused mixins
- **Testing Impact**: Each mixin must be tested independently
- **Factory Pattern**: Unified access through `sql` object

#### 2. ADAPTER_EXECUTION_REFACTOR_PLAN.md Integration

- **Standardization**: All adapters must follow SQLite execution method structure
- **Consistency**: Changes to one adapter must be reflected in all others
- **Priority List**: 9 adapters identified for refactoring

#### 3. PARSE_VS_PARSE_ONE.md Integration

- **Script Parsing**: Use `sqlglot.parse()` not `sqlglot.parse_one()` for multi-statement scripts
- **Auto-Detection**: Semicolon analysis to identify scripts automatically
- **Expression Wrapping**: Scripts wrapped in `exp.Command(this="SCRIPT", expressions=statements)`

#### 4. SQL_REFACTOR.md Integration

- **Process Once**: `_ensure_processed()` implements single-pass processing through StatementPipeline
- **State Management**: `_ProcessedState` caches all pipeline artifacts
- **Invalidation**: Operations that modify SQL must set `_processed_state = None`

### Performance and Architectural Benefits

#### 1. Single-Pass Processing

- **Parse Once**: SQL parsed into sqlglot expression once
- **Transform Once**: All transformations applied in single pipeline pass
- **Validate Once**: Validation performed once and cached

#### 2. Proper State Management

- **Immutability**: SQL objects return new instances on modification
- **Caching**: Processed state cached until invalidated
- **Context Preservation**: Relevant processing context preserved during operations

#### 3. Testing Reliability

- **Mock Accuracy**: Tests use exact same mock objects as production code
- **Execution Path Coverage**: All three execution paths independently tested
- **Parameter Flow Verification**: End-to-end parameter handling tested

### Future Development Guidelines

#### 1. Property Implementation Validation

- Always verify properties return correct underlying state
- Never return processed state flags from instance property getters
- Cover all properties with unit tests

#### 2. Parameter Access Patterns

- Use `_raw_parameters` for validation-bypass operations
- Use `parameters` property for fully processed parameters
- Document when and why each is used

#### 3. Mock Setup Standards

- Use `MagicMock` for context manager objects
- Configure complete mock path used by production code
- Assert on actual mocks used, not locally created ones

#### 4. Script Execution Requirements

- Use `sqlglot.parse()` for multi-statement parsing
- Preserve transformed expressions in `as_script()`
- Convert parameters to literals for script execution

#### 5. Cross-Adapter Consistency

- SQLite adapter as reference implementation
- Apply execution structure to all adapters
- Test all adapters with same patterns

### Error Prevention Checklist

#### Before Implementing New Adapter

- [ ] Study SQLite adapter execution method structure
- [ ] Implement all four execution methods in correct order
- [ ] Use SQL object as single source of truth
- [ ] Set up proper mock fixtures with MagicMock
- [ ] Test all three execution paths independently
- [ ] Verify parameter flow end-to-end
- [ ] Test script execution with multiple statements

#### Before Modifying SQL Class

- [ ] Understand current pipeline state management
- [ ] Preserve single-pass processing principle
- [ ] Invalidate processed state when needed
- [ ] Use correct parameter access patterns
- [ ] Test property implementations thoroughly

#### Before Writing Tests

- [ ] Use MagicMock for context managers
- [ ] Trace exact mock path used by code
- [ ] Assert on correct mock objects
- [ ] Cover all execution modes
- [ ] Test parameter transformation

### Key Metrics and Validation

#### Test Success Metrics

- All 21 SQLite adapter unit tests passing
- Proper mock setup eliminating `AttributeError: __enter__`
- Correct cursor method calls (execute, executemany, executescript)
- Parameter validation working for all execution modes

#### Architecture Validation

- Single-pass processing through StatementPipeline
- Proper state management in SQL class
- Consistent execution patterns across adapters
- Builder integration with pipeline architecture

**USER BENEFIT**:

- Reliable adapter behavior across all databases
- Consistent testing patterns preventing regressions
- Clear debugging guidance for future issues
- Robust parameter handling and script execution

---
