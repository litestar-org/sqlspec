# SQLSpec Testing Strategy & Rules

## Overview

This document outlines the comprehensive testing strategy for sqlspec, aimed at achieving near 100% test coverage while maintaining high code quality and reliability.

## Testing Philosophy

1. **Pyramid Approach**: More unit tests, fewer integration tests, minimal end-to-end tests
2. **Behavior-Driven**: Test behaviors and contracts, not implementation details
3. **Parameterized Testing**: Use `pytest.mark.parametrize` extensively for testing multiple scenarios
4. **Isolation**: Each test should be independent and able to run in any order
5. **Fast Feedback**: Unit tests should run quickly (< 1ms per test when possible)
6. **Function-Based**: Prefer simple test functions over test classes for better readability and simplicity

## Test Categories

### 1. Unit Tests (`tests/unit/`)

- **Purpose**: Test individual functions, classes, and methods in isolation
- **Scope**: Single module or class
- **Dependencies**: Mock external dependencies
- **Coverage Target**: 95%+

### 2. Integration Tests (`tests/integration/`)

- **Purpose**: Test interactions between components and with real databases
- **Scope**: Multiple modules working together
- **Dependencies**: Real database connections (via pytest-databases)
- **Coverage Target**: 80%+

### 3. End-to-End Tests (Future)

- **Purpose**: Test complete workflows from user perspective
- **Scope**: Full application scenarios
- **Dependencies**: Complete environment setup

## Directory Structure

```
tests/
├── conftest.py              # Global pytest configuration
├── fixtures/                # Shared test fixtures and data
├── unit/                    # Unit tests mirroring src structure
│   ├── test_utils/          # Utils module tests
│   ├── test_statement/      # Statement building tests
│   ├── test_adapters/       # Adapter tests (mocked)
│   └── test_extensions/     # Extension tests
└── integration/             # Integration tests
    ├── test_adapters/       # Real database tests
    └── test_workflows/      # End-to-end workflows
```

## Naming Conventions

### Test Files

- Unit tests: `test_{module_name}.py`
- Integration tests: `test_{feature}_integration.py`
- Mirror source structure: `sqlspec/utils/text.py` → `tests/unit/test_utils/test_text.py`

### Test Functions

- Descriptive names: `test_snake_case_converts_camel_case_to_snake_case`
- Test functions: `test_{function_name}_{scenario}_{expected_outcome}`
- Parameterized tests: `test_{function}_with_{parameter_description}`
- Group related tests with module-level organization and descriptive function names

### Test Organization

- **No Test Classes**: Use functions only, group related tests by module and descriptive naming
- Use `test_{module_name}_{function_name}_{scenario}` for complex scenarios
- Leverage pytest fixtures for setup/teardown instead of class-based setUp/tearDown

## Test Patterns

### 1. Parameterized Tests

```python
@pytest.mark.parametrize(
    ("input_value", "expected", "description"),
    [
        ("CamelCase", "camel_case", "basic camel case"),
        ("HTTPRequest", "http_request", "consecutive capitals"),
        ("", "", "empty string"),
    ],
    ids=["basic_camel_case", "consecutive_capitals", "empty_string"]
)
def test_snake_case(input_value, expected, description):
    assert snake_case(input_value) == expected
```

### 2. Fixture Usage

```python
@pytest.fixture
def mock_driver():
    driver = Mock()
    driver.dialect = "postgresql"
    return driver

def test_instrument_operation_with_mock_driver(mock_driver):
    # Test implementation
```

### 3. Exception Testing

```python
def test_function_raises_specific_exception():
    with pytest.raises(SpecificException, match="Expected error message"):
        function_that_should_fail()
```

### 4. Mock Usage

```python
@patch('sqlspec.utils.module_loader.importlib')
def test_load_module_with_mock(mock_importlib):
    # Configure mock
    mock_importlib.import_module.return_value = Mock()
    # Test implementation
```

### 5. Related Test Grouping

```python
# Group related tests with descriptive function names
def test_singleton_basic_instance_creation():
    # Test basic singleton behavior

def test_singleton_ignores_constructor_args_after_first_call():
    # Test argument handling

def test_singleton_different_classes_different_instances():
    # Test class separation

# Use fixtures for shared setup
@pytest.fixture
def singleton_test_class():
    class TestClass(metaclass=SingletonMeta):
        def __init__(self, value=42):
            self.value = value
    return TestClass
```

## Coverage Requirements

### Unit Tests

- **Target**: 95%+ line coverage
- **Mandatory**: All public APIs must be tested
- **Exception**: Only skip trivial property getters/setters
- **Focus Areas**:
    - Happy path scenarios
    - Edge cases and boundary conditions
    - Error conditions and exception handling
    - Type validation and conversion

### Integration Tests

- **Target**: 80%+ line coverage
- **Focus**: Real database interactions
- **Scenarios**: Multiple SQL dialects where applicable

## Test Organization by Module

### Utils Module (`tests/unit/test_utils/`)

1. **test_deprecation.py**
   - `test_warn_deprecation_*` functions for all parameters and scenarios
   - `test_deprecated_decorator_*` functions for decorator behavior
   - Warning message formatting validation
   - PendingDeprecationWarning vs DeprecationWarning testing

2. **test_singleton.py**
   - `test_singleton_*` functions for various singleton scenarios
   - Instance creation, argument handling, inheritance
   - Thread safety testing
   - Multiple inheritance scenarios

3. **test_telemetry.py**
   - `test_instrument_operation_*` functions for sync/async context managers
   - Logging behavior validation
   - Metrics collection testing
   - Error handling and exception scenarios
   - OpenTelemetry integration testing

4. **test_fixtures.py**
   - `test_open_fixture_*` functions for sync/async file loading
   - JSON parsing validation
   - FileNotFoundError handling
   - Path handling scenarios (pathlib vs anyio)

5. **test_text.py** (enhance existing)
   - Add more edge cases with descriptive function names
   - Unicode handling tests
   - Performance edge case validation

6. **test_sync_tools.py** (enhance existing)
   - Add async/await scenario functions
   - Error propagation testing
   - Performance validation

7. **test_module_loader.py** (enhance existing)
   - Import error handling functions
   - Module reloading scenarios
   - Circular dependency testing

### Statement Module (`tests/unit/test_statement/`)

- SQL generation across dialects
- Query building patterns
- Parameter binding
- Validation logic

### Adapters Module (`tests/unit/test_adapters/`)

- Mock all database connections
- Test adapter interfaces
- Configuration validation
- Error handling

## Pytest Configuration

### Markers

```python
# Database-specific tests
@pytest.mark.postgres
@pytest.mark.mysql
@pytest.mark.sqlite
@pytest.mark.bigquery

# Driver-specific tests
@pytest.mark.asyncpg
@pytest.mark.psycopg
@pytest.mark.aiomysql

# Test types
@pytest.mark.unit
@pytest.mark.integration
@pytest.mark.slow
```

### Fixtures Strategy

- **Scope appropriately**: `session` for expensive setup, `function` for isolation
- **Use autouse sparingly**: Only for essential setup
- **Parameterize fixtures**: Test multiple configurations
- **Clean up**: Ensure proper teardown

## Tools and Quality Checks

### Code Coverage

- Use `pytest-cov` for coverage reporting
- Minimum thresholds enforced in CI
- Coverage reports in HTML format for local development

### Test Performance

- Monitor test execution time
- Flag slow tests with `@pytest.mark.slow`
- Optimize or parallelize long-running tests

### Database Testing

- Use `pytest-databases` for real database testing
- Docker containers for isolation
- Test data cleanup between tests

## Implementation Priority

### Phase 1: Utils Module (Current)

1. Complete test coverage for all utils modules
2. Establish testing patterns and conventions
3. Set up coverage reporting

### Phase 2: Statement Module

1. SQL generation testing
2. Dialect-specific behavior
3. Query building validation

### Phase 3: Adapters Module

1. Unit tests with mocked databases
2. Integration tests with real databases
3. Error handling and edge cases

### Phase 4: Extensions Module

1. Framework integration testing
2. Plugin behavior validation
3. Configuration testing

## Continuous Integration

### Test Execution

- Run unit tests on every commit
- Run integration tests on PR
- Generate coverage reports
- Performance regression detection

### Quality Gates

- All tests must pass
- Coverage thresholds must be met
- No new linting errors
- Type checking must pass

## Best Practices Summary

1. **Write tests first** when fixing bugs (TDD for bug fixes)
2. **One assertion per test** when possible
3. **Use descriptive test function names** that explain the scenario
4. **Group related tests** with consistent naming patterns and module organization
5. **Parameterize similar tests** to reduce duplication
6. **Mock external dependencies** in unit tests
7. **Test edge cases** and error conditions
8. **Keep tests simple** and focused
9. **Use fixtures** for common setup instead of test classes
10. **Clean up after tests** to prevent side effects
11. **Prefer functions over classes** for simpler, more readable tests

---

This document should be updated as the project evolves and new testing patterns emerge.
