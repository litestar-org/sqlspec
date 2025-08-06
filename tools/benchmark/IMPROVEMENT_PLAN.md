# SQLSpec Benchmark Tool Improvement Plan

## Overview

This document outlines specific improvements needed before moving the benchmark tool to its own project. The analysis focuses on cleanup, accuracy, fairness, and usability improvements.

## Current Issues Identified

### 1. CLI Interface Complexity

**File**: `cli.py` (lines 67-91)
**Issue**: Excessive display configuration options that don't align with user needs
**Problems**:

- Too many visual config options (`--show-all`, `--max-items`, `--table-width`, `--display-mode`, `--no-truncate`)
- Focus on individual metrics rather than driver grouping
- Complex visualization options that obscure core comparison data

### 2. Missing Oracle Database Support

**File**: `suites/orm_comparison.py` (lines 140-230)
**Issue**: Oracle adapter exists in codebase but not integrated into benchmarks
**Missing**:

- Oracle sync benchmark configuration (using `oracledb` adapter)
- Oracle async benchmark configuration (using `oracledb` adapter)
- Container setup for Oracle database testing

### 3. Unfair Test Scenarios

**File**: `suites/orm_comparison.py` (lines 530-859)
**Issues**:

- Hardcoded IDs (line 24: `SINGLE_ROW_ID = 500`) make tests predictable
- Limited query complexity (mostly simple SELECT/INSERT/UPDATE)
- No realistic workload simulation (mixed operations, varying data sizes)
- Batch sizes are static (lines 25-26: `BATCH_UPDATE_LIMIT = 100`)

### 4. Display Logic Complexity

**File**: `visualization/reports.py` (lines 40-100)
**Issues**:

- Individual metric focus instead of driver-grouped results
- Complex display options that don't serve the core use case
- Multiple visualization modes when only driver comparison is needed

## Concrete Improvement Plan

### Phase 1: Simplify CLI Interface (Priority: High)

**Target File**: `cli.py`

**Changes Needed**:

1. **Remove unnecessary display options** (lines 67-91):

   ```python
   # REMOVE these options:
   --show-all, --max-items, --table-width, --display-mode, --no-truncate

   # KEEP only:
   --suite, --adapter, --iterations, --quick
   ```

2. **Default to driver-grouped results**:

   ```python
   # Default display should group by driver, not individual metrics
   display_options = {
       "group_by": "driver",  # NEW: Always group by database driver
       "show_write_operations": True,  # NEW: Focus on write performance
       "hide_individual_metrics": True  # NEW: Hide granular breakdowns
   }
   ```

### Phase 2: Add Oracle Support (Priority: High)

**Target File**: `suites/orm_comparison.py`

**Changes Needed**:

1. **Add Oracle sync configuration** (after line 114):

   ```python
   {
       "name": "Oracle Sync",
       "type": "sync",
       "get_sqlspec_config": lambda: self._get_oracle_sync_configs(host, port),
       "get_sqlalchemy_engine": lambda: create_engine(
           f"oracle+oracledb://{user}:{password}@{host}:{port}/{service}",
           poolclass=QueuePool,
           pool_size=20,
           max_overflow=0,
           pool_pre_ping=True,
       ),
       "setup_func": self._setup_sync_db,
       "requires_container": True,
   }
   ```

2. **Add Oracle async configuration**:

   ```python
   {
       "name": "Oracle Async",
       "type": "async",
       "get_sqlspec_config": lambda: self._get_oracle_async_configs(host, port),
       "get_sqlalchemy_engine": lambda: create_async_engine(
           f"oracle+oracledb_async://{user}:{password}@{host}:{port}/{service}",
           poolclass=pool.AsyncAdaptedQueuePool,
           pool_size=20,
           max_overflow=0,
           pool_pre_ping=True,
       ),
       "setup_func": self._setup_async_db,
       "requires_container": True,
   }
   ```

3. **Add Oracle container support** in `infrastructure/containers.py`:

   ```python
   def start_oracle(self, keep_containers: bool = False) -> tuple[str, int]:
       """Start Oracle container and return host, port."""
       # Implementation needed for Oracle XE container
   ```

4. **Add Oracle config methods**:

   ```python
   def _get_oracle_sync_configs(self, host: str, port: int) -> tuple[OracleConfig, OracleConfig]:
       """Get Oracle sync configs with and without caching."""

   def _get_oracle_async_configs(self, host: str, port: int) -> tuple[OracleAsyncConfig, OracleAsyncConfig]:
       """Get Oracle async configs with and without caching."""
   ```

### Phase 3: Improve Test Scenarios (Priority: Medium)

**Target File**: `suites/orm_comparison.py`

**Changes Needed**:

1. **Randomize test data** (lines 24-26):

   ```python
   # REPLACE static values with:
   import random

   def get_random_test_parameters():
       return {
           "single_row_id": random.randint(100, 900),
           "batch_size": random.choice([50, 100, 200]),
           "update_limit": random.randint(50, 150)
       }
   ```

2. **Add complex query scenarios**:

   ```python
   # NEW benchmark methods:
   def _benchmark_complex_join(self, ...):
       """Test multi-table joins with WHERE clauses."""

   def _benchmark_aggregation(self, ...):
       """Test GROUP BY, COUNT, SUM operations."""

   def _benchmark_mixed_workload(self, ...):
       """Test realistic mixed read/write operations."""
   ```

3. **Add varying data sizes**:

   ```python
   # NEW test scenarios:
   operations = {
       "select_single": self._benchmark_sync_select_single,
       "select_small_batch": lambda *args: self._benchmark_sync_select_bulk(*args, limit=10),
       "select_medium_batch": lambda *args: self._benchmark_sync_select_bulk(*args, limit=100),
       "select_large_batch": lambda *args: self._benchmark_sync_select_bulk(*args, limit=1000),
       # ... similar for insert/update operations
   }
   ```

### Phase 4: Simplify Reporting (Priority: Medium)

**Target File**: `visualization/reports.py`

**Changes Needed**:

1. **Default to driver-grouped display** (lines 62-79):

   ```python
   def display_suite_results(self, suite_name: str, results: dict[str, TimingResult]) -> None:
       """Display results grouped by driver, focusing on write operations."""
       # Group results by database driver first
       driver_results = self._group_by_driver(results)

       # Show only write operation performance by driver
       self._display_driver_comparison_table(driver_results)
   ```

2. **Remove individual metric displays**:

   ```python
   # REMOVE complex visualization methods, keep only:
   def _display_driver_comparison_table(self, driver_results: dict) -> None:
       """Simple table showing write performance by driver."""
   ```

3. **Focus on write operation metrics**:

   ```python
   def _filter_write_operations(self, results: dict) -> dict:
       """Filter to show only insert/update/delete operations."""
       write_ops = ['insert_bulk', 'update_bulk', 'delete_bulk']
       return {k: v for k, v in results.items()
               if any(op in k for op in write_ops)}
   ```

## Implementation Priority

### Immediate (Before Project Move)

1. ✅ **CLI Simplification**: Remove unnecessary display options
2. ✅ **Oracle Integration**: Add sync/async Oracle benchmarks
3. ✅ **Driver Grouping**: Default display to group by driver

### Near-term (After Project Move)

4. **Test Scenarios**: Add complex queries and randomized data
5. **Reporting Cleanup**: Simplify visualization logic
6. **Container Support**: Oracle database container integration

### Future Enhancements

7. **Mixed Workloads**: Realistic application usage patterns
8. **Performance Baselines**: Historical performance tracking
9. **Automated Regression Detection**: Alert on performance drops

## Estimated Effort

- **CLI Simplification**: 2-4 hours (remove code, update defaults)
- **Oracle Integration**: 6-8 hours (config setup, container support)
- **Test Improvement**: 4-6 hours (randomization, complex queries)
- **Reporting Cleanup**: 3-4 hours (simplify display logic)

**Total Effort**: 15-22 hours

## Success Criteria

1. **Simplified CLI**: Only essential options remain (`--suite`, `--adapter`, `--iterations`)
2. **Oracle Support**: Both sync and async Oracle benchmarks working
3. **Driver Focus**: Results grouped by database driver, not individual metrics
4. **Fair Testing**: Randomized data, varied query complexity
5. **Write Focus**: Emphasis on write operation performance comparison

## Files Requiring Changes

1. `cli.py` - Remove display complexity
2. `suites/orm_comparison.py` - Add Oracle, improve scenarios
3. `visualization/reports.py` - Simplify reporting
4. `infrastructure/containers.py` - Oracle container support
5. `config.py` - Remove unnecessary config options

This plan addresses all user requirements: cleanup, accuracy, fairness, Oracle support, and driver-focused results display.
