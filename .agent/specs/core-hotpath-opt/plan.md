# Implementation Plan: Core Hotpath Optimization

**Flow ID:** `core-hotpath-opt`

## Phase 1: Dispatch Optimization
- [x] **Task 1: Create `TypeDispatcher` utility** f1fbb8da
- [x] **Task 2: Refactor `StatementFilter`** ac0b28b3
- [x] **Task 3: Optimize `_should_auto_detect_many`** 785d5197

## Phase 2: Compilation Caching & AST Reuse
- [x] **Task 4: Shared `StatementConfig`** c11594ac
- [x] **Task 5: Stable Cache Keys** f1ac98de
- [x] **Task 8: SQLGlot Usage Audit** (Verified existing behavior)

## Phase 3: Driver Hotpath
- [x] **Task 6: Refactor `_sync.py` Execution Loop** c8f43f64
- [x] **Task 11: Optimize Parameter Cache Key** (Implemented tuple keys)
- [x] **Task 12: Disable Parameter Type Wrapping for SQLite** (Implemented in build_statement_config)
- [x] **Task 13: Optimize prepare_driver_parameters** (Implemented bypass optimization)
- [x] **Task 14: Optimize _structural_fingerprint** (Implemented tuple return)

## Phase 4: Verification
- [x] **Task 9: Mypyc Compatibility Check** Verified
- [x] **Task 10: Optimize Config Hashing** (Verified StatementConfig caching logic)
- [~] **Task 7: Run Benchmark** (Ongoing check - currently ~27x slowdown)

## Phase 5: Deep Dive Investigation (Revision 3)
- [ ] **Task 15: Profile SQLGlot Overhead**
  - Create micro-benchmark for `sqlglot.parse_one` and `expression.sql()`.
  - Isolate impact on hot path.
- [ ] **Task 16: Benchmark Result Building**
  - Profile `collect_rows` and `create_execution_result`.
  - Compare raw `fetchall()` vs dict construction loop.
- [ ] **Task 17: Analyze Universal Driver Overhead**
  - Review `_sync.py` and `_common.py` for hidden per-row costs (spans, logging).
- [ ] **Task 18: Final Verification**
  - Confirm final performance gains after deep dive fixes.