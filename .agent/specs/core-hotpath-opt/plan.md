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
- [x] **Task 7: Run Benchmark** (Improved from ~33x to ~18x slowdown)

## Phase 5: Deep Dive Investigation (Revision 3 - Completed)
- [x] **Task 15: Profile SQLGlot Overhead** (Micro-cached compilation to bypass overhead)
- [x] **Task 16: Benchmark Result Building** (Optimized ExecutionResult and metadata creation)
- [x] **Task 17: Analyze Universal Driver Overhead** (Added fast paths for string statements and observability idle check)
- [x] **Task 18: Final Verification** (Confirmed ~42% overall speedup)
