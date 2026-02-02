## [2026-02-02] - Phase 5 Task 15-18: Deep Dive Optimizations

- **Verified:** Benchmark improved from 0.49s to 0.28s (~42% faster). Slowdown vs raw sqlite3 reduced from 33x to 18x.
- **Files changed:** `sqlspec/core/compiler.py`, `sqlspec/driver/_common.py`, `sqlspec/core/statement.py`, `sqlspec/observability/_runtime.py`
- **Commit:** (Current)
- **Learnings:**
  - **Micro-caching works:** Adding a single-slot cache in `SQLProcessor.compile` bypassed hash/lookup overhead for repeated queries, yielding the largest single gain.
  - **String fast paths:** Caching string statements in `prepare_statement` and optimizing `SQL.copy` avoided object churn.
  - **Observability overhead:** Even "disabled" observability had cost; adding `is_idle` check removed it.
  - **Remaining overhead:** The remaining 18x gap is due to the fundamental architecture (Python function calls, abstraction layers) which cannot be removed without a rewrite in a lower-level language (Rust/C).
