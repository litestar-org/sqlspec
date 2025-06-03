# Adapter Execution Refactor Plan

## Rationale

To ensure consistency, maintainability, and correctness across all database adapters in the `sqlspec` project, all driver classes must follow a unified execution method structure. This structure is already implemented in the `SqliteDriver` and must be adopted by all other adapters (sync and async).

## Required Method Structure (MANDATORY)

Each driver class **MUST** define the following methods at the top of the class, in this order:

1. `_execute_statement` — Main dispatch method. Determines execution mode and delegates to one of the following:
2. `_execute` — Handles single-statement execution.
3. `_execute_many` — Handles batch/many execution.
4. `_execute_script` — Handles script execution.

**Each method should contain only the logic relevant to its execution mode.**

- All parameter handling must use the new SQL object's `.parameters` and `.to_sql(placeholder_style=...)`.
- Result wrapping must use `_wrap_select_result` and `_wrap_execute_result` as per protocol.
- Methods must be as concise as possible, following strict in-lining and minimal variable rules.

## Adapters to Update

- [ ] `sqlspec/adapters/duckdb/driver.py`
- [ ] `sqlspec/adapters/bigquery/driver.py`
- [ ] `sqlspec/adapters/oracledb/driver.py`
- [ ] `sqlspec/adapters/psycopg/driver.py`
- [ ] `sqlspec/adapters/adbc/driver.py`
- [ ] `sqlspec/adapters/aiosqlite/driver.py`
- [ ] `sqlspec/adapters/asyncpg/driver.py`
- [ ] `sqlspec/adapters/asyncmy/driver.py`
- [ ] `sqlspec/adapters/psqlpy/driver.py`

## Next Steps

1. Start with one or two of the most widely used adapters (e.g., DuckDB and Psycopg) and refactor them to match the `SqliteDriver` structure.
2. Apply the same pattern to the rest, including all async drivers.
3. Ensure all tests pass after each refactor.
4. Remove any legacy or redundant execution logic.
5. Update documentation and code comments as needed.

---

**This plan is mandatory for all new and existing adapters.**

For questions or clarifications, refer to the project rules or contact the maintainers.
