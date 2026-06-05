# Adapter Contract Tests

These tests hold behavior expected from more than one adapter. Case records live in `_cases.py` and reference driver fixtures by name. Contract fixtures resolve those names with `request.getfixturevalue()` so marks, xdist grouping, and optional service behavior stay attached to the case metadata.

Keep adapter-specific SQL, optional service setup, and one-off regressions in adapter-local files. Move behavior here only when at least two adapters should satisfy the same contract without adapter-name conditionals in the test body.

## Adding A Case

1. Add or update the `DriverCase` in `_cases.py`.
2. Keep unsupported or unwired adapters in `DEFERRED_DRIVER_CASES` with a concrete `reason`.
3. Add fixture setup in `conftest.py` only when the adapter needs contract-owned table setup.
4. Put reusable assertions in `behaviors.py`; central tests should stay thin.
5. Add input data to `_inputs.py` or schema metadata to `_schema.py` instead of branching on adapter names inside tests.

Default local coverage currently runs SQLite, aiosqlite, DuckDB, and MySQL-family contracts. Service-backed adapters
should keep their existing opt-in marks and xdist groups when they move from deferred rows to active rows.

## Capability Flags

`DriverCase` carries `supports_*` flags that gate behavior so a single contract can branch on
capability instead of adapter name. Flags are **additive and opt-in**: every flag defaults to a value
that means "no extra behavior" (`False`, except a few baseline truths like `supports_transactions`).
An adapter that opts into nothing sees identical behavior to before the flag existed.

Capability flags available for gating contracts:

- Result/IO: `supports_arrow`, `supports_explain`, `supports_storage_bridge`, `supports_copy`
- Statements: `supports_execute_many`, `supports_execute_script`, `supports_filtered_statement`,
  `supports_loader_input`, `supports_merge`, `supports_returning`, `supports_for_update`
- Types/codecs: `supports_json`, `supports_json_native`, `supports_arrays`,
  `supports_native_array_codec`, `supports_vector`, `supports_lob`
- Schema/migrations: `supports_migrations`, `supports_schema_qualified_ddl`,
  `supports_multi_schema_migrations`, `supports_data_dictionary`
- Connectivity: `supports_pooling`, `supports_transactions`, `supports_exception_translation`

### Deviations vs Extra Assertions

`deviations` is **subtractive**: a tuple of string keys that gate or relax a generic assertion
(`if "key" not in case.deviations: ...`). Use it when an adapter cannot satisfy part of a shared
contract.

`extra_assertions` is the **additive** counterpart: a tuple of registered proof keys that let a single
contract run adapter-specific extra checks without a separate per-adapter file. Register a proof with
`register_sync_extra_assertion(key, scope, fn)` / `register_async_extra_assertion(...)` in
`behaviors.py`, opt a case in via `extra_assertions=("scope:key",)`, and dispatch it from the owning
behavior with `dispatch_sync_extra_assertions(driver, case, scope)`. Keys registered nowhere fail loud
via `validate_extra_assertions(case)` so consolidating a per-adapter test never silently drops coverage.

## Parameter Styles

Generic parameter-style behavior belongs in `test_parameter_styles_contract.py` with cases from `_inputs.py`.
Use those cases for qmark tuple/list binding, named dictionaries, `SQL(...)` objects, repeated named binds,
`execute_many()` tuple and dictionary payloads, LIKE predicates, and injection-looking values.

Keep adapter-local parameter files only for dialect-specific behavior or edge regressions. SQLite and aiosqlite
None-heavy edge coverage lives in adapter-local `test_none_parameters.py` files because those cases assert
SQLite storage and parameter-count behavior rather than the shared SQLSpec parameter-style contract.
DuckDB numeric, mixed-style, array/list, analytics, and None-heavy parameter behavior remains in adapter-local
variant files because those cases assert DuckDB-specific parameter semantics.
MySQL native pyformat, named pyformat, boolean, float, empty-string, and special-character parameter behavior
remains in adapter-local variant files because those cases assert MySQL-family or driver-specific parameter
semantics rather than generic SQLSpec parameter-style behavior.
