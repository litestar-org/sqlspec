# Adapter Contract Tests

These tests hold behavior expected from more than one adapter. Case records live in `_cases.py` and reference driver fixtures by name. Contract fixtures resolve those names with `request.getfixturevalue()` so marks, xdist grouping, and optional service behavior stay attached to the case metadata.

Keep adapter-specific SQL, optional service setup, and one-off regressions in adapter-local files. Move behavior here only when at least two adapters should satisfy the same contract without adapter-name conditionals in the test body.

## Adding A Case

1. Add or update the `DriverCase` in `_cases.py`.
2. Keep unsupported or unwired adapters in `DEFERRED_DRIVER_CASES` with a concrete `reason`.
3. Add fixture setup in `conftest.py` only when the adapter needs contract-owned table setup.
4. Put reusable assertions in `behaviors.py`; central tests should stay thin.
5. Add input data to `_inputs.py` or schema metadata to `_schema.py` instead of branching on adapter names inside tests.

Default local coverage currently runs SQLite, aiosqlite, and DuckDB contracts. Service-backed adapters should keep
their existing opt-in marks and xdist groups when they move from deferred rows to active rows.

## Parameter Styles

Generic parameter-style behavior belongs in `test_parameter_styles_contract.py` with cases from `_inputs.py`.
Use those cases for qmark tuple/list binding, named dictionaries, `SQL(...)` objects, repeated named binds,
`execute_many()` tuple and dictionary payloads, LIKE predicates, and injection-looking values.

Keep adapter-local parameter files only for dialect-specific behavior or edge regressions. SQLite and aiosqlite
None-heavy edge coverage lives in adapter-local `test_none_parameters.py` files because those cases assert
SQLite storage and parameter-count behavior rather than the shared SQLSpec parameter-style contract.
DuckDB numeric, mixed-style, array/list, analytics, and None-heavy parameter behavior remains in adapter-local
variant files because those cases assert DuckDB-specific parameter semantics.
