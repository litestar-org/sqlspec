# Adapter Contract Tests

These tests hold behavior expected from more than one adapter. Case records live in `_cases.py` and reference driver fixtures by name. Contract fixtures resolve those names with `request.getfixturevalue()` so marks, xdist grouping, and optional service behavior stay attached to the case metadata.

Keep adapter-specific SQL, optional service setup, and one-off regressions in adapter-local files. Move behavior here only when at least two adapters should satisfy the same contract without adapter-name conditionals in the test body.
