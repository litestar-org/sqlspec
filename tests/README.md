# Adding SQLSpec tests

Put each new test at the lowest layer that proves the behavior once. This keeps adapter coverage aligned and avoids repeats.

## Choose the test location

Use `tests/integration/adapters/contracts/` when every capable adapter should act the same. Add the case to the shared suite. Describe any needed capability flag in its `README.md`. Contract tests use shared driver cases and real sessions.

Use `tests/integration/adapters/<adapter>/` for behavior that belongs to one backend or vendor API. Examples include emulator differences, native bulk loads, server data types, and unique driver options. Reuse `tests/integration/fixtures/`. Do not create a second pool or service fixture in the adapter folder.

Use `tests/unit/` for code that does not need a database. Good targets include parsing, config setup, conversion functions, and clear third-party boundaries. Do not build a driver over a mock connection. Do not patch private compile or dispatch methods. Test those paths through an integration test or contract.

## Use integration resources correctly

Integration tests use `pytest-databases`. Keep pool configs session-scoped and give each test a fresh driver session. Use the existing `xdist_group` for shared services such as Oracle, MySQL, BigQuery, and Spanner.

Cloud emulator tests are off locally unless their noted environment flag and command option are both present. A missing service may skip at its availability boundary. A failed query or setup step must fail the test.

## Verify the change

Run the smallest relevant file or adapter directory first.

```bash
uv run pytest tests/path/to/test_file.py -n 2
```

Then run the repository gates before opening a pull request.

```bash
make lint
make type-check
make test
make coverage
```

When adapter behavior changes, also run the matching contract cases and the adapter's integration directory. Compiled-build changes require `make install-compiled` followed by `make test`.
