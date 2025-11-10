# Docs & Example Alignment

## Why

- Keep literalincluded snippets in `docs/` authoritative and executable.
- Reduce drift between prose and runnable code by treating documentation examples as pytest cases.

## Workflow

1. Update the Python example under `docs/examples/...` first and keep it function-based (`def test_*`).
2. Refresh the corresponding ``literalinclude`` in the `.rst` file:
   - Adjust `:lines:` and `:dedent:` ranges so the rendered snippet only shows the relevant part of the test.
   - Mention any helper imports or context (e.g., `contextlib.suppress`) in nearby prose.
3. Re-run the targeted example tests locally and record failures that require external services (Postgres, etc.) so reviewers know what still needs coverage. Use helpers like `SQLSPEC_QUICKSTART_PG_*` to keep DSNs out of docs snippets.
4. When SQLite pooling is involved, use `tempfile.NamedTemporaryFile` (or `tmp_path`) to guarantee isolation. Delete any prior tables at the top of the example to keep re-runs deterministic.
5. Reference this checklist in PR descriptions whenever docs/examples are touched.

## Testing Command Examples

```bash
uv run pytest docs/examples/quickstart -q
```

- `docs/examples/quickstart/conftest.py` sets `SQLSPEC_QUICKSTART_PG_*` from `pytest-databases` so `quickstart_5.py` stays copy/paste friendly in the docs.
- Async or adapter-specific samples no longer need separate commands—pytest collects `quickstart_4.py` and `quickstart_5.py` automatically via `python_files`.
- Prefer smaller batches (per topic/section) to keep feedback loops fast.

## Review Checklist

- [ ] Example is function-based and runnable via pytest.
- [ ] Docs include/excerpt ranges match the function body.
- [ ] Tests were re-run or limitations were documented.
- [ ] Temporary SQLite files are used for pooled configs to avoid leakage between examples.
