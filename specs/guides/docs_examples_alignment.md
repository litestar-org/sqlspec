# Docs & Example Alignment

## Why
- Keep literalincluded snippets in `docs/` authoritative and executable.
- Reduce drift between prose and runnable code by treating documentation examples as pytest cases.

## Workflow
1. Update the Python example under `docs/examples/...` first and keep it function-based (`def test_*`).
2. Refresh the corresponding ``literalinclude`` in the `.rst` file:
   - Adjust `:lines:` and `:dedent:` ranges so the rendered snippet only shows the relevant part of the test.
   - Mention any helper imports or context (e.g., `contextlib.suppress`) in nearby prose.
3. Re-run the targeted example tests locally and record failures that require external services (Postgres, etc.) so reviewers know what still needs coverage.
4. When SQLite pooling is involved, use `tempfile.NamedTemporaryFile` (or `tmp_path`) to guarantee isolation. Delete any prior tables at the top of the example to keep re-runs deterministic.
5. Reference this checklist in PR descriptions whenever docs/examples are touched.

## Testing Command Examples
```bash
uv run pytest docs/examples/quickstart/quickstart_1.py docs/examples/quickstart/quickstart_2.py docs/examples/quickstart/quickstart_3.py docs/examples/quickstart/quickstart_6.py docs/examples/quickstart/quickstart_7.py docs/examples/quickstart/quickstart_8.py -q
```

- Async or adapter-specific samples (`quickstart_4.py`, `quickstart_5.py`, etc.) may need dedicated infrastructure. Explain any skips in the PR body so CI owners can follow up.
- Prefer smaller batches (per topic/section) to keep feedback loops fast.

## Review Checklist
- [ ] Example is function-based and runnable via pytest.
- [ ] Docs include/excerpt ranges match the function body.
- [ ] Tests were re-run or limitations were documented.
- [ ] Temporary SQLite files are used for pooled configs to avoid leakage between examples.
