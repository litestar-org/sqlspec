# Plan: Documentation Review & Theming

## Tasks

- [x] **Fix Layout & Styling** `task-layout-css` <!-- id: 0 --> [1365b91]
  - Modify `docs/_static/custom.css`.
  - Add rules to hide `.sy-sidebar-secondary`.
  - Add rules to expand `.sy-content` / `.sy-layout`.
  - Add rules for `.sd-card` spacing (gap/margin).
  - verify layout with screenshot tools (optional) or visual confirm.

- [x] **Content Integrity Check** `task-content-check` <!-- id: 1 --> [0e2e2a0]
  - Run `uv run sphinx-build -b linkcheck docs docs/_build/linkcheck`.
  - Review output.
  - Fix broken links in `docs/` files.
  - Commit fixes.

## Recovery Plan
- If CSS grid changes break the mobile layout, wrap the overrides in a `@media (min-width: ...)` query.
- If `linkcheck` fails on flaky URLs (e.g., GitHub anchors), add them to `linkcheck_ignore` in `conf.py`.
