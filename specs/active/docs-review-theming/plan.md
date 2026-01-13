# Plan: Documentation Review & Theming

## Tasks

- [ ] **Fix Layout & Styling** `task-layout-css` <!-- id: 0 -->
  - Modify `docs/_static/custom.css`.
  - Add rules to hide `.sy-sidebar-secondary`.
  - Add rules to expand `.sy-content` / `.sy-layout`.
  - Add rules for `.sd-card` spacing (gap/margin).
  - verify layout with screenshot tools (optional) or visual confirm.

- [ ] **Content Integrity Check** `task-content-check` <!-- id: 1 -->
  - Run `uv run sphinx-build -b linkcheck docs docs/_build/linkcheck`.
  - Review output.
  - Fix broken links in `docs/` files.
  - Commit fixes.

## Recovery Plan
- If CSS grid changes break the mobile layout, wrap the overrides in a `@media (min-width: ...)` query.
- If `linkcheck` fails on flaky URLs (e.g., GitHub anchors), add them to `linkcheck_ignore` in `conf.py`.
