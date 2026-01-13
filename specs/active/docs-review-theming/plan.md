# Plan: Documentation Review & Theming

## Tasks

- [x] **Fix Layout & Styling** `task-layout-css` <!-- id: 0 --> [1365b91]
  - Modify `docs/_static/custom.css`.
  - Add rules to hide `.sy-sidebar-secondary`.
  - Add rules to expand `.sy-content` / `.sy-layout`.
  - Add rules for `.sd-card` spacing (gap/margin).

- [x] **Global Theme Update** `task-theme-navy` <!-- id: 2 --> [bbe8593f]
  - Modify `docs/_static/custom.css`.
  - Change `html.dark` background variables (`--sy-c-bg`, `--sy-c-foot-background`, etc.) to `var(--litestar-navy)`.
  - Adjust code block backgrounds if necessary to ensure contrast.

- [x] **Observability Docs** `task-docs-observability` <!-- id: 3 --> [bbe8593f]
  - Update `docs/usage/observability.rst`.
  - Add detailed explanation of Observability features.
  - Add "OpenTelemetry Integration" section using `sqlspec.extensions.otel.enable_tracing`.
  - Add "Prometheus Integration" section using `sqlspec.extensions.prometheus.enable_metrics`.

- [x] **Content Integrity Check** `task-content-check` <!-- id: 1 --> [0e2e2a0]
  - Run `uv run sphinx-build -b linkcheck docs docs/_build/linkcheck`.
  - Review output.
  - Fix broken links in `docs/` files.
  - Commit fixes.

- [x] **Fix Footer & Branding Alignment** `task-fix-branding` <!-- id: 4 --> [a87005d1]
  - Update `docs/_static/custom.css`:
    - Ensure `html[data-theme="dark"]` selector is used for specificity.
    - Set `--sy-c-background` and `--sy-c-foot-background` to `#202235`.
    - Adjust surface colors for contrast.
  - Update `tools/sphinx_ext/playground_template.html` to use `#202235`.

## Recovery Plan
- If CSS grid changes break the mobile layout, wrap the overrides in a `@media (min-width: ...)` query.
- If `linkcheck` fails on flaky URLs (e.g., GitHub anchors), add them to `linkcheck_ignore` in `conf.py`.
