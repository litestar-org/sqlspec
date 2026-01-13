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

- [x] **Fix Footer & Branding Alignment** `task-fix-branding` <!-- id: 4 --> [28072835]
  - Update `docs/_static/custom.css`:
    - Ensure `html[data-theme="dark"]` selector is used for specificity.
    - Set `--sy-c-background` and `--sy-c-foot-background` to `#202235`.
    - Adjust surface colors for contrast.
  - Update `tools/sphinx_ext/playground_template.html` to use `#202235`.
  - Removed playground status badge and moved Usage Tips to the primary actions row.

- [ ] **Enhanced Playground Output** `task-playground-table` <!-- id: 10 -->
  - Modify `tools/sphinx_ext/playground_template.html`.
  - Add CSS/HTML for a results table container.
  - Update JavaScript to capture the return value of the Python script.
  - Detect if the return value is a list of records and render it as a table.
  - Improve error display (friendly error screen).
  - Update the default Python script to return data instead of printing.

- [x] **Fix Card Colors** `task-fix-cards` <!-- id: 5 --> [7072ea1d]
  - Update `docs/_static/custom.css` to use `var(--sy-c-surface)` for `.sd-card` background in dark mode.

- [x] **Sticky Navigation Bar** `task-sticky-nav` <!-- id: 6 --> [b4d76813]
  - Modify `docs/_static/custom.css`.
  - Set `.navigation` to `position: fixed; bottom: 0`.
  - Adjust `left` offset for sidebars (0 on mobile, 18rem on tablet+).
  - Add `padding-bottom` to `.sy-content` to prevent content obscuration.
  - Style the bar with background/border/shadow.

- [x] **Top Bar Submenu** `task-topbar-submenu` <!-- id: 7 --> [12a14b98]
  - Modify `docs/conf.py`.
  - Update `html_theme_options['nav_links']` to group "Get Started", "Usage", "API", "Playground" into a "Docs" submenu.
  - Removed "Home" button from top nav.

- [x] **Fix External Links** `task-fix-links` <!-- id: 8 --> [3ea01240]
  - Investigate and fix broken "View Source", "Open in Claude" links.
  - Add "Open in Gemini" link.
  - Likely locations: `docs/_static/theme.js` or `tools/sphinx_ext/`.
  - Resolution: Overrode `copy-page-button.html` template from Shibuya theme.

- [x] **Fix Footer Overlap** `task-footer-overlap` <!-- id: 9 --> [8c6e774b]
  - Modify `docs/_static/custom.css`.
  - Revert fixed positioning of navigation; let it flow naturally at the end of content.

## Recovery Plan
- If CSS grid changes break the mobile layout, wrap the overrides in a `@media (min-width: ...)` query.
- If `linkcheck` fails on flaky URLs (e.g., GitHub anchors), add them to `linkcheck_ignore` in `conf.py`.