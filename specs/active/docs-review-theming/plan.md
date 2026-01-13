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

- [x] **Fix Card Colors** `task-fix-cards` <!-- id: 5 --> [7072ea1d]
  - Update `docs/_static/custom.css` to use `var(--sy-c-surface)` for `.sd-card` background in dark mode.

- [x] **Sticky Navigation Bar** `task-sticky-nav` <!-- id: 6 --> [b4d76813]
  - Modify `docs/_static/custom.css`.
  - Set `.navigation` to `position: fixed; bottom: 0`.
  - Adjust `left` offset for sidebars (0 on mobile, 18rem on tablet+).
  - Add `padding-bottom` to `.sy-content` to prevent content obscuration.
  - Style the bar with background/border/shadow.

- [ ] **Top Bar Submenu** `task-topbar-submenu` <!-- id: 7 -->
  - Modify `docs/conf.py`.
  - Update `html_theme_options['nav_links']` to group "Get Started", "Usage", "API", "Playground" into a "Docs" submenu.

- [ ] **Fix External Links** `task-fix-links` <!-- id: 8 -->
  - Investigate and fix broken "View Source", "Open in Claude" links.
  - Add "Open in Gemini" link.
  - Likely locations: `docs/_static/theme.js` or `tools/sphinx_ext/`.

- [ ] **Fix Footer Overlap** `task-footer-overlap` <!-- id: 9 -->
  - Modify `docs/_static/custom.css`.
  - Add padding to `.sy-foot` or `body` to ensure footer content is not covered by the fixed navigation bar.

## Recovery Plan
- If CSS grid changes break the mobile layout, wrap the overrides in a `@media (min-width: ...)` query.
- If `linkcheck` fails on flaky URLs (e.g., GitHub anchors), add them to `linkcheck_ignore` in `conf.py`.