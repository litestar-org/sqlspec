# Specification: Documentation Review & Theming Overhaul

## 1. Overview
This specification addresses layout constraints, styling inconsistencies, and content accuracy in the SQLSpec documentation. The primary goal is to maximize the utility of the screen real estate by removing the unused right sidebar (Table of Contents) and ensuring the main content spans that area. Additionally, we will polish the styling of navigation elements and verify content integrity.

We are also enhancing the global theme to match the "Live Playground" aesthetic (using Litestar Navy #202235 for backgrounds) and expanding the Observability documentation to include clear guides for OpenTelemetry and Prometheus integration.

## 2. Problem Statement
- **Wasted Space:** The current layout reserves a significant portion of the right screen for a Table of Contents (TOC). On pages like the "Live Playground" and "API Reference" index, this sidebar is often empty or unnecessary, artificially constraining the main content and making the UI feel cramped.
- **Styling Issues:** The "Quick Navigation" buttons (cards) in the reference section lack sufficient spacing. The global dark mode background uses a standard dark gray, which conflicts with the richer Litestar Navy used in the Playground.
- **Content Gaps:** The Observability page is sparse, lacking explanation of components and specific instructions for enabling standard integrations (Prometheus, OpenTelemetry).
- **Content Trust:** A general review is needed to ensure links are valid and content is accurate.

## 3. User Stories
- As a **Developer**, I want the Playground to use the full width of my screen so I can see the code and results clearly without scrolling horizontally.
- As a **Reader**, I want the documentation to have a cohesive, branded look, using the Litestar Navy background in dark mode.
- As a **DevOps Engineer**, I want clear instructions on how to enable OpenTelemetry tracing and Prometheus metrics so I can monitor my application effectively.
- As a **Maintainer**, I want to ensure all links in the documentation are valid so users don't encounter 404s.

## 4. Proposed Solution

### 4.1. Layout Engineering (CSS)
We will modify `docs/_static/custom.css` to globally alter the Shibuya theme's grid layout.

- **Hide Secondary Sidebar:**
  - Target `.sy-sidebar-secondary` (or the equivalent Shibuya class for the right sidebar).
  - Set `display: none !important;`.
- **Expand Main Content:**
  - Target the main layout container (e.g., `.sy-layout` grid or `.sy-content`).
  - Adjust `grid-template-columns` or `width`/`max-width` to allow the content area to consume the space previously held by the secondary sidebar.
  - Ensure the content container allows itself to grow.

### 4.2. Theme Enhancements
- **Global Dark Background:**
  - Update `html.dark` CSS variables to use `var(--litestar-navy)` (#202235) for the main background.
  - Ensure code blocks and other elements contrast appropriately against this new background.

### 4.3. Component Styling
- **Quick Navigation Cards (`sphinx-design`):**
  - Increase the `gap` property (e.g., `gap: 1.5rem`).
  - Ensure individual `.sd-card` elements have appropriate margins.

### 4.4. Content Verification & Expansion
- **Observability Guide:**
  - Rewrite `docs/usage/observability.rst`.
  - Add sections for "OpenTelemetry" and "Prometheus".
  - Document `sqlspec.extensions.otel.enable_tracing` and `sqlspec.extensions.prometheus.enable_metrics`.
- **Link Checking:** Execute `sphinx-build -b linkcheck` and fix broken links.

## 5. Implementation Plan

### Phase 1: Styling & Layout
- **File:** `docs/_static/custom.css`
- **Action:** Implement CSS overrides to hide the right sidebar and expand the main content area.
- **Action:** Update `html.dark` background to `var(--litestar-navy)`.
- **Action:** Add spacing rules for `.sd-card`.

### Phase 2: Content Assurance
- **File:** `docs/usage/observability.rst`
- **Action:** Expand content with OTEL and Prometheus guides.
- **Tool:** `sphinx-build -b linkcheck docs docs/_build/linkcheck`
- **Action:** Fix broken links.

## 6. Acceptance Criteria

### Layout
- [ ] **Full Width:** The right sidebar is not visible on `playground.html` and `reference/index.html`.
- [ ] **Expansion:** The main content area visually expands to occupy the center and right columns.

### Styling
- [ ] **Global Theme:** Dark mode background is Litestar Navy (#202235).
- [ ] **Card Spacing:** Quick Navigation cards have visible separation.

### Content
- [ ] **Observability:** Docs clearly explain how to enable OTEL and Prometheus using `enable_tracing` and `enable_metrics`.
- [ ] **Link Check:** The `linkcheck` build completes with no critical errors.

## 7. Testing Strategy
- **Visual Inspection:** Build the docs locally and view the HTML in a browser.
- **Automated Check:** Run the linkcheck builder.