# Specification: Documentation Review & Theming Overhaul

## 1. Overview
This specification addresses layout constraints, styling inconsistencies, and content accuracy in the SQLSpec documentation. The primary goal is to maximize the utility of the screen real estate by removing the unused right sidebar (Table of Contents) and ensuring the main content spans that area. Additionally, we will polish the styling of navigation elements and verify content integrity.

## 2. Problem Statement
- **Wasted Space:** The current layout reserves a significant portion of the right screen for a Table of Contents (TOC). On pages like the "Live Playground" and "API Reference" index, this sidebar is often empty or unnecessary, artificially constraining the main content and making the UI feel cramped.
- **Styling Issues:** The "Quick Navigation" buttons (cards) in the reference section lack sufficient spacing, causing them to visually blend or touch.
- **Content Trust:** A general review is needed to ensure links are valid and content is accurate.

## 3. User Stories
- As a **Developer**, I want the Playground to use the full width of my screen so I can see the code and results clearly without scrolling horizontally.
- As a **Reader**, I want the API Reference landing page to present navigation options clearly with proper spacing, so I don't misclick or feel overwhelmed by clutter.
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
  - Ensure the content container allows itself to grow (e.g., `max-width: 100%` or a larger `rem` value).

### 4.2. Component Styling
- **Quick Navigation Cards (`sphinx-design`):**
  - Target the grid/flex container holding the cards.
  - Increase the `gap` property (e.g., `gap: 1.5rem`).
  - Ensure individual `.sd-card` elements have appropriate margins if `gap` is not supported or effective in the specific context.

### 4.3. Content Verification
- **Link Checking:** Execute `sphinx-build -b linkcheck` to identify broken external and internal links.
- **Fixes:** Correct any broken links found in `docs/`.

## 5. Implementation Plan

### Phase 1: Styling & Layout
- **File:** `docs/_static/custom.css`
- **Action:** Implement CSS overrides to hide the right sidebar and expand the main content area.
- **Action:** Add spacing rules for `.sd-card` or their containers.

### Phase 2: Content Assurance
- **Tool:** `sphinx-build -b linkcheck docs docs/_build/linkcheck`
- **Action:** Analyze report and fix broken links in `.rst` / `.md` files.

## 6. Acceptance Criteria

### Layout
- [ ] **Full Width:** The right sidebar is not visible on `playground.html` and `reference/index.html`.
- [ ] **Expansion:** The main content area (text, code blocks, cards) visually expands to occupy the center and right columns.
- [ ] **Responsiveness:** The layout remains usable on mobile (where sidebars are typically hidden anyway) without horizontal scrollbar issues.

### Styling
- [ ] **Card Spacing:** Quick Navigation cards in the API Reference section have visible separation (at least `1rem` or `16px`) between them vertically and horizontally.

### Accuracy
- [ ] **Link Check:** The `linkcheck` build completes with no critical errors (ignoring known flaky external URLs if necessary).

## 7. Testing Strategy
- **Visual Inspection:** Build the docs locally (`make docs` or `uv run sphinx-build ...`) and view the HTML in a browser (or use the MCP screenshot tool if available).
- **Automated Check:** Run the linkcheck builder.
