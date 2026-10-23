# Litestar & Cofin Ecosystem Branding Pack

Unified vector and raster branding pack across all 14 ecosystem packages.

## Brand Specifications

- **Primary Accent (Litestar Gold)**: `#EDB641` (`rgb(237, 182, 65)`)
- **Primary Dark Surface (Litestar Navy)**: `#202235` (`rgb(32, 34, 53)`)
- **Primary Light Surface**: `#F8FAFC` / `#FFFFFF`
- **Vector Grid**: `256x256` (`viewBox="0 0 256 256"`), `stroke-width="14"`, `stroke-linecap="round"`, `stroke-linejoin="round"`

## Included Projects

| Slug | Project | Description | Master Vector |
| :--- | :--- | :--- | :--- |
| `sqlspec` | **SQLSpec** | SQL Query Specification & Driver Execution Engine | [`sqlspec/svg/icon-gold.svg`](sqlspec/svg/icon-gold.svg) |
| `advanced-alchemy` | **Advanced Alchemy** | SQLAlchemy Repository, Service & Migration Toolkit | [`advanced-alchemy/svg/icon-gold.svg`](advanced-alchemy/svg/icon-gold.svg) |
| `pytest-databases` | **Pytest Databases** | Containerized Database Fixtures for Pytest | [`pytest-databases/svg/icon-gold.svg`](pytest-databases/svg/icon-gold.svg) |
| `litestar-vite` | **Litestar Vite** | Vite, Inertia.js & Modern Frontend Integration | [`litestar-vite/svg/icon-gold.svg`](litestar-vite/svg/icon-gold.svg) |
| `litestar-granian` | **Litestar Granian** | High-Performance Rust HTTP/ASGI Server Plugin | [`litestar-granian/svg/icon-gold.svg`](litestar-granian/svg/icon-gold.svg) |
| `litestar-mcp` | **Litestar MCP** | Model Context Protocol & A2A Integration | [`litestar-mcp/svg/icon-gold.svg`](litestar-mcp/svg/icon-gold.svg) |
| `litestar-queues` | **Litestar Queues** | Multi-Backend Async Task & Message Queues | [`litestar-queues/svg/icon-gold.svg`](litestar-queues/svg/icon-gold.svg) |
| `litestar-saq` | **Litestar SAQ** | Simple Async Queues Worker & Cron Plugin | [`litestar-saq/svg/icon-gold.svg`](litestar-saq/svg/icon-gold.svg) |
| `litestar-security` | **Litestar Security** | Authentication, Authorization & Guard Primitives | [`litestar-security/svg/icon-gold.svg`](litestar-security/svg/icon-gold.svg) |
| `litestar-autowire` | **Litestar Autowire** | Automatic Domain & Dependency Wiring | [`litestar-autowire/svg/icon-gold.svg`](litestar-autowire/svg/icon-gold.svg) |
| `litestar-email` | **Litestar Email** | Multi-Provider Transactional Email Plugin | [`litestar-email/svg/icon-gold.svg`](litestar-email/svg/icon-gold.svg) |
| `litestar-htmx` | **Litestar HTMX** | Server-Driven Hypermedia & HX-Swap Utilities | [`litestar-htmx/svg/icon-gold.svg`](litestar-htmx/svg/icon-gold.svg) |
| `vertebra` | **Vertebra** | High-Throughput Shared-Memory IPC & Process Backbone | [`vertebra/svg/icon-gold.svg`](vertebra/svg/icon-gold.svg) |
| `flow` | **Flow** | Spec-First Engineering & Directed Agent Workflows | [`flow/svg/icon-gold.svg`](flow/svg/icon-gold.svg) |

## Per-Project Folder Layout

Each `<project-slug>/` directory contains:

- **`svg/`**:
  - `icon-gold.svg` — Primary `#EDB641` monoline vector icon on transparent background (`logo.svg` / `logo-icon.svg`)
  - `icon-navy.svg` — Monochrome `#202235` vector icon on transparent background
  - `icon-white.svg` — Monochrome `#FFFFFF` vector icon on transparent background
  - `badge-dark.svg` — `#EDB641` icon on rounded `#202235` app tile with gold border
  - `badge-light.svg` — `#EDB641` icon on rounded `#F8FAFC` app tile
  - `square-dark.svg` — `#EDB641` icon on solid `#202235` square avatar tile
  - `banner-dark.svg` & `banner-light.svg` — `1200x300` horizontal lockup banners
- **`png/`**:
  - `icon-gold-{64,128,256,512,1024}.png` & `logo-default.png` (`256x256` transparent)
  - `icon-navy-512.png` & `icon-white-512.png`
  - `badge-dark-512.png`, `badge-light-512.png`, `square-dark-512.png`
- **`banners/`**:
  - `banner-dark.png` & `banner-light.png` (`1200x300` README hero banners)
  - `og-social-dark.png` & `og-social-light.png` (`1280x640` GitHub Social Preview / OpenGraph cards)
- **`favicons/`**:
  - `favicon.svg` (scalable vector favicon)
  - `favicon.ico` (multi-resolution `16x16`, `32x32`, `48x48` ICO container)
  - `favicon-16x16.png`, `favicon-32x32.png`, `favicon-48x48.png`, `favicon.png` (`64x64`)
  - `apple-touch-icon.png` (`180x180` on solid `#202235` with safe-zone margin)
  - `android-chrome-192x192.png` & `android-chrome-512x512.png`
  - `site.webmanifest`, `head-tags.html`, and `sphinx-conf-snippet.py`

## Regenerating the Pack

```bash
uv run python generate_branding_pack.py
```
