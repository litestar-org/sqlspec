"""Generate the complete multi-variant Branding Pack and Favicon suite for all 14 projects."""

import re
import shutil
import subprocess
import sys
from pathlib import Path

import msgspec

REPO_ROOT = Path(__file__).resolve().parent.parent
BRANDING_OUT_DIR = REPO_ROOT / "branding"

GOLD = "#EDB641"
NAVY = "#202235"
WHITE = "#FFFFFF"
LIGHT_BG = "#F8FAFC"
SLATE_BORDER = "#CBD5E1"
SLATE_TEXT = "#475569"
CANVAS_SIZE = 256
MAGICK_BIN = shutil.which("magick") or "/usr/bin/magick"

_TAG_PATTERN = re.compile(r"<(ellipse|circle|rect|path)\s+([^/>]+)/>")
_ATTR_PATTERN = re.compile(r'(\w[\w-]*)="([^"]*)"')
_JSON_ENCODER = msgspec.json.Encoder()


def format_json_bytes(payload: object) -> bytes:
    """Encode and pretty-format a JSON payload using msgspec."""
    return msgspec.json.format(_JSON_ENCODER.encode(payload), indent=2)


class ProjectBrandSpec(msgspec.Struct, frozen=True, gc=False):
    """Metadata and geometry key for a single ecosystem project's brand pack."""

    slug: str
    title: str
    upper_title: str
    subtitle: str
    icon_key: str

    @staticmethod
    def stroke_attrs(color: str = GOLD, width: int = 14) -> str:
        """Return standard monoline SVG stroke attributes for the given color."""
        return f'fill="none" stroke="{color}" stroke-width="{width}" stroke-linecap="round" stroke-linejoin="round"'

    def svg_body(self, color: str = GOLD) -> str:
        """Return the inner 256x256 SVG path/shape elements in the requested stroke/fill color."""
        sa = self.stroke_attrs(color, 14)
        sa12 = self.stroke_attrs(color, 12)

        bodies: dict[str, str] = {
            "sqlspec": f"""  <ellipse cx="104" cy="56" rx="72" ry="26" {sa}/>
  <path d="M32 56V180" {sa}/>
  <path d="M176 56V100" {sa}/>
  <path d="M32 118C32 132.36 64.24 144 104 144C108.5 144 112.9 143.85 117.1 143.56" {sa}/>
  <path d="M32 180C32 194.36 64.24 206 104 206C108.5 206 112.9 205.85 117.1 205.56" {sa}/>
  <rect x="136" y="124" width="92" height="92" rx="22" {sa}/>
  <path d="M160 154L180 170L160 186" {sa}/>
  <path d="M188 186H204" {sa}/>""",
            "advanced_alchemy": f"""  <rect x="92" y="24" width="72" height="24" rx="12" {sa}/>
  <path d="M106 48V86.5C68.5 96.8 42 131.2 42 172C42 213.97 80.5 236 128 236C175.5 236 214 213.97 214 172C214 131.2 187.5 96.8 150 86.5V48" {sa}/>
  <path d="M46 140C78 158 104 158 128 142C152 126 178 126 210 144" {sa}/>
  <circle cx="90" cy="186" r="14" {sa12}/>
  <circle cx="134" cy="206" r="11" {sa12}/>
  <circle cx="164" cy="174" r="9" {sa12}/>""",
            "pytest_databases": f"""  <ellipse cx="104" cy="56" rx="72" ry="26" {sa}/>
  <path d="M32 56V180" {sa}/>
  <path d="M176 56V104" {sa}/>
  <path d="M32 118C32 132.36 64.24 144 104 144C111.5 144 118.7 143.58 125.4 142.8" {sa}/>
  <path d="M32 180C32 194.36 64.24 206 104 206C108.5 206 112.9 205.85 117.1 205.56" {sa}/>
  <circle cx="182" cy="172" r="46" {sa}/>
  <path d="M161 172L175 186L205 156" {sa}/>""",
            "litestar_email": f"""  <rect x="56" y="64" width="172" height="128" rx="20" {sa}/>
  <path d="M64 74L142 136L220 74" {sa}/>
  <path d="M20 96H36" {sa}/>
  <path d="M16 128H36" {sa}/>
  <path d="M24 160H36" {sa}/>""",
            "litestar_htmx": f"""  <path d="M72 68L24 128L72 188" {sa}/>
  <path d="M184 68L232 128L184 188" {sa}/>
  <path d="M96 104H160L140 84" {sa}/>
  <path d="M160 152H96L116 172" {sa}/>""",
            "litestar_vite": f"""  <path d="M148 24L52 140H126L102 232L204 112H130L148 24Z" {sa}/>""",
            "litestar_queues": f"""  <rect x="32" y="44" width="136" height="36" rx="18" {sa}/>
  <rect x="60" y="110" width="136" height="36" rx="18" {sa}/>
  <rect x="32" y="176" width="116" height="36" rx="18" {sa}/>
  <path d="M176 194H224" {sa}/>
  <path d="M204 174L224 194L204 214" {sa}/>""",
            "litestar_saq": f"""  <path d="M204 104A80 80 0 1 0 194 176" {sa}/>
  <path d="M204 68V106H166" {sa}/>
  <path d="M138 72L92 132H132L116 184L164 124H124L138 72Z" {sa}/>""",
            "litestar_mcp": f"""  <path d="M148 44L52 140C38.75 153.25 38.75 174.75 52 188C65.25 201.25 86.75 201.25 100 188L176 112" {sa}/>
  <path d="M80 144L156 68C169.25 54.75 190.75 54.75 204 68C217.25 81.25 217.25 102.75 204 116L108 212" {sa}/>
  <circle cx="156" cy="36" r="12" {sa}/>
  <circle cx="100" cy="220" r="12" {sa}/>""",
            "litestar_granian": f"""  <path d="M196 154A74 74 0 1 1 148 56" {sa}/>
  <path d="M28 182C80 198 144 176 208 92" {sa}/>
  <path d="M172 92H208V128" {sa}/>
  <circle cx="114" cy="116" r="20" {sa}/>""",
            "litestar_security": f"""  <path d="M128 28L48 60V120C48 174 82 214 128 232C174 214 208 174 208 120V60L128 28Z" {sa}/>
  <circle cx="128" cy="112" r="20" {sa}/>
  <path d="M128 132V176" {sa}/>""",
            "litestar_autowire": f"""  <path d="M128 84L166 106V150L128 172L90 150V106L128 84Z" {sa}/>
  <circle cx="128" cy="128" r="10" fill="{color}"/>
  <circle cx="52" cy="60" r="16" {sa}/>
  <path d="M64 72L94 102" {sa}/>
  <circle cx="204" cy="60" r="16" {sa}/>
  <path d="M192 72L162 102" {sa}/>
  <circle cx="52" cy="196" r="16" {sa}/>
  <path d="M64 184L94 154" {sa}/>
  <circle cx="204" cy="196" r="16" {sa}/>
  <path d="M192 184L162 154" {sa}/>""",
            "vertebra": f"""  <path d="M64 56L128 32L192 56L168 88H88L64 56Z" {sa}/>
  <path d="M56 120L128 96L200 120L172 152H84L56 120Z" {sa}/>
  <path d="M68 184L128 160L188 184L160 216H96L68 184Z" {sa}/>
  <path d="M128 24V232" {sa}/>""",
            "flow": f"""  <circle cx="52" cy="64" r="22" {sa}/>
  <circle cx="52" cy="192" r="22" {sa}/>
  <circle cx="156" cy="128" r="22" {sa}/>
  <path d="M74 64C112 64 108 128 134 128" {sa}/>
  <path d="M74 192C112 192 108 128 134 128" {sa}/>
  <path d="M178 128H232" {sa}/>
  <path d="M210 106L232 128L210 150" {sa}/>""",
        }
        return bodies[self.icon_key]

    def wrap_svg(
        self, body: str, width: int = CANVAS_SIZE, height: int = CANVAS_SIZE, viewbox: str = "0 0 256 256"
    ) -> str:
        """Wrap SVG elements in a standard root element."""
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="{viewbox}" fill="none" role="img" aria-label="{self.title}">\n'
            f"  <title>{self.title}</title>\n"
            f"{body}\n"
            f"</svg>\n"
        )

    def banner_svg(self, dark: bool = True) -> str:
        """Build a 1200x300 horizontal lockup SVG banner for dark or light mode."""
        bg = NAVY if dark else LIGHT_BG
        title_color = WHITE if dark else NAVY
        sub_color = GOLD if dark else SLATE_TEXT
        body = (
            f'  <rect width="1200" height="300" rx="16" fill="{bg}"/>\n'
            f'  <g transform="translate(56, 22)">\n'
            f"{self.svg_body(GOLD)}\n"
            f"  </g>\n"
            f'  <line x1="348" y1="48" x2="348" y2="252" stroke="{GOLD}" stroke-width="5" stroke-linecap="round"/>\n'
            f'  <text x="392" y="148" fill="{title_color}" font-family="Inter, system-ui, -apple-system, sans-serif" '
            f'font-size="76" font-weight="700" letter-spacing="6">{self.upper_title}</text>\n'
            f'  <text x="396" y="206" fill="{sub_color}" font-family="Inter, system-ui, -apple-system, sans-serif" '
            f'font-size="26" font-weight="500" letter-spacing="1.5">{self.subtitle.replace("&", "&amp;")}</text>'
        )
        return self.wrap_svg(body, width=1200, height=300, viewbox="0 0 1200 300")


PROJECTS: tuple[ProjectBrandSpec, ...] = (
    ProjectBrandSpec("sqlspec", "SQLSpec", "SQLSPEC", "SQL Query Specification & Driver Execution Engine", "sqlspec"),
    ProjectBrandSpec(
        "advanced-alchemy",
        "Advanced Alchemy",
        "ADVANCED ALCHEMY",
        "SQLAlchemy Repository, Service & Migration Toolkit",
        "advanced_alchemy",
    ),
    ProjectBrandSpec(
        "pytest-databases",
        "Pytest Databases",
        "PYTEST DATABASES",
        "Containerized Database Fixtures for Pytest",
        "pytest_databases",
    ),
    ProjectBrandSpec(
        "litestar-vite",
        "Litestar Vite",
        "LITESTAR VITE",
        "Vite, Inertia.js & Modern Frontend Integration",
        "litestar_vite",
    ),
    ProjectBrandSpec(
        "litestar-granian",
        "Litestar Granian",
        "LITESTAR GRANIAN",
        "High-Performance Rust HTTP/ASGI Server Plugin",
        "litestar_granian",
    ),
    ProjectBrandSpec(
        "litestar-mcp", "Litestar MCP", "LITESTAR MCP", "Model Context Protocol & A2A Integration", "litestar_mcp"
    ),
    ProjectBrandSpec(
        "litestar-queues",
        "Litestar Queues",
        "LITESTAR QUEUES",
        "Multi-Backend Async Task & Message Queues",
        "litestar_queues",
    ),
    ProjectBrandSpec(
        "litestar-saq", "Litestar SAQ", "LITESTAR SAQ", "Simple Async Queues Worker & Cron Plugin", "litestar_saq"
    ),
    ProjectBrandSpec(
        "litestar-security",
        "Litestar Security",
        "LITESTAR SECURITY",
        "Authentication, Authorization & Guard Primitives",
        "litestar_security",
    ),
    ProjectBrandSpec(
        "litestar-autowire",
        "Litestar Autowire",
        "LITESTAR AUTOWIRE",
        "Automatic Domain & Dependency Wiring",
        "litestar_autowire",
    ),
    ProjectBrandSpec(
        "litestar-email",
        "Litestar Email",
        "LITESTAR EMAIL",
        "Multi-Provider Transactional Email Plugin",
        "litestar_email",
    ),
    ProjectBrandSpec(
        "litestar-htmx",
        "Litestar HTMX",
        "LITESTAR HTMX",
        "Server-Driven Hypermedia & HX-Swap Utilities",
        "litestar_htmx",
    ),
    ProjectBrandSpec(
        "vertebra", "Vertebra", "VERTEBRA", "High-Throughput Shared-Memory IPC & Process Backbone", "vertebra"
    ),
    ProjectBrandSpec("flow", "Flow", "FLOW", "Spec-First Engineering & Directed Agent Workflows", "flow"),
)


def svg_to_mvg_draw(svg_text: str) -> str:
    """Convert clean 256x256 SVG icon elements into an ImageMagick MVG draw string."""
    cmds: list[str] = ["stroke-linecap round", "stroke-linejoin round"]
    for match in _TAG_PATTERN.finditer(svg_text):
        tag = match.group(1)
        attrib = dict(_ATTR_PATTERN.findall(match.group(2)))
        fill = attrib.get("fill", "none")
        stroke = attrib.get("stroke", "none")
        sw = attrib.get("stroke-width", "14")
        if fill != "none" and stroke == "none":
            cmds.append(f"fill '{fill}' stroke none")
        elif fill != "none" and stroke != "none":
            cmds.append(f"fill '{fill}' stroke '{stroke}' stroke-width {sw}")
        else:
            cmds.append(f"fill none stroke '{stroke}' stroke-width {sw}")

        if tag == "ellipse":
            cmds.append(f"ellipse {attrib['cx']},{attrib['cy']} {attrib['rx']},{attrib['ry']} 0,360")
        elif tag == "circle":
            cx = float(attrib["cx"])
            cy = float(attrib["cy"])
            r = float(attrib["r"])
            cmds.append(f"circle {cx},{cy} {cx + r},{cy}")
        elif tag == "rect":
            x = float(attrib["x"])
            y = float(attrib["y"])
            w = float(attrib["width"])
            h = float(attrib["height"])
            rx = float(attrib.get("rx", "0"))
            if rx > 0:
                cmds.append(f"roundrectangle {x},{y} {x + w},{y + h} {rx},{rx}")
            else:
                cmds.append(f"rectangle {x},{y} {x + w},{y + h}")
        elif tag == "path":
            cmds.append(f"path '{attrib['d']}'")
    return " ".join(cmds)


def render_canvas_png(
    mvg: str, out_path: Path, target_size: int, bg: str = "none", pad_canvas: int = CANVAS_SIZE
) -> None:
    """Render an MVG vector command string at 256x256 and resize cleanly to target_size."""
    cmd = [MAGICK_BIN, "-size", "256x256", f"xc:{bg}", "-draw", mvg]
    if pad_canvas != CANVAS_SIZE:
        cmd.extend(["-gravity", "center", "-background", bg, "-extent", f"{pad_canvas}x{pad_canvas}"])
    if target_size != pad_canvas:
        cmd.extend(["-filter", "Lanczos", "-resize", f"{target_size}x{target_size}"])
    cmd.append(str(out_path))
    subprocess.run(cmd, check=True)


def generate_project_pack(spec: ProjectBrandSpec, root_dir: Path) -> None:
    """Generate svg/, png/, banners/, and favicons/ for a single project."""
    proj_dir = root_dir / spec.slug
    svg_dir = proj_dir / "svg"
    png_dir = proj_dir / "png"
    banners_dir = proj_dir / "banners"
    favicons_dir = proj_dir / "favicons"

    for d in (svg_dir, png_dir, banners_dir, favicons_dir):
        d.mkdir(parents=True, exist_ok=True)

    icon_gold_svg = spec.wrap_svg(spec.svg_body(GOLD))
    icon_navy_svg = spec.wrap_svg(spec.svg_body(NAVY))
    icon_white_svg = spec.wrap_svg(spec.svg_body(WHITE))

    badge_dark_svg = spec.wrap_svg(
        f'  <rect x="8" y="8" width="240" height="240" rx="52" fill="{NAVY}" stroke="{GOLD}" stroke-width="6"/>\n'
        + spec.svg_body(GOLD)
    )
    badge_light_svg = spec.wrap_svg(
        f'  <rect x="8" y="8" width="240" height="240" rx="52" fill="{LIGHT_BG}" stroke="{SLATE_BORDER}" stroke-width="6"/>\n'
        + spec.svg_body(GOLD)
    )
    square_dark_svg = spec.wrap_svg(
        f'  <rect x="0" y="0" width="256" height="256" fill="{NAVY}"/>\n' + spec.svg_body(GOLD)
    )

    (svg_dir / "icon-gold.svg").write_text(icon_gold_svg, encoding="utf-8")
    (svg_dir / "icon-navy.svg").write_text(icon_navy_svg, encoding="utf-8")
    (svg_dir / "icon-white.svg").write_text(icon_white_svg, encoding="utf-8")
    (svg_dir / "badge-dark.svg").write_text(badge_dark_svg, encoding="utf-8")
    (svg_dir / "badge-light.svg").write_text(badge_light_svg, encoding="utf-8")
    (svg_dir / "square-dark.svg").write_text(square_dark_svg, encoding="utf-8")
    (svg_dir / "banner-dark.svg").write_text(spec.banner_svg(dark=True), encoding="utf-8")
    (svg_dir / "banner-light.svg").write_text(spec.banner_svg(dark=False), encoding="utf-8")

    mvg_gold = svg_to_mvg_draw(icon_gold_svg)
    mvg_navy = svg_to_mvg_draw(icon_navy_svg)
    mvg_white = svg_to_mvg_draw(icon_white_svg)
    mvg_badge_dark = svg_to_mvg_draw(badge_dark_svg)
    mvg_badge_light = svg_to_mvg_draw(badge_light_svg)

    for sz in (64, 128, 256, 512, 1024):
        render_canvas_png(mvg_gold, png_dir / f"icon-gold-{sz}.png", target_size=sz, bg="none")
    shutil.copyfile(png_dir / "icon-gold-256.png", png_dir / "logo-default.png")

    render_canvas_png(mvg_navy, png_dir / "icon-navy-512.png", target_size=512, bg="none")
    render_canvas_png(mvg_white, png_dir / "icon-white-512.png", target_size=512, bg="none")
    render_canvas_png(mvg_badge_dark, png_dir / "badge-dark-512.png", target_size=512, bg="none")
    render_canvas_png(mvg_badge_light, png_dir / "badge-light-512.png", target_size=512, bg="none")
    render_canvas_png(mvg_gold, png_dir / "square-dark-512.png", target_size=512, bg=NAVY, pad_canvas=300)

    (favicons_dir / "favicon.svg").write_text(icon_gold_svg, encoding="utf-8")
    for fsz in (16, 32, 48):
        render_canvas_png(mvg_gold, favicons_dir / f"favicon-{fsz}x{fsz}.png", target_size=fsz, bg="none")
    shutil.copyfile(png_dir / "icon-gold-64.png", favicons_dir / "favicon.png")

    subprocess.run(
        [
            MAGICK_BIN,
            str(favicons_dir / "favicon-16x16.png"),
            str(favicons_dir / "favicon-32x32.png"),
            str(favicons_dir / "favicon-48x48.png"),
            str(favicons_dir / "favicon.ico"),
        ],
        check=True,
    )

    render_canvas_png(mvg_gold, favicons_dir / "apple-touch-icon.png", target_size=180, bg=NAVY, pad_canvas=310)
    render_canvas_png(mvg_gold, favicons_dir / "android-chrome-192x192.png", target_size=192, bg=NAVY, pad_canvas=300)
    render_canvas_png(mvg_gold, favicons_dir / "android-chrome-512x512.png", target_size=512, bg=NAVY, pad_canvas=300)

    manifest_payload = {
        "name": spec.title,
        "short_name": spec.slug,
        "icons": [
            {"src": "/android-chrome-192x192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/android-chrome-512x512.png", "sizes": "512x512", "type": "image/png"},
        ],
        "theme_color": NAVY,
        "background_color": NAVY,
        "display": "standalone",
    }
    (favicons_dir / "site.webmanifest").write_bytes(format_json_bytes(manifest_payload))

    head_tags = (
        f'<link rel="icon" type="image/svg+xml" href="/favicon.svg">\n'
        f'<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png">\n'
        f'<link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png">\n'
        f'<link rel="shortcut icon" href="/favicon.ico">\n'
        f'<link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png">\n'
        f'<link rel="manifest" href="/site.webmanifest">\n'
        f'<meta name="theme-color" content="{NAVY}">\n'
    )
    (favicons_dir / "head-tags.html").write_text(head_tags, encoding="utf-8")

    for is_dark, suffix in ((True, "dark"), (False, "light")):
        bg = NAVY if is_dark else LIGHT_BG
        title_color = WHITE if is_dark else NAVY
        sub_color = GOLD if is_dark else SLATE_TEXT

        subprocess.run(
            [
                MAGICK_BIN,
                "-size",
                "256x256",
                f"xc:{bg}",
                "-draw",
                mvg_gold,
                "-resize",
                "230x230",
                "-gravity",
                "west",
                "-background",
                bg,
                "-extent",
                "1200x300",
                "-fill",
                GOLD,
                "-draw",
                "rectangle 280,50 285,250",
                "-fill",
                title_color,
                "-font",
                "DejaVu-Sans-Bold",
                "-pointsize",
                "64",
                "-gravity",
                "northwest",
                "-annotate",
                "+324+82",
                spec.upper_title,
                "-fill",
                sub_color,
                "-font",
                "DejaVu-Sans",
                "-pointsize",
                "24",
                "-annotate",
                "+328+178",
                spec.subtitle,
                str(banners_dir / f"banner-{suffix}.png"),
            ],
            check=True,
        )

        subprocess.run(
            [
                MAGICK_BIN,
                "-size",
                "256x256",
                f"xc:{bg}",
                "-draw",
                mvg_gold,
                "-resize",
                "380x380",
                "-gravity",
                "west",
                "-background",
                bg,
                "-extent",
                "1280x640",
                "-fill",
                GOLD,
                "-draw",
                "rectangle 420,140 426,500",
                "-fill",
                title_color,
                "-font",
                "DejaVu-Sans-Bold",
                "-pointsize",
                "78",
                "-gravity",
                "northwest",
                "-annotate",
                "+468+210",
                spec.upper_title,
                "-fill",
                sub_color,
                "-font",
                "DejaVu-Sans",
                "-pointsize",
                "28",
                "-annotate",
                "+472+330",
                spec.subtitle,
                str(banners_dir / f"og-social-{suffix}.png"),
            ],
            check=True,
        )


def write_root_readme(root_dir: Path) -> None:
    """Write the Branding Pack documentation and usage guide."""
    rows = "\n".join(
        f"| `{p.slug}` | **{p.title}** | {p.subtitle} | [`{p.slug}/svg/icon-gold.svg`]({p.slug}/svg/icon-gold.svg) |"
        for p in PROJECTS
    )
    readme = f"""# Litestar & Cofin Ecosystem Branding Pack

Unified vector and raster branding pack across all 14 ecosystem packages.

## Brand Specifications

- **Primary Accent (Litestar Gold)**: `#EDB641` (`rgb(237, 182, 65)`)
- **Primary Dark Surface (Litestar Navy)**: `#202235` (`rgb(32, 34, 53)`)
- **Primary Light Surface**: `#F8FAFC` / `#FFFFFF`
- **Vector Grid**: `256x256` (`viewBox="0 0 256 256"`), `stroke-width="14"`, `stroke-linecap="round"`, `stroke-linejoin="round"`

## Included Projects

| Slug | Project | Description | Master Vector |
| :--- | :--- | :--- | :--- |
{rows}

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
  - `icon-gold-{{64,128,256,512,1024}}.png` & `logo-default.png` (`256x256` transparent)
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
  - `site.webmanifest` and `head-tags.html`

## Regenerating the Pack

```bash
uv run python tools/generate_branding_pack.py
```
"""
    (root_dir / "README.md").write_text(readme, encoding="utf-8")


def main() -> None:
    """Build the complete Branding Pack for all 14 projects."""
    target_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else BRANDING_OUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    for spec in PROJECTS:
        generate_project_pack(spec, target_dir)
    write_root_readme(target_dir)
    sys.stdout.write(f"Generated branding pack for {len(PROJECTS)} projects in {target_dir}\n")


if __name__ == "__main__":
    main()
