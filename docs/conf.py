# Configuration file for the Sphinx documentation builder.
from __future__ import annotations

import os
from datetime import date
from functools import partial
from typing import TYPE_CHECKING

from sqlspec.__metadata__ import __project__, __version__

if TYPE_CHECKING:
    from typing import Any

    from sphinx.addnodes import document  # type: ignore[attr-defined,unused-ignore]
    from sphinx.application import Sphinx

# -- Environmental Data ------------------------------------------------------
__all__ = ("setup", "update_html_context")

current_year = date.today().year


# -- Project information -----------------------------------------------------
project = __project__
copyright = f"{current_year}, Litestar Organization"
author = "Litestar Organization"
release = os.getenv("_SQLSPEC_DOCS_BUILD_VERSION", __version__.rsplit(".")[0])
suppress_warnings = [
    "autosectionlabel.*",
    "ref.python",  # TODO: remove when https://github.com/sphinx-doc/sphinx/issues/4961 is fixed
    "autodoc.import_object",  # Suppress autodoc import warnings for mocked dependencies
    "autodoc",  # Suppress other autodoc warnings
    "myst.xref_missing",  # Suppress missing cross-references in cheat sheets
    "misc.highlighting_failure",  # Suppress pygments highlighting issues in cheat sheets
    "app.add_directive",  # Suppress extension parallel safety warnings
    "docutils",  # Suppress docstring formatting warnings from source code
    "ref.doc",  # Suppress document reference warnings
    "toc.not_readable",  # Suppress cheat sheet files not in toctree warnings
]
# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.githubpages",
    "sphinx.ext.viewcode",
    "tools.sphinx_ext.missing_references",
    "tools.sphinx_ext.changelog",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "auto_pytabs.sphinx_ext",
    "sphinx_copybutton",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_click",
    "sphinx_toolbox.collapse",
    "sphinx_design",
    "sphinx_togglebutton",
    "sphinx_paramlinks",
    "sphinxcontrib.mermaid",
]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "msgspec": ("https://jcristharif.com/msgspec/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
    "alembic": ("https://alembic.sqlalchemy.org/en/latest/", None),
    "litestar": ("https://docs.litestar.dev/latest/", None),
    "click": ("https://click.palletsprojects.com/en/stable/", None),
    "anyio": ("https://anyio.readthedocs.io/en/stable/", None),
    "multidict": ("https://multidict.aio-libs.org/en/stable/", None),
    "cryptography": ("https://cryptography.io/en/latest/", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
    "sanic": ("https://sanic.readthedocs.io/en/latest/", None),
    "flask": ("https://flask.palletsprojects.com/en/stable/", None),
    "typing_extensions": ("https://typing-extensions.readthedocs.io/en/stable/", None),
}
PY_CLASS = "py:class"
PY_EXC = "py:exc"
PY_RE = r"py:.*"
PY_METH = "py:meth"
PY_ATTR = "py:attr"
PY_OBJ = "py:obj"
PY_FUNC = "py:func"

nitpicky = False  # Disable nitpicky mode to reduce warnings
nitpick_ignore: list[str] = []
nitpick_ignore_regex: list[str] = []

auto_pytabs_min_version = (3, 9)
auto_pytabs_max_version = (3, 13)

napoleon_google_docstring = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = False
napoleon_attr_annotations = True

autoclass_content = "class"
autodoc_class_signature = "separated"
autodoc_default_options = {"special-members": "__init__", "show-inheritance": True, "members": True}
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autodoc_warningiserror = False  # Don't treat autodoc warnings as errors
autodoc_type_aliases = {
    "SQLConfig": "sqlspec.base.SQLConfig",
    "SessionProtocol": "sqlspec.protocols.SessionProtocol",
    "DriverProtocol": "sqlspec.protocols.DriverProtocol",
    "StatementProtocol": "sqlspec.protocols.StatementProtocol",
    "ResultProtocol": "sqlspec.protocols.ResultProtocol",
    "ModelT": "sqlspec.typing.ModelT",
    "FilterTypeT": "sqlspec.typing.FilterTypeT",
    "StatementTypeT": "sqlspec.typing.StatementTypeT",
    "Union": "typing.Union",
    "Callable": "typing.Callable",
    "Any": "typing.Any",
    "Optional": "typing.Optional",
}
autodoc_mock_imports = [
    "sqlalchemy",
    "alembic",
    "asyncpg",
    "psycopg",
    "aiomysql",
    "asyncmy",
    "aiosqlite",
    "duckdb",
    "oracledb",
    "psqlpy",
    "adbc_driver_postgresql",
    "adbc_driver_sqlite",
    "adbc_driver_flightsql",
    "google.cloud.bigquery",
]


autosectionlabel_prefix_document = True

# Strip the dollar prompt when copying code
# https://sphinx-copybutton.readthedocs.io/en/latest/use.html#strip-and-configure-input-prompts-for-code-cells
copybutton_prompt_text = "$ "

# -- Style configuration -----------------------------------------------------
html_theme = "shibuya"
html_title = "SQLSpec"
html_short_title = "SQLSpec"
todo_include_todos = True

html_static_path = ["_static"]
html_favicon = "_static/favicon.png"
templates_path = ["_templates"]
html_js_files = ["versioning.js"]
html_css_files = ["custom.css"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "PYPI_README.md",
    "STYLE_GUIDE.md",
    "VOICE_AUDIT_REPORT.md",
]
html_show_sourcelink = True
html_copy_source = True

html_context = {
    "source_type": "github",
    "source_user": "litestar-org",
    "source_repo": "sqlspec",
    "current_version": "latest",
    "version": release,
}


# Mermaid configuration
mermaid_version = "11.2.0"
mermaid_init_js = """
mermaid.initialize({
    startOnLoad: true,
    theme: 'default',
    securityLevel: 'loose',
    flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
    }
});
"""

html_theme_options = {
    "logo_target": "/",
    "accent_color": "amber",
    "github_url": "https://github.com/litestar-org/sqlspec",
    "discord_url": "https://discord.gg/dSDXd4mKhp",
    "navigation_with_keys": True,
    "globaltoc_expand_depth": 0,
    "light_logo": "_static/logo-default.png",
    "dark_logo": "_static/logo-default.png",
    "discussion_url": "https://discord.gg/dSDXd4mKhp",
    "nav_links": [
        {"title": "Home", "url": "index"},
        {"title": "Get Started", "url": "getting_started/index"},
        {"title": "Usage", "url": "usage/index"},
        {"title": "Examples", "url": "examples/index"},
        {"title": "API", "url": "reference/index"},
        {
            "title": "About",
            "children": [
                {"title": "Changelog", "url": "changelog", "summary": "All changes for SQLSpec"},
                {
                    "title": "Litestar Organization",
                    "summary": "Details about the Litestar organization, the team behind SQLSpec",
                    "url": "https://litestar.dev/about/organization",
                    "icon": "org",
                },
                {
                    "title": "Releases",
                    "summary": "Explore the release process, versioning, and deprecation policy for SQLSpec",
                    "url": "releases",
                    "icon": "releases",
                },
                {
                    "title": "Contributing",
                    "summary": "Learn how to contribute to the SQLSpec project",
                    "url": "contributing/index",
                    "icon": "contributing",
                },
                {
                    "title": "Code of Conduct",
                    "summary": "Review the etiquette for interacting with the SQLSpec community",
                    "url": "https://github.com/litestar-org/.github?tab=coc-ov-file",
                    "icon": "coc",
                },
                {
                    "title": "Security",
                    "summary": "Overview of SQLSpec's security protocols",
                    "url": "https://github.com/litestar-org/.github?tab=coc-ov-file#security-ov-file",
                    "icon": "coc",
                },
                {"title": "Sponsor", "url": "https://github.com/sponsors/Litestar-Org", "icon": "heart"},
            ],
        },
        {
            "title": "Help",
            "children": [
                {
                    "title": "Discord Help Forum",
                    "summary": "Dedicated Discord help forum",
                    "url": "https://discord.gg/dSDXd4mKhp",
                    "icon": "coc",
                },
                {
                    "title": "GitHub Discussions",
                    "summary": "GitHub Discussions",
                    "url": "https://github.com/litestar-org/sqlspec/discussions",
                    "icon": "coc",
                },
            ],
        },
    ],
}


def update_html_context(
    _app: Sphinx, _pagename: str, _templatename: str, context: dict[str, Any], _doctree: document
) -> None:
    context["generate_toctree_html"] = partial(context["generate_toctree_html"], startdepth=0)


def setup(app: Sphinx) -> dict[str, bool]:
    app.setup_extension("shibuya")
    return {"parallel_read_safe": True, "parallel_write_safe": True}
