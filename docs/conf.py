# Configuration file for the Sphinx documentation builder.
from __future__ import annotations

import os
from functools import partial
from typing import TYPE_CHECKING

from sqlspec.__metadata__ import __project__ as project
from sqlspec.__metadata__ import __version__ as version

if TYPE_CHECKING:
    from typing import Any

    from sphinx.addnodes import document  # type: ignore[attr-defined,unused-ignore]
    from sphinx.application import Sphinx
# -- Environmental Data ------------------------------------------------------
__all__ = ("setup", "update_html_context")


# -- Project information -----------------------------------------------------
project = project
copyright = "2023, Litestar-Org"
author = "Litestar-Org"
release = os.getenv("_SQLSPEC_DOCS_BUILD_VERSION", version.rsplit(".")[0])
# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.githubpages",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "auto_pytabs.sphinx_ext",
    "tools.sphinx_ext",
    "sphinx_copybutton",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_click",
    "sphinx_toolbox.collapse",
    "sphinx_design",
]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "msgspec": ("https://jcristharif.com/msgspec/", None),
    "anyio": ("https://anyio.readthedocs.io/en/stable/", None),
    "click": ("https://click.palletsprojects.com/en/8.1.x/", None),
    "litestar": ("https://docs.litestar.dev/latest/", None),
    "multidict": ("https://multidict.aio-libs.org/en/stable/", None),
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

nitpicky = True
nitpick_ignore: list[str] = []
nitpick_ignore_regex = [(PY_RE, r"sqlspec.*\.T")]

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
autodoc_type_aliases: dict[str, str] = {}
autodoc_mock_imports: list[str] = []

autosectionlabel_prefix_document = True

todo_include_todos = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Strip the dollar prompt when copying code
# https://sphinx-copybutton.readthedocs.io/en/latest/use.html#strip-and-configure-input-prompts-for-code-cells
copybutton_prompt_text = "$ "

# -- Style configuration -----------------------------------------------------
html_theme = "shibuya"
html_static_path = ["_static"]
html_favicon = "_static/favicon.png"
templates_path = ["_templates"]
html_js_files = ["versioning.js"]
html_css_files = ["custom.css"]
html_show_sourcelink = False
html_title = "SQLSpec"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "PYPI_README.md"]
html_show_sourcelink = True
html_copy_source = True
html_context = {"source_type": "github", "source_user": "litestar-org", "source_repo": project.replace("_", "-")}

brand_colors = {
    "--brand-primary": {"rgb": "245, 0, 87", "hex": "#f50057"},
    "--brand-secondary": {"rgb": "32, 32, 32", "hex": "#202020"},
    "--brand-tertiary": {"rgb": "161, 173, 161", "hex": "#A1ADA1"},
    "--brand-green": {"rgb": "0, 245, 151", "hex": "#00f597"},
    "--brand-alert": {"rgb": "243, 96, 96", "hex": "#f36060"},
    "--brand-dark": {"rgb": "0, 0, 0", "hex": "#000000"},
    "--brand-light": {"rgb": "235, 221, 221", "hex": "#ebdddd"},
}

html_theme_options = {
    "logo_target": "/",
    "accent_color": "amber",
    "github_url": "https://github.com/litestar-org/sqlspec",
    "discord_url": "https://discord.gg/dSDXd4mKhp",
    "navigation_with_keys": True,
    "globaltoc_expand_depth": 2,
    "light_logo": "_static/logo-default.png",
    "dark_logo": "_static/logo-default.png",
    "discussion_url": "https://discord.gg/dSDXd4mKhp",
    "nav_links": [
        {"title": "Home", "url": "index"},
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
                    "url": "contribution-guide",
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
                {"title": "Sponsor", "url": "https://github.com/sponsors/Litestar-org", "icon": "heart"},
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
