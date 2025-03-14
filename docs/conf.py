# Configuration file for the Sphinx documentation builder.
from __future__ import annotations

import os

from sqlspec.__metadata__ import __project__ as project
from sqlspec.__metadata__ import __version__ as version

# -- Environmental Data ------------------------------------------------------


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
}
PY_CLASS = "py:class"
PY_RE = r"py:.*"
PY_METH = "py:meth"
PY_ATTR = "py:attr"
PY_OBJ = "py:obj"

nitpicky = True
nitpick_ignore: list[str] = []
nitpick_ignore_regex = [
    (PY_RE, r"sqlspec.*\.T"),
]

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

autosectionlabel_prefix_document = True

todo_include_todos = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Style configuration -----------------------------------------------------
html_theme = "litestar_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["style.css"]
html_show_sourcelink = False
html_title = "SQLSpec"
# html_favicon = "_static/logo.png"
# html_logo = "_static/logo.png"
html_context = {
    "source_type": "github",
    "source_user": "cofin",
    "source_repo": project.replace("_", "-"),
}

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
    # "logo_target": "/",
    "announcement": "This documentation is currently under development.",
    # "logo": {
    #     "link": "https://litestar.dev",
    # },
    "github_repo_name": "SQLSpec",
    "github_url": "https://github.com/litestar-org/sqlspec",
    # "pygment_light_style": "xcode",
    # "pygment_dark_style": "lightbulb",
    # "nav_links": [
    #     {"title": "Home", "url": "https://cofin.github.io/sqlspec/"},
    #     {"title": "Docs", "url": "https://cofin.github.io/sqlspec/latest/"},
    #     {"title": "Code", "url": "https://github.com/cofin/sqlspec"},
    # ],
    # "light_css_variables": {
    #     # RGB
    #     "--sy-rc-theme": brand_colors["--brand-primary"]["rgb"],
    #     "--sy-rc-text": brand_colors["--brand-primary"]["rgb"],
    #     "--sy-rc-invert": brand_colors["--brand-primary"]["rgb"],
    #     # "--sy-rc-bg": brand_colors["--brand-secondary"]["rgb"],
    #     # Hex
    #     "--sy-c-link": brand_colors["--brand-secondary"]["hex"],
    #     # "--sy-c-foot-bg": "#191919",
    #     "--sy-c-foot-divider": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-foot-text": brand_colors["--brand-dark"]["hex"],
    #     "--sy-c-bold": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-heading": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-text-weak": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-text": brand_colors["--brand-dark"]["hex"],
    #     "--sy-c-bg-weak": brand_colors["--brand-dark"]["rgb"],
    # },
    # "dark_css_variables": {
    #     # RGB
    #     "--sy-rc-theme": brand_colors["--brand-primary"]["rgb"],
    #     "--sy-rc-text": brand_colors["--brand-primary"]["rgb"],
    #     "--sy-rc-invert": brand_colors["--brand-primary"]["rgb"],
    #     "--sy-rc-bg": brand_colors["--brand-dark"]["rgb"],
    #     # Hex
    #     "--sy-c-link": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-foot-bg": brand_colors["--brand-dark"]["hex"],
    #     "--sy-c-foot-divider": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-foot-text": brand_colors["--brand-light"]["hex"],
    #     "--sy-c-bold": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-heading": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-text-weak": brand_colors["--brand-primary"]["hex"],
    #     "--sy-c-text": brand_colors["--brand-light"]["hex"],
    #     "--sy-c-bg-weak": brand_colors["--brand-dark"]["hex"],
    #     "--sy-c-bg": brand_colors["--brand-primary"]["hex"],
    # },
}
