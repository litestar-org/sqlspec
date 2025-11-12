# docs/_ext/playground.py

import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

from docutils import nodes
from docutils.parsers.rst import Directive
from jinja2 import Environment, FileSystemLoader
from typing_extensions import Self

__all__ = ("WasmPlayground", "setup", )


logger = logging.getLogger(__name__)


class WasmPlayground(Directive):
    """
    A custom Sphinx directive to embed a Pyodide-powered code playground.
    """

    logger.info("Initializing WasmPlayground directive")
    has_content = True

    def run(self: Self) -> list[Any]:
        # Generate unique IDs for the HTML elements
        id = uuid4().hex
        env = Environment(loader=FileSystemLoader(Path(__file__).parent))
        template = env.get_template("playground_template.html")
        rendered = template.render(id=id)
        return [nodes.raw(text=rendered, format="html")]


def setup(app: Any) -> dict[str, Any]:
    """
    Register the directive with Sphinx.
    """
    app.add_js_file("https://cdn.jsdelivr.net/pyodide/v0.29.0/full/pyodide.js", priority=100)
    app.add_directive("wasm-playground", WasmPlayground)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}
