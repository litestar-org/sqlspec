"""Sphinx directive for the SQLSpec Pyodide playground."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

from docutils import nodes
from docutils.parsers.rst import Directive
from jinja2 import Environment, FileSystemLoader, select_autoescape
from typing_extensions import Self

__all__ = ("WasmPlayground", "setup")

logger = logging.getLogger(__name__)
TEMPLATE_NAME = "playground_template.html"


class WasmPlayground(Directive):
    """Embed a Pyodide-powered playground in the docs."""

    has_content = False

    def run(self: Self) -> list[nodes.Node]:
        playground_id = uuid4().hex
        env = Environment(loader=FileSystemLoader(Path(__file__).parent), autoescape=select_autoescape(["html", "xml"]))
        template = env.get_template(TEMPLATE_NAME)
        rendered = template.render(id=playground_id)
        return [nodes.raw(text=rendered, format="html")]


def setup(app: Any) -> dict[str, Any]:
    """Register the Wasm playground directive."""

    app.add_directive("wasm-playground", WasmPlayground)
    return {"version": "1.1", "parallel_read_safe": True, "parallel_write_safe": True}
