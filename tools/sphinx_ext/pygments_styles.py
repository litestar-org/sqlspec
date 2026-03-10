"""Custom Pygments syntax highlighting styles for SQLSpec documentation.

Provides light and dark themes that match the CodeMirror theme used in the
SQLSpec playground, unifying code block styling across the documentation.
"""

from __future__ import annotations

from pygments.style import Style
from pygments.token import (
    Comment,
    Error,
    Generic,
    Keyword,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Token,
    Whitespace,
)


class SQLSpecLightStyle(Style):
    """Light syntax highlighting style for SQLSpec documentation."""

    name = "sqlspec-light"
    background_color = "#f8f9fa"

    styles = {
        Token: "",
        Whitespace: "",
        Error: "#c62828",
        Keyword: "bold #0369a1",
        Keyword.Constant: "bold #0369a1",
        Keyword.Declaration: "bold #0369a1",
        Keyword.Namespace: "bold #0369a1",
        Keyword.Pseudo: "bold #0369a1",
        Keyword.Reserved: "bold #0369a1",
        Keyword.Type: "bold #0369a1",
        # SQL-specific keyword tokens
        Keyword.DML: "bold #0369a1",
        Keyword.DDL: "bold #0369a1",
        Keyword.DQL: "bold #0369a1",
        # Names
        Name: "#0277BD",
        Name.Attribute: "#5D4037",
        Name.Builtin: "#00838F",
        Name.Builtin.Pseudo: "#00838F",
        Name.Class: "#7B1FA2",
        Name.Constant: "#0277BD",
        Name.Decorator: "#7B1FA2",
        Name.Entity: "#0277BD",
        Name.Exception: "#c62828",
        Name.Function: "#7B1FA2",
        Name.Function.Magic: "#7B1FA2",
        Name.Label: "#0277BD",
        Name.Namespace: "#0277BD",
        Name.Other: "#0277BD",
        Name.Property: "#0277BD",
        Name.Tag: "#0277BD",
        Name.Variable: "#202235",
        Name.Variable.Class: "#202235",
        Name.Variable.Global: "#202235",
        Name.Variable.Instance: "#202235",
        Name.Variable.Magic: "#202235",
        # Strings
        String: "#2E7D32",
        String.Affix: "#2E7D32",
        String.Backtick: "#2E7D32",
        String.Char: "#2E7D32",
        String.Delimiter: "#2E7D32",
        String.Doc: "#2E7D32",
        String.Double: "#2E7D32",
        String.Escape: "#2E7D32",
        String.Heredoc: "#2E7D32",
        String.Interpol: "#2E7D32",
        String.Other: "#2E7D32",
        String.Regex: "#2E7D32",
        String.Single: "#2E7D32",
        String.Symbol: "#2E7D32",
        # Numbers
        Number: "#1565C0",
        Number.Bin: "#1565C0",
        Number.Float: "#1565C0",
        Number.Hex: "#1565C0",
        Number.Integer: "#1565C0",
        Number.Integer.Long: "#1565C0",
        Number.Oct: "#1565C0",
        # Comments
        Comment: "italic #6B7280",
        Comment.Hashbang: "italic #6B7280",
        Comment.Multiline: "italic #6B7280",
        Comment.Preproc: "italic #6B7280",
        Comment.PreprocFile: "italic #6B7280",
        Comment.Single: "italic #6B7280",
        Comment.Special: "italic #6B7280",
        # Operators and punctuation
        Operator: "#546E7A",
        Operator.Word: "#546E7A",
        Punctuation: "#374151",
        # Generic tokens
        Generic.Deleted: "#c62828",
        Generic.Emph: "italic",
        Generic.Error: "#c62828",
        Generic.Heading: "bold",
        Generic.Inserted: "#2E7D32",
        Generic.Output: "",
        Generic.Prompt: "bold",
        Generic.Strong: "bold",
        Generic.Subheading: "bold",
        Generic.Traceback: "#c62828",
    }


class SQLSpecDarkStyle(Style):
    """Dark syntax highlighting style for SQLSpec documentation."""

    name = "sqlspec-dark"
    background_color = "#1a1b2e"

    styles = {
        Token: "#e6edf3",
        Whitespace: "",
        Error: "#ef9a9a",
        Keyword: "bold #7dd3fc",
        Keyword.Constant: "bold #7dd3fc",
        Keyword.Declaration: "bold #7dd3fc",
        Keyword.Namespace: "bold #7dd3fc",
        Keyword.Pseudo: "bold #7dd3fc",
        Keyword.Reserved: "bold #7dd3fc",
        Keyword.Type: "bold #7dd3fc",
        # SQL-specific keyword tokens
        Keyword.DML: "bold #7dd3fc",
        Keyword.DDL: "bold #7dd3fc",
        Keyword.DQL: "bold #7dd3fc",
        # Names
        Name: "#81D4FA",
        Name.Attribute: "#FFCC80",
        Name.Builtin: "#4DD0E1",
        Name.Builtin.Pseudo: "#4DD0E1",
        Name.Class: "#CE93D8",
        Name.Constant: "#81D4FA",
        Name.Decorator: "#CE93D8",
        Name.Entity: "#81D4FA",
        Name.Exception: "#ef9a9a",
        Name.Function: "#CE93D8",
        Name.Function.Magic: "#CE93D8",
        Name.Label: "#81D4FA",
        Name.Namespace: "#81D4FA",
        Name.Other: "#81D4FA",
        Name.Property: "#81D4FA",
        Name.Tag: "#81D4FA",
        Name.Variable: "#e6edf3",
        Name.Variable.Class: "#e6edf3",
        Name.Variable.Global: "#e6edf3",
        Name.Variable.Instance: "#e6edf3",
        Name.Variable.Magic: "#e6edf3",
        # Strings
        String: "#A5D6A7",
        String.Affix: "#A5D6A7",
        String.Backtick: "#A5D6A7",
        String.Char: "#A5D6A7",
        String.Delimiter: "#A5D6A7",
        String.Doc: "#A5D6A7",
        String.Double: "#A5D6A7",
        String.Escape: "#A5D6A7",
        String.Heredoc: "#A5D6A7",
        String.Interpol: "#A5D6A7",
        String.Other: "#A5D6A7",
        String.Regex: "#A5D6A7",
        String.Single: "#A5D6A7",
        String.Symbol: "#A5D6A7",
        # Numbers
        Number: "#90CAF9",
        Number.Bin: "#90CAF9",
        Number.Float: "#90CAF9",
        Number.Hex: "#90CAF9",
        Number.Integer: "#90CAF9",
        Number.Integer.Long: "#90CAF9",
        Number.Oct: "#90CAF9",
        # Comments
        Comment: "italic #9CA3AF",
        Comment.Hashbang: "italic #9CA3AF",
        Comment.Multiline: "italic #9CA3AF",
        Comment.Preproc: "italic #9CA3AF",
        Comment.PreprocFile: "italic #9CA3AF",
        Comment.Single: "italic #9CA3AF",
        Comment.Special: "italic #9CA3AF",
        # Operators and punctuation
        Operator: "#B0BEC5",
        Operator.Word: "#B0BEC5",
        Punctuation: "#9CA3AF",
        # Generic tokens
        Generic.Deleted: "#ef9a9a",
        Generic.Emph: "italic",
        Generic.Error: "#ef9a9a",
        Generic.Heading: "bold",
        Generic.Inserted: "#A5D6A7",
        Generic.Output: "",
        Generic.Prompt: "bold",
        Generic.Strong: "bold",
        Generic.Subheading: "bold",
        Generic.Traceback: "#ef9a9a",
    }
