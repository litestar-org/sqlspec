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
        Error: "#dc2626",
        Keyword: "bold #8839ef",
        Keyword.Constant: "bold #8839ef",
        Keyword.Declaration: "bold #8839ef",
        Keyword.Namespace: "bold #8839ef",
        Keyword.Pseudo: "bold #8839ef",
        Keyword.Reserved: "bold #8839ef",
        Keyword.Type: "bold #7c3aed",
        # SQL-specific keyword tokens
        Keyword.DML: "bold #8839ef",
        Keyword.DDL: "bold #8839ef",
        Keyword.DQL: "bold #8839ef",
        # Names
        Name: "#1e293b",
        Name.Attribute: "#6d4c07",
        Name.Builtin: "#0369a1",
        Name.Builtin.Pseudo: "#0369a1",
        Name.Class: "#d4520c",
        Name.Constant: "#1e293b",
        Name.Decorator: "italic #d4520c",
        Name.Entity: "#1e293b",
        Name.Exception: "#c2410c",
        Name.Function: "#d4520c",
        Name.Function.Magic: "#d4520c",
        Name.Label: "#1e293b",
        Name.Namespace: "#0e7490",
        Name.Other: "#1e293b",
        Name.Property: "#1e293b",
        Name.Tag: "#1e293b",
        Name.Variable: "#202235",
        Name.Variable.Class: "#202235",
        Name.Variable.Global: "#202235",
        Name.Variable.Instance: "#202235",
        Name.Variable.Magic: "#202235",
        # Strings
        String: "#107535",
        String.Affix: "#107535",
        String.Backtick: "#107535",
        String.Char: "#107535",
        String.Delimiter: "#107535",
        String.Doc: "italic #4d7c0f",
        String.Double: "#107535",
        String.Escape: "bold #0d6d6e",
        String.Heredoc: "#107535",
        String.Interpol: "#0d6d6e",
        String.Other: "#107535",
        String.Regex: "#107535",
        String.Single: "#107535",
        String.Symbol: "#107535",
        # Numbers
        Number: "#b45309",
        Number.Bin: "#b45309",
        Number.Float: "#b45309",
        Number.Hex: "#b45309",
        Number.Integer: "#b45309",
        Number.Integer.Long: "#b45309",
        Number.Oct: "#b45309",
        # Comments
        Comment: "italic #6b7280",
        Comment.Hashbang: "italic #6b7280",
        Comment.Multiline: "italic #6b7280",
        Comment.Preproc: "italic #6b7280",
        Comment.PreprocFile: "italic #6b7280",
        Comment.Single: "italic #6b7280",
        Comment.Special: "italic #6b7280",
        # Operators and punctuation
        Operator: "#64748b",
        Operator.Word: "bold #8839ef",
        Punctuation: "#475569",
        # Generic tokens
        Generic.Deleted: "#dc2626",
        Generic.Emph: "italic",
        Generic.Error: "#dc2626",
        Generic.Heading: "bold",
        Generic.Inserted: "#107535",
        Generic.Output: "",
        Generic.Prompt: "bold",
        Generic.Strong: "bold",
        Generic.Subheading: "bold",
        Generic.Traceback: "#dc2626",
    }


class SQLSpecDarkStyle(Style):
    """Dark syntax highlighting style for SQLSpec documentation."""

    name = "sqlspec-dark"
    background_color = "#1a1b2e"

    styles = {
        Token: "#e6edf3",
        Whitespace: "",
        Error: "#ef9a9a",
        Keyword: "bold #cba6f7",
        Keyword.Constant: "bold #cba6f7",
        Keyword.Declaration: "bold #cba6f7",
        Keyword.Namespace: "bold #cba6f7",
        Keyword.Pseudo: "bold #cba6f7",
        Keyword.Reserved: "bold #cba6f7",
        Keyword.Type: "bold #b4befe",
        # SQL-specific keyword tokens
        Keyword.DML: "bold #cba6f7",
        Keyword.DDL: "bold #cba6f7",
        Keyword.DQL: "bold #cba6f7",
        # Names
        Name: "#cdd6f4",
        Name.Attribute: "#f9e2af",
        Name.Builtin: "#89dceb",
        Name.Builtin.Pseudo: "#89dceb",
        Name.Class: "#fab387",
        Name.Constant: "#cdd6f4",
        Name.Decorator: "italic #fab387",
        Name.Entity: "#cdd6f4",
        Name.Exception: "#ef9a9a",
        Name.Function: "#fab387",
        Name.Function.Magic: "#fab387",
        Name.Label: "#cdd6f4",
        Name.Namespace: "#94e2d5",
        Name.Other: "#cdd6f4",
        Name.Property: "#cdd6f4",
        Name.Tag: "#cdd6f4",
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
        String.Doc: "italic #a6e3a1",
        String.Double: "#A5D6A7",
        String.Escape: "bold #94e2d5",
        String.Heredoc: "#A5D6A7",
        String.Interpol: "#94e2d5",
        String.Other: "#A5D6A7",
        String.Regex: "#A5D6A7",
        String.Single: "#A5D6A7",
        String.Symbol: "#A5D6A7",
        # Numbers
        Number: "#fab387",
        Number.Bin: "#fab387",
        Number.Float: "#fab387",
        Number.Hex: "#fab387",
        Number.Integer: "#fab387",
        Number.Integer.Long: "#fab387",
        Number.Oct: "#fab387",
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
        Operator.Word: "bold #cba6f7",
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
