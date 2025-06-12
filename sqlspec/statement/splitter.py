# ruff: noqa: PLR6301
"""SQL script statement splitter with dialect-aware lexer-driven state machine.

This module provides a robust way to split SQL scripts into individual statements,
handling complex constructs like PL/SQL blocks, T-SQL batches, and nested blocks.
"""

import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Union

from sqlspec.utils.logging import get_logger

__all__ = (
    "DialectConfig",
    "OracleDialectConfig",
    "PostgreSQLDialectConfig",
    "StatementSplitter",
    "TSQLDialectConfig",
    "Token",
    "TokenType",
    "split_sql_script",
)


logger = get_logger(__name__)


class TokenType(Enum):
    """Types of tokens recognized by the SQL lexer."""

    COMMENT_LINE = "COMMENT_LINE"
    COMMENT_BLOCK = "COMMENT_BLOCK"
    STRING_LITERAL = "STRING_LITERAL"
    QUOTED_IDENTIFIER = "QUOTED_IDENTIFIER"
    KEYWORD = "KEYWORD"
    TERMINATOR = "TERMINATOR"
    BATCH_SEPARATOR = "BATCH_SEPARATOR"
    WHITESPACE = "WHITESPACE"
    OTHER = "OTHER"


@dataclass
class Token:
    """Represents a single token in the SQL script."""

    type: TokenType
    value: str
    line: int
    column: int
    position: int  # Absolute position in the script


class DialectConfig(ABC):
    """Abstract base class for SQL dialect configurations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the dialect (e.g., 'oracle', 'tsql')."""

    @property
    @abstractmethod
    def block_starters(self) -> set[str]:
        """Keywords that start a block (e.g., BEGIN, DECLARE)."""

    @property
    @abstractmethod
    def block_enders(self) -> set[str]:
        """Keywords that end a block (e.g., END)."""

    @property
    @abstractmethod
    def statement_terminators(self) -> set[str]:
        """Characters that terminate statements (e.g., ;)."""

    @property
    def batch_separators(self) -> set[str]:
        """Keywords that separate batches (e.g., GO for T-SQL)."""
        return set()

    @property
    def special_terminators(self) -> dict[str, Callable[[list[Token], int], bool]]:
        """Special terminators that need custom handling."""
        return {}

    @property
    def max_nesting_depth(self) -> int:
        """Maximum allowed nesting depth for blocks."""
        return 256

    def get_all_token_patterns(self) -> list[tuple[TokenType, Union[str, Callable]]]:
        """Assembles the complete, ordered list of token regex patterns."""
        # 1. Start with high-precedence patterns
        patterns: list[tuple[TokenType, Union[str, Callable]]] = [
            (TokenType.COMMENT_LINE, r"--[^\n]*"),
            (TokenType.COMMENT_BLOCK, r"/\*[\s\S]*?\*/"),
            (TokenType.STRING_LITERAL, r"'(?:[^']|'')*'"),
            (TokenType.QUOTED_IDENTIFIER, r'"[^"]*"|\[[^\]]*\]'),  # Standard and T-SQL
        ]

        # 2. Add dialect-specific patterns (can be overridden)
        patterns.extend(self._get_dialect_specific_patterns())

        # 3. Dynamically build and insert keyword/separator patterns
        all_keywords = self.block_starters | self.block_enders | self.batch_separators
        if all_keywords:
            # Sort by length descending to match longer keywords first
            sorted_keywords = sorted(all_keywords, key=len, reverse=True)
            patterns.append((TokenType.KEYWORD, r"\b(" + "|".join(re.escape(kw) for kw in sorted_keywords) + r")\b"))

        # 4. Add terminators
        all_terminators = self.statement_terminators | set(self.special_terminators.keys())
        if all_terminators:
            # Escape special regex characters
            patterns.append((TokenType.TERMINATOR, "|".join(re.escape(t) for t in all_terminators)))

        # 5. Add low-precedence patterns
        patterns.extend([
            (TokenType.WHITESPACE, r"\s+"),
            (TokenType.OTHER, r"."),  # Fallback for any other character
        ])

        return patterns

    def _get_dialect_specific_patterns(self) -> list[tuple[TokenType, Union[str, Callable]]]:
        """Override to add dialect-specific token patterns."""
        return []

    def is_real_block_ender(self, tokens: list[Token], current_pos: int) -> bool:
        """Check if this END keyword is actually a block ender.

        Override in dialect configs to handle cases like END IF, END LOOP, etc.
        that are not true block enders.
        """
        return True

    def should_delay_semicolon_termination(self, tokens: list[Token], current_pos: int) -> bool:
        """Check if semicolon termination should be delayed.

        Override in dialect configs to handle special cases like Oracle END; /
        """
        return False


class OracleDialectConfig(DialectConfig):
    """Configuration for Oracle PL/SQL dialect."""

    @property
    def name(self) -> str:
        return "oracle"

    @property
    def block_starters(self) -> set[str]:
        return {"BEGIN", "DECLARE", "CASE"}

    @property
    def block_enders(self) -> set[str]:
        return {"END"}

    @property
    def statement_terminators(self) -> set[str]:
        return {";"}

    @property
    def special_terminators(self) -> dict[str, Callable[[list[Token], int], bool]]:
        return {"/": self._handle_slash_terminator}

    def should_delay_semicolon_termination(self, tokens: list[Token], current_pos: int) -> bool:
        """Check if we should delay semicolon termination to look for a slash.

        In Oracle, after END; we should check if there's a / coming up on its own line.
        """
        # Look backwards to see if we just processed an END token
        pos = current_pos - 1
        while pos >= 0:
            token = tokens[pos]
            if token.type == TokenType.WHITESPACE:
                pos -= 1
                continue
            if token.type == TokenType.KEYWORD and token.value.upper() == "END":
                # We found END just before this semicolon
                # Now look ahead to see if there's a / on its own line
                return self._has_upcoming_slash(tokens, current_pos)
            # Found something else, not an END
            break

        return False

    def _has_upcoming_slash(self, tokens: list[Token], current_pos: int) -> bool:
        """Check if there's a / terminator coming up on its own line."""
        pos = current_pos + 1
        found_newline = False

        while pos < len(tokens):
            token = tokens[pos]
            if token.type == TokenType.WHITESPACE:
                if "\n" in token.value:
                    found_newline = True
                pos += 1
                continue
            if token.type == TokenType.TERMINATOR and token.value == "/":
                # Found a /, check if it's valid (on its own line)
                return found_newline and self._handle_slash_terminator(tokens, pos)
            if token.type in {TokenType.COMMENT_LINE, TokenType.COMMENT_BLOCK}:
                # Skip comments
                pos += 1
                continue
            # Found non-whitespace, non-comment content
            break

        return False

    def is_real_block_ender(self, tokens: list[Token], current_pos: int) -> bool:
        """Check if this END keyword is actually a block ender.

        In Oracle PL/SQL, END followed by IF, LOOP, CASE etc. are not block enders
        for BEGIN blocks - they terminate control structures.
        """
        # Look ahead for the next non-whitespace token(s)
        pos = current_pos + 1
        while pos < len(tokens):
            next_token = tokens[pos]

            if next_token.type == TokenType.WHITESPACE:
                pos += 1
                continue
            if next_token.type == TokenType.OTHER:
                # Collect consecutive OTHER tokens to form a word
                word_chars = []
                word_pos = pos
                while word_pos < len(tokens) and tokens[word_pos].type == TokenType.OTHER:
                    word_chars.append(tokens[word_pos].value)
                    word_pos += 1

                word = "".join(word_chars).upper()
                if word in {"IF", "LOOP", "CASE", "WHILE"}:
                    return False  # This is not a block ender
            # Found a non-whitespace token that's not one of our special cases
            break
        return True  # This is a real block ender

    def _handle_slash_terminator(self, tokens: list[Token], current_pos: int) -> bool:
        """Oracle / must be on its own line after whitespace only."""
        if current_pos == 0:
            return True  # / at start is valid

        # Look backwards to find the start of the line
        pos = current_pos - 1
        while pos >= 0:
            token = tokens[pos]
            if "\n" in token.value:
                # Found newline, check if only whitespace between newline and /
                break
            if token.type not in {TokenType.WHITESPACE, TokenType.COMMENT_LINE}:
                return False  # Non-whitespace before / on same line
            pos -= 1

        return True


class TSQLDialectConfig(DialectConfig):
    """Configuration for T-SQL (SQL Server) dialect."""

    @property
    def name(self) -> str:
        return "tsql"

    @property
    def block_starters(self) -> set[str]:
        return {"BEGIN", "TRY"}

    @property
    def block_enders(self) -> set[str]:
        return {"END", "CATCH"}

    @property
    def statement_terminators(self) -> set[str]:
        return {";"}

    @property
    def batch_separators(self) -> set[str]:
        return {"GO"}

    def validate_batch_separator(self, tokens: list[Token], current_pos: int) -> bool:
        """GO must be the only keyword on its line."""
        # Look for non-whitespace tokens on the same line
        # Implementation similar to Oracle slash handler
        return True  # Simplified for now


class PostgreSQLDialectConfig(DialectConfig):
    """Configuration for PostgreSQL dialect with dollar-quoted strings."""

    @property
    def name(self) -> str:
        return "postgresql"

    @property
    def block_starters(self) -> set[str]:
        return {"BEGIN", "DECLARE", "CASE", "DO"}

    @property
    def block_enders(self) -> set[str]:
        return {"END"}

    @property
    def statement_terminators(self) -> set[str]:
        return {";"}

    def _get_dialect_specific_patterns(self) -> list[tuple[TokenType, Union[str, Callable]]]:
        """Add PostgreSQL-specific patterns like dollar-quoted strings."""
        return [
            (TokenType.STRING_LITERAL, self._handle_dollar_quoted_string),
        ]

    def _handle_dollar_quoted_string(self, text: str, position: int, line: int, column: int) -> Optional[Token]:
        """Handle PostgreSQL dollar-quoted strings like $tag$...$tag$."""
        # Match opening tag
        start_match = re.match(r"\$([a-zA-Z_][a-zA-Z0-9_]*)?\$", text[position:])
        if not start_match:
            return None

        tag = start_match.group(0)  # The full opening tag, e.g., "$tag$"
        content_start = position + len(tag)

        # Find the corresponding closing tag
        try:
            content_end = text.index(tag, content_start)
            full_value = text[position : content_end + len(tag)]

            # Count newlines to update line number
            _newlines_in_string = full_value.count("\n")

            return Token(type=TokenType.STRING_LITERAL, value=full_value, line=line, column=column, position=position)
        except ValueError:
            # Closing tag not found
            return None


class StatementSplitter:
    """Splits SQL scripts into individual statements using a lexer-driven state machine."""

    def __init__(self, dialect: DialectConfig, strip_trailing_semicolon: bool = False) -> None:
        """Initialize the splitter with a specific dialect configuration.

        Args:
            dialect: The dialect configuration to use
            strip_trailing_semicolon: If True, remove trailing semicolons from statements
        """
        self.dialect = dialect
        self.strip_trailing_semicolon = strip_trailing_semicolon
        self.token_patterns = dialect.get_all_token_patterns()
        self._compiled_patterns = self._compile_patterns()

    def _compile_patterns(self) -> list[tuple[TokenType, Union[re.Pattern, Callable]]]:
        """Compile regex patterns for efficiency."""
        compiled = []
        for token_type, pattern in self.token_patterns:
            if isinstance(pattern, str):
                compiled.append((token_type, re.compile(pattern, re.IGNORECASE | re.DOTALL)))
            else:
                # It's a callable
                compiled.append((token_type, pattern))
        return compiled

    def _tokenize(self, sql: str) -> Generator[Token, None, None]:
        """Tokenize the SQL script into a stream of tokens.

        sql: The SQL script to tokenize

        Yields:
            Token objects representing the recognized tokens in the script.

        """
        pos = 0
        line = 1
        line_start = 0

        while pos < len(sql):
            matched = False

            for token_type, pattern in self._compiled_patterns:
                if callable(pattern):
                    # Call the handler function
                    column = pos - line_start + 1
                    token = pattern(sql, pos, line, column)
                    if token:
                        # Update position tracking
                        newlines = token.value.count("\n")
                        if newlines > 0:
                            line += newlines
                            last_newline = token.value.rfind("\n")
                            line_start = pos + last_newline + 1

                        yield token
                        pos += len(token.value)
                        matched = True
                        break
                else:
                    # Use regex
                    match = pattern.match(sql, pos)
                    if match:
                        value = match.group(0)
                        column = pos - line_start + 1

                        # Update line tracking
                        newlines = value.count("\n")
                        if newlines > 0:
                            line += newlines
                            last_newline = value.rfind("\n")
                            line_start = pos + last_newline + 1

                        yield Token(type=token_type, value=value, line=line, column=column, position=pos)
                        pos = match.end()
                        matched = True
                        break

            if not matched:
                # This should never happen with our catch-all OTHER pattern
                logger.error("Failed to tokenize at position %d: %s", pos, sql[pos : pos + 20])
                pos += 1  # Skip the problematic character

    def split(self, sql: str) -> list[str]:
        """Split the SQL script into individual statements."""
        statements = []
        current_statement_tokens = []
        current_statement_chars = []
        block_stack = []

        # Convert token generator to list so we can look ahead
        all_tokens = list(self._tokenize(sql))

        for token_idx, token in enumerate(all_tokens):
            # Always accumulate the original text
            current_statement_chars.append(token.value)

            # Skip whitespace and comments for logic (but keep in output)
            if token.type in {TokenType.WHITESPACE, TokenType.COMMENT_LINE, TokenType.COMMENT_BLOCK}:
                current_statement_tokens.append(token)
                continue

            current_statement_tokens.append(token)
            token_upper = token.value.upper()

            # Update block nesting
            if token.type == TokenType.KEYWORD:
                if token_upper in self.dialect.block_starters:
                    block_stack.append(token_upper)
                    if len(block_stack) > self.dialect.max_nesting_depth:
                        msg = f"Maximum nesting depth ({self.dialect.max_nesting_depth}) exceeded"
                        raise ValueError(msg)
                elif token_upper in self.dialect.block_enders:
                    # Check if this is actually a block ender (not END IF, END LOOP, etc.)
                    if block_stack and self.dialect.is_real_block_ender(all_tokens, token_idx):
                        block_stack.pop()

            # Check for statement termination
            is_terminator = False
            if not block_stack:  # Only terminate when not inside a block
                if token.type == TokenType.TERMINATOR:
                    if token.value in self.dialect.statement_terminators:
                        # Check if we should delay this termination (e.g., for Oracle END; /)
                        should_delay = self.dialect.should_delay_semicolon_termination(all_tokens, token_idx)

                        # Also check if there's a batch separator coming up (for T-SQL GO)
                        if not should_delay and token.value == ";" and self.dialect.batch_separators:
                            # In dialects with batch separators, semicolons don't terminate
                            # statements - only batch separators do
                            should_delay = True

                        if not should_delay:
                            is_terminator = True
                    elif token.value in self.dialect.special_terminators:
                        # Call the handler to validate
                        handler = self.dialect.special_terminators[token.value]
                        if handler(all_tokens, token_idx):
                            is_terminator = True

                elif token.type == TokenType.KEYWORD and token_upper in self.dialect.batch_separators:
                    # Batch separators like GO should be included with the preceding statement
                    is_terminator = True

            if is_terminator:
                # Save the statement
                statement = "".join(current_statement_chars).strip()

                # Optionally strip the trailing terminator
                if (
                    self.strip_trailing_semicolon
                    and token.type == TokenType.TERMINATOR
                    and statement.endswith(token.value)
                ):
                    statement = statement[: -len(token.value)].rstrip()

                if statement and self._contains_executable_content(statement):
                    statements.append(statement)
                current_statement_tokens = []
                current_statement_chars = []

        # Handle any remaining content
        if current_statement_chars:
            statement = "".join(current_statement_chars).strip()
            if statement and self._contains_executable_content(statement):
                statements.append(statement)

        return statements

    def _contains_executable_content(self, statement: str) -> bool:
        """Check if a statement contains actual executable content (not just comments/whitespace).

        Args:
            statement: The statement string to check

        Returns:
            True if the statement contains executable SQL, False if it's only comments/whitespace
        """
        # Tokenize the statement to check its content
        tokens = list(self._tokenize(statement))

        # Check if there are any non-comment, non-whitespace tokens
        for token in tokens:
            if token.type not in {TokenType.WHITESPACE, TokenType.COMMENT_LINE, TokenType.COMMENT_BLOCK}:
                return True

        return False


def split_sql_script(script: str, dialect: str = "generic", strip_trailing_semicolon: bool = False) -> list[str]:
    """Split a SQL script into statements using the appropriate dialect.

    Args:
        script: The SQL script to split
        dialect: The SQL dialect name ('oracle', 'tsql', 'postgresql', etc.)
        strip_trailing_semicolon: If True, remove trailing terminators from statements

    Returns:
        List of individual SQL statements
    """
    dialect_configs = {
        "oracle": OracleDialectConfig(),
        "tsql": TSQLDialectConfig(),
        "postgresql": PostgreSQLDialectConfig(),
    }

    config = dialect_configs.get(dialect.lower())
    if not config:
        # Fall back to a generic config (could be implemented)
        msg = f"Unsupported dialect: {dialect}"
        raise ValueError(msg)

    splitter = StatementSplitter(config, strip_trailing_semicolon=strip_trailing_semicolon)
    return splitter.split(script)
