"""Security validator for SQL statements."""

import contextlib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.expressions import EQ, Binary, Func, Literal, Or, Subquery, Union

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import ValidationError

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("SecurityIssue", "SecurityIssueType", "SecurityValidator", "SecurityValidatorConfig")

# Constants for magic values
MAX_FUNCTION_ARGS = 10
MAX_NESTING_LEVELS = 5
MIN_UNION_COUNT_FOR_INJECTION = 2

logger = logging.getLogger(__name__)

# Constants
SUSPICIOUS_FUNC_THRESHOLD = 2


class SecurityIssueType(Enum):
    """Types of security issues that can be detected."""

    INJECTION = auto()
    TAUTOLOGY = auto()
    SUSPICIOUS_KEYWORD = auto()
    COMBINED_ATTACK = auto()
    AST_ANOMALY = auto()  # New: AST-based detection
    STRUCTURAL_ATTACK = auto()  # New: Structural analysis


@dataclass
class SecurityIssue:
    """Represents a detected security issue in SQL."""

    issue_type: "SecurityIssueType"
    risk_level: "RiskLevel"
    description: str
    location: Optional[str] = None
    pattern_matched: Optional[str] = None
    recommendation: Optional[str] = None
    metadata: "dict[str, Any]" = field(default_factory=dict)
    ast_node_type: Optional[str] = None  # New: AST node type for AST-based detection
    confidence: float = 1.0  # New: Confidence level (0.0 to 1.0)


@dataclass
class SecurityValidatorConfig:
    """Configuration for the unified security validator."""

    # Feature toggles
    check_injection: bool = True
    check_tautology: bool = True
    check_keywords: bool = True
    check_combined_patterns: bool = True
    check_ast_anomalies: bool = True  # New: AST-based anomaly detection
    check_structural_attacks: bool = True  # New: Structural attack detection

    # Risk levels
    default_risk_level: "RiskLevel" = RiskLevel.HIGH
    injection_risk_level: "RiskLevel" = RiskLevel.HIGH
    tautology_risk_level: "RiskLevel" = RiskLevel.MEDIUM
    keyword_risk_level: "RiskLevel" = RiskLevel.MEDIUM
    ast_anomaly_risk_level: "RiskLevel" = RiskLevel.MEDIUM

    # Thresholds
    max_union_count: int = 3
    max_null_padding: int = 5
    max_system_tables: int = 2
    max_nesting_depth: int = 5  # New: Maximum nesting depth
    max_literal_length: int = 1000  # New: Maximum literal length
    min_confidence_threshold: float = 0.7  # New: Minimum confidence for reporting

    # Allowed/blocked lists
    allowed_functions: "list[str]" = field(default_factory=list)
    blocked_functions: "list[str]" = field(default_factory=list)
    allowed_system_schemas: "list[str]" = field(default_factory=list)

    # Custom patterns (legacy support)
    custom_injection_patterns: "list[str]" = field(default_factory=list)
    custom_suspicious_patterns: "list[str]" = field(default_factory=list)


# Common regex patterns used across security checks
PATTERNS = {
    # Injection patterns
    "union_null": re.compile(r"UNION\s+(?:ALL\s+)?SELECT\s+(?:NULL(?:\s*,\s*NULL)*)", re.IGNORECASE),
    "comment_evasion": re.compile(r"/\*.*?\*/|--.*?$|#.*?$", re.MULTILINE),
    "encoded_chars": re.compile(r"(?:CHAR|CHR)\s*\([0-9]+\)", re.IGNORECASE),
    "hex_encoding": re.compile(r"0x[0-9a-fA-F]+"),
    "concat_evasion": re.compile(r"(?:CONCAT|CONCAT_WS|\|\|)\s*\([^)]+\)", re.IGNORECASE),
    # Tautology patterns
    "always_true": re.compile(r"(?:1\s*=\s*1|'1'\s*=\s*'1'|true|TRUE)\s*(?:OR|AND)?", re.IGNORECASE),
    "or_patterns": re.compile(r"\bOR\s+1\s*=\s*1\b", re.IGNORECASE),
    # Suspicious function patterns
    "file_operations": re.compile(r"\b(?:LOAD_FILE|INTO\s+(?:OUTFILE|DUMPFILE))\b", re.IGNORECASE),
    "exec_functions": re.compile(r"\b(?:EXEC|EXECUTE|xp_cmdshell|sp_executesql)\b", re.IGNORECASE),
    "admin_functions": re.compile(r"\b(?:CREATE\s+USER|DROP\s+USER|GRANT|REVOKE)\b", re.IGNORECASE),
}

# System schemas that are often targeted in attacks
SYSTEM_SCHEMAS = {
    "mysql": ["information_schema", "mysql", "performance_schema", "sys"],
    "postgresql": ["information_schema", "pg_catalog", "pg_temp"],
    "mssql": ["information_schema", "sys", "master", "msdb"],
    "oracle": ["sys", "system", "dba_", "all_", "user_"],
}

# Functions commonly used in SQL injection attacks
SUSPICIOUS_FUNCTIONS = [
    # String manipulation
    "concat",
    "concat_ws",
    "substring",
    "substr",
    "char",
    "chr",
    "ascii",
    "hex",
    "unhex",
    # File operations
    "load_file",
    "outfile",
    "dumpfile",
    # System information
    "database",
    "version",
    "user",
    "current_user",
    "system_user",
    "session_user",
    # Time-based
    "sleep",
    "benchmark",
    "pg_sleep",
    "waitfor",
    # Execution
    "exec",
    "execute",
    "xp_cmdshell",
    "sp_executesql",
    # XML/JSON (for data extraction)
    "extractvalue",
    "updatexml",
    "xmltype",
    "json_extract",
]


class SecurityValidator(ProcessorProtocol):
    """Unified security validator that performs comprehensive security checks in a single pass."""

    def __init__(self, config: Optional["SecurityValidatorConfig"] = None, **kwargs: Any) -> None:
        """Initialize the security validator with configuration."""
        self.config = config or SecurityValidatorConfig()
        self._compiled_patterns: dict[str, re.Pattern[str]] = {}
        self._compile_custom_patterns()

    def _compile_custom_patterns(self) -> None:
        """Compile custom regex patterns from configuration."""
        for i, pattern in enumerate(self.config.custom_injection_patterns):
            with contextlib.suppress(re.error):
                self._compiled_patterns[f"custom_injection_{i}"] = re.compile(pattern, re.IGNORECASE)

        for i, pattern in enumerate(self.config.custom_suspicious_patterns):
            with contextlib.suppress(re.error):
                self._compiled_patterns[f"custom_suspicious_{i}"] = re.compile(pattern, re.IGNORECASE)

    def process(self, expression: Optional[exp.Expression], context: "SQLProcessingContext") -> None:
        """Process the SQL expression and detect security issues in a single pass."""
        if not context.current_expression:
            return

        security_issues: list[SecurityIssue] = []
        visited_nodes: set[int] = set()

        # Single AST traversal for all security checks
        nesting_depth = 0
        for node in context.current_expression.walk():
            node_id = id(node)
            if node_id in visited_nodes:
                continue
            visited_nodes.add(node_id)

            # Track nesting depth
            if isinstance(node, (Subquery, exp.Select)):
                nesting_depth += 1

            # Check injection patterns (enhanced AST-based)
            if self.config.check_injection:
                injection_issues = self._check_injection_patterns(node, context)
                security_issues.extend(injection_issues)

            # Check tautology conditions (enhanced)
            if self.config.check_tautology:
                tautology_issues = self._check_tautology_patterns(node, context)
                security_issues.extend(tautology_issues)

            # Check suspicious keywords/functions
            if self.config.check_keywords:
                keyword_issues = self._check_suspicious_keywords(node, context)
                security_issues.extend(keyword_issues)

            # New: Check AST anomalies
            if self.config.check_ast_anomalies:
                anomaly_issues = self._check_ast_anomalies(node, context, nesting_depth)
                security_issues.extend(anomaly_issues)

            # New: Check structural attacks
            if self.config.check_structural_attacks:
                structural_issues = self._check_structural_attacks(node, context)
                security_issues.extend(structural_issues)

        # Check combined attack patterns
        if self.config.check_combined_patterns and security_issues:
            combined_issues = self._check_combined_patterns(context.current_expression, security_issues)
            security_issues.extend(combined_issues)

        # Also check the initial SQL string for custom patterns (handles unparsed parts)
        if self.config.check_injection and context.initial_sql_string:
            for name, pattern in self._compiled_patterns.items():
                if name.startswith("custom_injection_") and pattern.search(context.initial_sql_string):
                    security_issues.append(
                        SecurityIssue(
                            issue_type=SecurityIssueType.INJECTION,
                            risk_level=self.config.injection_risk_level,
                            description=f"Custom injection pattern matched: {name}",
                            location=context.initial_sql_string[:100],
                            pattern_matched=name,
                        )
                    )

        # Determine overall risk level
        if security_issues:
            max(issue.risk_level for issue in security_issues)

        # Create validation errors
        for issue in security_issues:
            error = ValidationError(
                message=issue.description,
                code="security-issue",
                risk_level=issue.risk_level,
                processor="SecurityValidator",
                expression=expression,
            )
            context.validation_errors.append(error)

        # Store metadata in context for access by caller
        context.metadata["security_validator"] = {
            "security_issues": security_issues,
            "checks_performed": [
                "injection" if self.config.check_injection else None,
                "tautology" if self.config.check_tautology else None,
                "keywords" if self.config.check_keywords else None,
                "combined" if self.config.check_combined_patterns else None,
            ],
            "total_issues": len(security_issues),
            "issue_breakdown": {
                issue_type.name: sum(1 for issue in security_issues if issue.issue_type == issue_type)
                for issue_type in SecurityIssueType
            },
        }

        # Filter issues by confidence threshold
        filtered_issues = [
            issue for issue in security_issues if issue.confidence >= self.config.min_confidence_threshold
        ]

        # Update validation result with filtered issues
        if filtered_issues != security_issues:
            # Clear previous errors and add filtered ones
            context.validation_errors = []
            for issue in filtered_issues:
                error = ValidationError(
                    message=issue.description,
                    code="security-issue",
                    risk_level=issue.risk_level,
                    processor="SecurityValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)

            # Update metadata with filtered issues
            context.metadata["security_validator"] = {
                "security_issues": filtered_issues,
                "total_issues_found": len(security_issues),
                "issues_after_confidence_filter": len(filtered_issues),
                "confidence_threshold": self.config.min_confidence_threshold,
                "checks_performed": [
                    "injection" if self.config.check_injection else None,
                    "tautology" if self.config.check_tautology else None,
                    "keywords" if self.config.check_keywords else None,
                    "combined" if self.config.check_combined_patterns else None,
                    "ast_anomalies" if self.config.check_ast_anomalies else None,
                    "structural" if self.config.check_structural_attacks else None,
                ],
                "issue_breakdown": {
                    issue_type.name: sum(1 for issue in filtered_issues if issue.issue_type == issue_type)
                    for issue_type in SecurityIssueType
                },
            }

    def _check_injection_patterns(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for SQL injection patterns in the node."""
        issues: list[SecurityIssue] = []

        # Check UNION-based injection
        if isinstance(node, exp.Union):
            union_issues = self._check_union_injection(node, context)
            issues.extend(union_issues)

        sql_text = node.sql()
        if PATTERNS["comment_evasion"].search(sql_text):
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.INJECTION,
                    risk_level=self.config.injection_risk_level,
                    description="Comment-based SQL injection attempt detected",
                    location=sql_text[:100],
                    pattern_matched="comment_evasion",
                    recommendation="Remove or sanitize SQL comments",
                )
            )

        # Check for encoded characters
        if PATTERNS["encoded_chars"].search(sql_text) or PATTERNS["hex_encoding"].search(sql_text):
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.INJECTION,
                    risk_level=self.config.injection_risk_level,
                    description="Encoded character evasion detected",
                    location=sql_text[:100],
                    pattern_matched="encoding_evasion",
                    recommendation="Validate and decode input properly",
                )
            )

        # Check for system schema access
        if isinstance(node, exp.Table):
            system_access = self._check_system_schema_access(node)
            if system_access:
                issues.append(system_access)

        for name, pattern in self._compiled_patterns.items():
            if name.startswith("custom_injection_") and pattern.search(sql_text):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.INJECTION,
                        risk_level=self.config.injection_risk_level,
                        description=f"Custom injection pattern matched: {name}",
                        location=sql_text[:100],
                        pattern_matched=name,
                    )
                )

        return issues

    def _check_union_injection(self, union_node: "exp.Union", context: "SQLProcessingContext") -> "list[SecurityIssue]":
        """Check for UNION-based SQL injection patterns."""
        issues: list[SecurityIssue] = []

        # Count UNIONs in the query
        if context.current_expression:
            union_count = len(list(context.current_expression.find_all(exp.Union)))
        else:
            return []
        if union_count > self.config.max_union_count:
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.INJECTION,
                    risk_level=self.config.injection_risk_level,
                    description=f"Excessive UNION operations detected ({union_count})",
                    location=union_node.sql()[:100],
                    pattern_matched="excessive_unions",
                    recommendation="Limit the number of UNION operations",
                    metadata={"union_count": union_count},
                )
            )

        # Check for NULL padding in UNION SELECT
        if hasattr(union_node, "right") and isinstance(union_node.right, exp.Select):
            select_expr = union_node.right
            if select_expr.expressions:
                null_count = sum(1 for expr in select_expr.expressions if isinstance(expr, exp.Null))
                if null_count > self.config.max_null_padding:
                    issues.append(
                        SecurityIssue(
                            issue_type=SecurityIssueType.INJECTION,
                            risk_level=self.config.injection_risk_level,
                            description=f"UNION with excessive NULL padding ({null_count} NULLs)",
                            location=union_node.sql()[:100],
                            pattern_matched="union_null_padding",
                            recommendation="Validate UNION queries for proper column matching",
                            metadata={"null_count": null_count},
                        )
                    )

        return issues

    def _check_system_schema_access(self, table_node: "exp.Table") -> Optional["SecurityIssue"]:
        """Check if a table reference is accessing system schemas."""
        table_name = table_node.name.lower() if table_node.name else ""
        schema_name = table_node.db.lower() if table_node.db else ""
        table_node.catalog.lower() if table_node.catalog else ""

        # Check if schema is in allowed list
        if schema_name in self.config.allowed_system_schemas:
            return None

        # Check against known system schemas
        for db_type, schemas in SYSTEM_SCHEMAS.items():
            if schema_name in schemas or any(schema in table_name for schema in schemas):
                return SecurityIssue(
                    issue_type=SecurityIssueType.INJECTION,
                    risk_level=self.config.injection_risk_level,
                    description=f"Access to system schema detected: {schema_name or table_name}",
                    location=table_node.sql(),
                    pattern_matched="system_schema_access",
                    recommendation="Restrict access to system schemas",
                    metadata={"database_type": db_type, "schema": schema_name, "table": table_name},
                )

        return None

    def _check_tautology_patterns(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for tautology conditions that are always true."""
        issues: list[SecurityIssue] = []

        # Check for boolean literals in WHERE conditions
        if isinstance(node, exp.Boolean) and node.this is True:
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.TAUTOLOGY,
                    risk_level=self.config.tautology_risk_level,
                    description="Tautology: always-true literal condition detected",
                    location=node.sql(),
                    pattern_matched="always-true",
                    recommendation="Remove always-true conditions from WHERE clause",
                )
            )

        # Check for tautological conditions
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)) and self._is_tautology(node):
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.TAUTOLOGY,
                    risk_level=self.config.tautology_risk_level,
                    description="Tautology: always-true condition detected",
                    location=node.sql(),
                    pattern_matched="tautology_condition",
                    recommendation="Review WHERE conditions for always-true statements",
                )
            )

        # Check for OR 1=1 patterns
        if isinstance(node, exp.Or):
            or_sql = node.sql()
            if PATTERNS["or_patterns"].search(or_sql) or PATTERNS["always_true"].search(or_sql):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.TAUTOLOGY,
                        risk_level=self.config.tautology_risk_level,
                        description="OR with always-true condition detected",
                        location=or_sql[:100],
                        pattern_matched="or_tautology",
                        recommendation="Validate OR conditions in WHERE clauses",
                    )
                )

        return issues

    def _is_tautology(self, comparison: "exp.Expression") -> bool:
        """Check if a comparison is a tautology."""
        if not isinstance(comparison, exp.Binary):
            return False

        # In sqlglot, binary expressions use 'this' and 'expression' for operands
        left = comparison.this
        right = comparison.expression

        # Check if comparing identical expressions
        if self._expressions_identical(left, right):
            if isinstance(comparison, (exp.EQ, exp.GTE, exp.LTE)):
                return True
            if isinstance(comparison, (exp.NEQ, exp.GT, exp.LT)):
                return False

        # Check for literal comparisons
        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            try:
                left_val = left.this
                right_val = right.this

                if isinstance(comparison, exp.EQ):
                    return bool(left_val == right_val)
                if isinstance(comparison, exp.NEQ):
                    return bool(left_val != right_val)
                # Add more comparison logic as needed
            except Exception:
                # Value extraction failed, can't evaluate the condition
                logger.debug("Failed to extract values for comparison evaluation")

        return False

    @staticmethod
    def _expressions_identical(expr1: "exp.Expression", expr2: "exp.Expression") -> bool:
        """Check if two expressions are structurally identical."""
        if type(expr1) is not type(expr2):
            return False

        if isinstance(expr1, exp.Column) and isinstance(expr2, exp.Column):
            return expr1.name == expr2.name and expr1.table == expr2.table

        if isinstance(expr1, exp.Literal) and isinstance(expr2, exp.Literal):
            return bool(expr1.this == expr2.this)

        # For other expressions, compare their SQL representations
        return expr1.sql() == expr2.sql()

    def _check_suspicious_keywords(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for suspicious functions and keywords."""
        issues: list[SecurityIssue] = []

        # Check function calls
        if isinstance(node, exp.Func):
            func_name = node.name.lower() if node.name else ""

            # Check if function is explicitly blocked
            if func_name in self.config.blocked_functions:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=RiskLevel.HIGH,
                        description=f"Blocked function used: {func_name}",
                        location=node.sql()[:100],
                        pattern_matched="blocked_function",
                        recommendation=f"Function {func_name} is not allowed",
                    )
                )
            # Check if function is suspicious but not explicitly allowed
            elif func_name in SUSPICIOUS_FUNCTIONS and func_name not in self.config.allowed_functions:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=self.config.keyword_risk_level,
                        description=f"Suspicious function detected: {func_name}",
                        location=node.sql()[:100],
                        pattern_matched="suspicious_function",
                        recommendation=f"Review usage of {func_name} function",
                        metadata={"function": func_name},
                    )
                )

        # Special handling for Command nodes (e.g., EXECUTE statements)
        if isinstance(node, exp.Command):
            # Commands are often used for dynamic SQL execution
            command_text = str(node)
            if any(
                keyword in command_text.lower() for keyword in ["execute", "exec", "sp_executesql", "grant", "revoke"]
            ):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=RiskLevel.HIGH,
                        description=f"Dynamic SQL execution command detected: {command_text.split()[0].lower()}",
                        location=command_text[:100],
                        pattern_matched="exec_command",
                        recommendation="Avoid dynamic SQL execution",
                    )
                )

        # Check for specific patterns in SQL text
        if hasattr(node, "sql"):
            sql_text = node.sql()

            # File operations
            if PATTERNS["file_operations"].search(sql_text):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=RiskLevel.HIGH,
                        description="File operation detected in SQL",
                        location=sql_text[:100],
                        pattern_matched="file_operation",
                        recommendation="File operations should be handled at application level",
                    )
                )

            # Execution functions
            if PATTERNS["exec_functions"].search(sql_text):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=RiskLevel.HIGH,
                        description="Dynamic SQL execution function detected",
                        location=sql_text[:100],
                        pattern_matched="exec_function",
                        recommendation="Avoid dynamic SQL execution",
                    )
                )

            # Administrative commands
            if PATTERNS["admin_functions"].search(sql_text):
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                        risk_level=RiskLevel.HIGH,
                        description="Administrative command detected",
                        location=sql_text[:100],
                        pattern_matched="admin_function",
                        recommendation="Administrative commands should be restricted",
                    )
                )

            # Check custom suspicious patterns
            for name, pattern in self._compiled_patterns.items():
                if name.startswith("custom_suspicious_") and pattern.search(sql_text):
                    issues.append(
                        SecurityIssue(
                            issue_type=SecurityIssueType.SUSPICIOUS_KEYWORD,
                            risk_level=self.config.keyword_risk_level,
                            description=f"Custom suspicious pattern matched: {name}",
                            location=sql_text[:100],
                            pattern_matched=name,
                        )
                    )

        return issues

    @staticmethod
    def _check_combined_patterns(
        expression: "exp.Expression",  # noqa: ARG004
        existing_issues: "list[SecurityIssue]",
    ) -> "list[SecurityIssue]":
        """Check for combined attack patterns that indicate sophisticated attacks."""
        combined_issues: list[SecurityIssue] = []

        # Group issues by type
        issue_types = {issue.issue_type for issue in existing_issues}

        # Tautology + UNION = Classic SQLi
        if SecurityIssueType.TAUTOLOGY in issue_types and SecurityIssueType.INJECTION in issue_types:
            has_union = any(
                "union" in issue.pattern_matched.lower() for issue in existing_issues if issue.pattern_matched
            )
            if has_union:
                combined_issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.COMBINED_ATTACK,
                        risk_level=RiskLevel.HIGH,
                        description="Classic SQL injection pattern detected (Tautology + UNION)",
                        pattern_matched="classic_sqli",
                        recommendation="This appears to be a deliberate SQL injection attempt",
                        metadata={"attack_components": ["tautology", "union"], "confidence": "high"},
                    )
                )

        # Multiple suspicious functions + system schema = Data extraction attempt
        suspicious_func_count = sum(
            1
            for issue in existing_issues
            if issue.issue_type == SecurityIssueType.SUSPICIOUS_KEYWORD and "function" in (issue.pattern_matched or "")
        )
        system_schema_access = any("system_schema" in (issue.pattern_matched or "") for issue in existing_issues)

        if suspicious_func_count >= SUSPICIOUS_FUNC_THRESHOLD and system_schema_access:
            combined_issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.COMBINED_ATTACK,
                    risk_level=RiskLevel.HIGH,
                    description="Data extraction attempt detected (Multiple functions + System schema)",
                    pattern_matched="data_extraction",
                    recommendation="Block queries attempting to extract system information",
                    metadata={"suspicious_functions": suspicious_func_count, "targets_system_schema": True},
                )
            )

        # Encoding + Injection = Evasion attempt
        has_encoding = any("encoding" in (issue.pattern_matched or "").lower() for issue in existing_issues)
        has_comment = any("comment" in (issue.pattern_matched or "").lower() for issue in existing_issues)

        if has_encoding or has_comment:
            combined_issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.COMBINED_ATTACK,
                    risk_level=RiskLevel.HIGH,
                    description="Evasion technique detected in SQL injection attempt",
                    pattern_matched="evasion_attempt",
                    recommendation="Input appears to be crafted to bypass security filters",
                    metadata={
                        "evasion_techniques": [
                            "encoding" if has_encoding else None,
                            "comments" if has_comment else None,
                        ]
                    },
                )
            )

        return combined_issues

    def _check_ast_anomalies(
        self, node: "exp.Expression", context: "SQLProcessingContext", nesting_depth: int
    ) -> "list[SecurityIssue]":
        """Check for AST-based anomalies that could indicate injection attempts.

        This method uses sophisticated AST analysis instead of regex patterns.
        """
        issues: list[SecurityIssue] = []

        # Check for excessive nesting (potential injection)
        if nesting_depth > self.config.max_nesting_depth:
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.AST_ANOMALY,
                    risk_level=self.config.ast_anomaly_risk_level,
                    description=f"Excessive query nesting detected (depth: {nesting_depth})",
                    location=node.sql()[:100] if hasattr(node, "sql") else str(node)[:100],
                    pattern_matched="excessive_nesting",
                    recommendation="Review query structure for potential injection",
                    ast_node_type=type(node).__name__,
                    confidence=0.8,
                    metadata={"nesting_depth": nesting_depth, "max_allowed": self.config.max_nesting_depth},
                )
            )

        # Check for suspiciously long literals (potential injection payload)
        if isinstance(node, Literal) and isinstance(node.this, str):
            literal_length = len(str(node.this))
            if literal_length > self.config.max_literal_length:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.AST_ANOMALY,
                        risk_level=self.config.ast_anomaly_risk_level,
                        description=f"Suspiciously long literal detected ({literal_length} chars)",
                        location=str(node.this)[:100],
                        pattern_matched="long_literal",
                        recommendation="Validate input length and content",
                        ast_node_type="Literal",
                        confidence=0.6,
                        metadata={"literal_length": literal_length, "max_allowed": self.config.max_literal_length},
                    )
                )

        # Check for unusual function call patterns
        if isinstance(node, Func):
            func_issues = self._analyze_function_anomalies(node)
            issues.extend(func_issues)

        # Check for suspicious binary operations (potential injection)
        if isinstance(node, Binary):
            binary_issues = self._analyze_binary_anomalies(node)
            issues.extend(binary_issues)

        return issues

    def _check_structural_attacks(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for structural attack patterns using AST analysis."""
        issues: list[SecurityIssue] = []

        # Check for UNION-based injection using AST structure
        if isinstance(node, Union):
            union_issues = self._analyze_union_structure(node)
            issues.extend(union_issues)

        # Check for subquery injection patterns
        if isinstance(node, Subquery):
            subquery_issues = self._analyze_subquery_structure(node)
            issues.extend(subquery_issues)

        # Check for OR-based injection using AST structure
        if isinstance(node, Or):
            or_issues = self._analyze_or_structure(node)
            issues.extend(or_issues)

        return issues

    @staticmethod
    def _analyze_function_anomalies(func_node: Func) -> "list[SecurityIssue]":
        """Analyze function calls for anomalous patterns."""
        issues: list[SecurityIssue] = []

        if not func_node.name:
            return issues

        func_name = func_node.name.lower()

        # Check for chained function calls (potential evasion)
        if hasattr(func_node, "this") and isinstance(func_node.this, Func):
            nested_func = func_node.this
            if nested_func.name and nested_func.name.lower() in SUSPICIOUS_FUNCTIONS:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.AST_ANOMALY,
                        risk_level=RiskLevel.MEDIUM,
                        description=f"Nested suspicious function call: {nested_func.name.lower()} inside {func_name}",
                        location=func_node.sql()[:100],
                        pattern_matched="nested_suspicious_function",
                        recommendation="Review nested function calls for evasion attempts",
                        ast_node_type="Func",
                        confidence=0.7,
                        metadata={"outer_function": func_name, "inner_function": nested_func.name.lower()},
                    )
                )

        # Check for unusual argument patterns
        if hasattr(func_node, "expressions") and func_node.expressions:
            arg_count = len(func_node.expressions)
            if func_name in {"concat", "concat_ws"} and arg_count > MAX_FUNCTION_ARGS:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.AST_ANOMALY,
                        risk_level=RiskLevel.MEDIUM,
                        description=f"Excessive arguments to {func_name} function ({arg_count} args)",
                        location=func_node.sql()[:100],
                        pattern_matched="excessive_function_args",
                        recommendation="Review function arguments for potential injection",
                        ast_node_type="Func",
                        confidence=0.6,
                        metadata={"function": func_name, "arg_count": arg_count},
                    )
                )

        return issues

    def _analyze_binary_anomalies(self, binary_node: Binary) -> "list[SecurityIssue]":
        """Analyze binary operations for suspicious patterns."""
        issues: list[SecurityIssue] = []

        # Check for deeply nested binary operations (potential injection)
        depth = self._calculate_binary_depth(binary_node)
        if depth > MAX_NESTING_LEVELS:  # Arbitrary threshold
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.AST_ANOMALY,
                    risk_level=RiskLevel.LOW,
                    description=f"Deeply nested binary operations detected (depth: {depth})",
                    location=binary_node.sql()[:100],
                    pattern_matched="deep_binary_nesting",
                    recommendation="Review complex condition structures",
                    ast_node_type="Binary",
                    confidence=0.5,
                    metadata={"nesting_depth": depth},
                )
            )

        return issues

    def _analyze_union_structure(self, union_node: Union) -> "list[SecurityIssue]":
        """Analyze UNION structure for injection patterns."""
        issues: list[SecurityIssue] = []

        # Check if UNION has mismatched column counts (classic injection)
        if hasattr(union_node, "left") and hasattr(union_node, "right"):
            left_cols = self._count_select_columns(union_node.left)
            right_cols = self._count_select_columns(union_node.right)

            if left_cols != right_cols and left_cols > 0 and right_cols > 0:
                issues.append(
                    SecurityIssue(
                        issue_type=SecurityIssueType.STRUCTURAL_ATTACK,
                        risk_level=RiskLevel.HIGH,
                        description=f"UNION with mismatched column counts ({left_cols} vs {right_cols})",
                        location=union_node.sql()[:100],
                        pattern_matched="union_column_mismatch",
                        recommendation="UNION queries should have matching column counts",
                        ast_node_type="Union",
                        confidence=0.9,
                        metadata={"left_columns": left_cols, "right_columns": right_cols},
                    )
                )

        return issues

    @staticmethod
    def _analyze_subquery_structure(subquery_node: Subquery) -> "list[SecurityIssue]":
        """Analyze subquery structure for injection patterns."""
        issues: list[SecurityIssue] = []

        # Check for subqueries that return unusual patterns
        if hasattr(subquery_node, "this") and isinstance(subquery_node.this, exp.Select):
            select_expr = subquery_node.this

            # Check if subquery selects only literals (potential injection)
            if hasattr(select_expr, "expressions") and select_expr.expressions:
                literal_count = sum(1 for expr in select_expr.expressions if isinstance(expr, Literal))
                total_expressions = len(select_expr.expressions)

                if literal_count == total_expressions and total_expressions > MIN_UNION_COUNT_FOR_INJECTION:
                    issues.append(
                        SecurityIssue(
                            issue_type=SecurityIssueType.STRUCTURAL_ATTACK,
                            risk_level=RiskLevel.MEDIUM,
                            description=f"Subquery selecting only literals ({literal_count} literals)",
                            location=subquery_node.sql()[:100],
                            pattern_matched="literal_only_subquery",
                            recommendation="Review subqueries that only select literal values",
                            ast_node_type="Subquery",
                            confidence=0.7,
                            metadata={"literal_count": literal_count, "total_expressions": total_expressions},
                        )
                    )

        return issues

    def _analyze_or_structure(self, or_node: Or) -> "list[SecurityIssue]":
        """Analyze OR conditions for tautology patterns."""
        issues: list[SecurityIssue] = []

        # Check for OR with tautological conditions using AST
        if (
            hasattr(or_node, "left")
            and hasattr(or_node, "right")
            and (self._is_always_true_condition(or_node.left) or self._is_always_true_condition(or_node.right))
        ):
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.STRUCTURAL_ATTACK,
                    risk_level=RiskLevel.HIGH,
                    description="OR condition with always-true clause detected",
                    location=or_node.sql()[:100],
                    pattern_matched="or_tautology_ast",
                    recommendation="Remove always-true conditions from OR clauses",
                    ast_node_type="Or",
                    confidence=0.95,
                    metadata={
                        "left_always_true": self._is_always_true_condition(or_node.left),
                        "right_always_true": self._is_always_true_condition(or_node.right),
                    },
                )
            )

        return issues

    def _calculate_binary_depth(self, node: Binary, depth: int = 0) -> int:
        """Calculate the depth of nested binary operations."""
        max_depth = depth

        if hasattr(node, "left") and isinstance(node.left, Binary):
            max_depth = max(max_depth, self._calculate_binary_depth(node.left, depth + 1))

        if hasattr(node, "right") and isinstance(node.right, Binary):
            max_depth = max(max_depth, self._calculate_binary_depth(node.right, depth + 1))

        return max_depth

    @staticmethod
    def _count_select_columns(node: "exp.Expression") -> int:
        """Count the number of columns in a SELECT statement."""
        if isinstance(node, exp.Select) and hasattr(node, "expressions"):
            return len(node.expressions) if node.expressions else 0
        return 0

    @staticmethod
    def _is_always_true_condition(node: "exp.Expression") -> bool:
        """Check if a condition is always true using AST analysis."""
        # Check for literal true
        if isinstance(node, Literal) and str(node.this).upper() in {"TRUE", "1"}:
            return True

        # Check for 1=1 or similar tautologies
        return bool(
            isinstance(node, EQ)
            and hasattr(node, "left")
            and hasattr(node, "right")
            and (
                isinstance(node.left, Literal)
                and isinstance(node.right, Literal)
                and str(node.left.this) == str(node.right.this)
            )
        )
