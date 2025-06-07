"""Security validator for SQL statements."""

import contextlib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("SecurityIssue", "SecurityIssueType", "SecurityValidator", "SecurityValidatorConfig")

logger = logging.getLogger(__name__)

# Constants
SUSPICIOUS_FUNC_THRESHOLD = 2


class SecurityIssueType(Enum):
    """Types of security issues that can be detected."""

    INJECTION = auto()
    TAUTOLOGY = auto()
    SUSPICIOUS_KEYWORD = auto()
    COMBINED_ATTACK = auto()


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


@dataclass
class SecurityValidatorConfig:
    """Configuration for the unified security validator."""

    # Feature toggles
    check_injection: bool = True
    check_tautology: bool = True
    check_keywords: bool = True
    check_combined_patterns: bool = True

    # Risk levels
    default_risk_level: "RiskLevel" = RiskLevel.HIGH
    injection_risk_level: "RiskLevel" = RiskLevel.HIGH
    tautology_risk_level: "RiskLevel" = RiskLevel.MEDIUM
    keyword_risk_level: "RiskLevel" = RiskLevel.MEDIUM

    # Thresholds
    max_union_count: int = 3
    max_null_padding: int = 5
    max_system_tables: int = 2

    # Allowed/blocked lists
    allowed_functions: "list[str]" = field(default_factory=list)
    blocked_functions: "list[str]" = field(default_factory=list)
    allowed_system_schemas: "list[str]" = field(default_factory=list)

    # Custom patterns
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


class SecurityValidator(ProcessorProtocol[exp.Expression]):
    """Unified security validator that performs comprehensive security checks in a single pass."""

    def __init__(
        self,
        config: Optional["SecurityValidatorConfig"] = None,
        **kwargs: Any,
    ) -> None:
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

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the SQL expression and detect security issues in a single pass."""
        if not context.current_expression:
            return exp.Placeholder(), ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)

        security_issues: list[SecurityIssue] = []
        visited_nodes: set[int] = set()

        # Single AST traversal for all security checks
        for node in context.current_expression.walk():
            node_id = id(node)
            if node_id in visited_nodes:
                continue
            visited_nodes.add(node_id)

            # Check injection patterns
            if self.config.check_injection:
                injection_issues = self._check_injection_patterns(node, context)
                security_issues.extend(injection_issues)

            # Check tautology conditions
            if self.config.check_tautology:
                tautology_issues = self._check_tautology_patterns(node, context)
                security_issues.extend(tautology_issues)

            # Check suspicious keywords/functions
            if self.config.check_keywords:
                keyword_issues = self._check_suspicious_keywords(node, context)
                security_issues.extend(keyword_issues)

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
        risk_level = RiskLevel.SKIP
        if security_issues:
            risk_level = max(issue.risk_level for issue in security_issues)

        # Create validation result
        is_safe = risk_level == RiskLevel.SKIP
        validation_result = ValidationResult(
            is_safe=is_safe,
            risk_level=risk_level,
            issues=[issue.description for issue in security_issues],
        )

        # Store metadata in context for access by caller
        context.set_additional_data(
            "security_validator",
            {
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
            },
        )

        return context.current_expression, validation_result

    def _check_injection_patterns(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for SQL injection patterns in the node."""
        issues: list[SecurityIssue] = []

        # Check UNION-based injection
        if isinstance(node, exp.Union):
            union_issues = self._check_union_injection(node, context)
            issues.extend(union_issues)

        # Check for comment-based evasion
        if hasattr(node, "sql"):
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

        # Check custom injection patterns
        if hasattr(node, "sql"):
            sql_text = node.sql()
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
                    metadata={
                        "database_type": db_type,
                        "schema": schema_name,
                        "table": table_name,
                    },
                )

        return None

    def _check_tautology_patterns(
        self, node: "exp.Expression", context: "SQLProcessingContext"
    ) -> "list[SecurityIssue]":
        """Check for tautology conditions that are always true."""
        issues: list[SecurityIssue] = []

        # Check for tautological conditions
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)) and self._is_tautology(node):
            issues.append(
                SecurityIssue(
                    issue_type=SecurityIssueType.TAUTOLOGY,
                    risk_level=self.config.tautology_risk_level,
                    description="Tautological condition detected",
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

        left = comparison.left
        right = comparison.right

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

    def _expressions_identical(self, expr1: "exp.Expression", expr2: "exp.Expression") -> bool:
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

    def _check_combined_patterns(
        self, expression: "exp.Expression", existing_issues: "list[SecurityIssue]"
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
                        metadata={
                            "attack_components": ["tautology", "union"],
                            "confidence": "high",
                        },
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
                    metadata={
                        "suspicious_functions": suspicious_func_count,
                        "targets_system_schema": True,
                    },
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
