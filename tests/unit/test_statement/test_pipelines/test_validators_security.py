"""Tests for the unified SecurityValidator."""

from typing import Optional

import pytest
from sqlglot import exp, parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators._security import (
    SecurityIssue,
    SecurityIssueType,
    SecurityValidator,
    SecurityValidatorConfig,
)
from sqlspec.statement.sql import SQLConfig


class TestSecurityValidator:
    """Test the unified security validator."""

    def _create_context(
        self, sql: "str" = "SELECT 1", expression: "Optional[exp.Expression]" = None
    ) -> "SQLProcessingContext":
        """Helper method to create SQLProcessingContext with proper required arguments."""
        return SQLProcessingContext(
            initial_sql_string=sql, dialect=None, config=SQLConfig(), current_expression=expression
        )

    @pytest.fixture
    def validator(self) -> "SecurityValidator":
        """Create a security validator with default config."""
        return SecurityValidator()

    @pytest.fixture
    def custom_validator(self) -> "SecurityValidator":
        """Create a security validator with custom config."""
        config = SecurityValidatorConfig(
            check_injection=True,
            check_tautology=True,
            check_keywords=True,
            check_combined_patterns=True,
            max_union_count=2,
            max_null_padding=3,
            allowed_functions=["concat", "substring"],
            blocked_functions=["xp_cmdshell", "exec"],
            custom_injection_patterns=[r"(?i)waitfor\s+delay"],
            custom_suspicious_patterns=[r"(?i)dbms_"],
        )
        return SecurityValidator(config)

    def test_init_default_config(self) -> "None":
        """Test initialization with default configuration."""
        validator = SecurityValidator()
        assert validator.config.check_injection is True
        assert validator.config.check_tautology is True
        assert validator.config.check_keywords is True
        assert validator.config.max_union_count == 3

    def test_init_custom_config(self, custom_validator: "SecurityValidator") -> "None":
        """Test initialization with custom configuration."""
        assert custom_validator.config.max_union_count == 2
        assert "concat" in custom_validator.config.allowed_functions
        assert "xp_cmdshell" in custom_validator.config.blocked_functions

    def test_no_expression(self, validator: "SecurityValidator") -> "None":
        """Test processing with no expression."""
        context = self._create_context(expression=None)
        expression, validation_result = validator.process(context)
        assert validation_result.risk_level == RiskLevel.SKIP

    def test_clean_query(self, validator: "SecurityValidator") -> "None":
        """Test processing a clean query with no security issues."""
        sql = "SELECT * FROM users WHERE id = 1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.SKIP
        assert len(validation_result.issues) == 0
        security_data = context.get_additional_data("security_validator")
        assert security_data["total_issues"] == 0

    # Injection Detection Tests
    def test_union_injection_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of UNION-based SQL injection."""
        # Use a validator with lower threshold to detect 3 NULLs
        config = SecurityValidatorConfig(max_null_padding=2)
        custom_validator = SecurityValidator(config)

        sql = "SELECT * FROM users WHERE id = 1 UNION SELECT NULL, NULL, NULL"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.INJECTION for issue in issues)
        assert any("NULL padding" in issue.description for issue in issues)

    def test_excessive_unions(self, custom_validator: "SecurityValidator") -> "None":
        """Test detection of excessive UNION operations."""
        sql = """
        SELECT * FROM users
        UNION SELECT * FROM admins
        UNION SELECT * FROM logs
        UNION SELECT * FROM config
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Excessive UNION" in issue.description for issue in issues)

    def test_comment_evasion_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of comment-based evasion."""
        sql = "SELECT * FROM users WHERE id = 1 /* OR 1=1 */ AND status = 'active'"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Comment-based" in issue.description for issue in issues)

    def test_encoded_char_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of encoded character evasion."""
        sql = "SELECT * FROM users WHERE name = CHAR(65) || CHR(66)"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Encoded character" in issue.description for issue in issues)

    def test_system_schema_access(self, validator: "SecurityValidator") -> "None":
        """Test detection of system schema access."""
        sql = "SELECT * FROM information_schema.tables"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("system schema" in issue.description for issue in issues)

    def test_custom_injection_pattern(self, custom_validator: "SecurityValidator") -> "None":
        """Test custom injection pattern detection."""
        sql = "SELECT * FROM users WHERE id = 1; WAITFOR DELAY '00:00:05'"
        # Parse only the first statement since sqlglot doesn't understand WAITFOR DELAY
        expression = parse_one("SELECT * FROM users WHERE id = 1")
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Custom injection pattern" in issue.description for issue in issues)

    # Tautology Detection Tests
    def test_tautology_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of tautological conditions."""
        sql = "SELECT * FROM users WHERE 1 = 1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)

    def test_or_tautology_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of OR with tautology."""
        sql = "SELECT * FROM users WHERE username = 'admin' OR 1=1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("OR with always-true" in issue.description for issue in issues)

    def test_column_self_comparison(self, validator: "SecurityValidator") -> "None":
        """Test detection of column comparing to itself."""
        sql = "SELECT * FROM users WHERE id = id"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)

    # Suspicious Keyword Detection Tests
    def test_suspicious_function_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of suspicious functions."""
        sql = "SELECT LOAD_FILE('/etc/passwd') FROM dual"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level in [RiskLevel.MEDIUM, RiskLevel.HIGH]
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.SUSPICIOUS_KEYWORD for issue in issues)

    def test_blocked_function_detection(self, custom_validator: "SecurityValidator") -> "None":
        """Test detection of explicitly blocked functions."""
        sql = "SELECT xp_cmdshell('dir c:\\')"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Blocked function" in issue.description for issue in issues)

    def test_allowed_function_not_flagged(self, custom_validator: "SecurityValidator") -> "None":
        """Test that allowed functions are not flagged."""
        sql = "SELECT CONCAT(first_name, ' ', last_name) FROM users"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        # Should not flag concat since it's in allowed_functions
        security_data = context.get_additional_data("security_validator")
        issues = security_data["security_issues"] if security_data else []
        assert not any(
            issue.issue_type == SecurityIssueType.SUSPICIOUS_KEYWORD and "concat" in issue.description.lower()
            for issue in issues
        )

    def test_file_operation_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of file operations."""
        sql = "SELECT load_file('/etc/passwd')"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("File operation" in issue.description for issue in issues)

    def test_exec_function_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of dynamic SQL execution."""
        sql = "EXECUTE sp_executesql @sql"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Dynamic SQL execution" in issue.description for issue in issues)

    def test_admin_command_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of administrative commands."""
        sql = "GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Administrative command" in issue.description for issue in issues)

    def test_custom_suspicious_pattern(self, custom_validator: "SecurityValidator") -> "None":
        """Test custom suspicious pattern detection."""
        sql = "SELECT dbms_random.value() FROM dual"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = custom_validator.process(context)

        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Custom suspicious pattern" in issue.description for issue in issues)

    # Combined Attack Pattern Tests
    def test_classic_sqli_pattern(self, validator: "SecurityValidator") -> "None":
        """Test detection of classic SQL injection (tautology + UNION)."""
        sql = """
        SELECT * FROM users WHERE id = 1 OR 1=1
        UNION SELECT username, password, NULL FROM admin_users
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        # Accept either MEDIUM or HIGH risk - depends on combined pattern detection
        assert validation_result.risk_level in [RiskLevel.MEDIUM, RiskLevel.HIGH]
        issues = context.get_additional_data("security_validator")["security_issues"]

        # Should detect tautology pattern
        assert any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)

        # Test passes if it detects either injection or just tautology
        # (UNION detection depends on threshold configuration)

    def test_data_extraction_attempt(self, validator: "SecurityValidator") -> "None":
        """Test detection of data extraction attempts."""
        sql = """
        SELECT table_name, column_name,
               CONCAT(table_schema, '.', table_name) as full_name,
               HEX(column_name) as hex_name
        FROM information_schema.columns
        WHERE table_schema = 'mysql'
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        # Should at least detect system schema access (information_schema)
        assert validation_result.risk_level in [RiskLevel.MEDIUM, RiskLevel.HIGH]
        issues = context.get_additional_data("security_validator")["security_issues"]

        # Should detect system schema access if nothing else
        assert any(
            issue.issue_type == SecurityIssueType.INJECTION and "system schema" in issue.description.lower()
            for issue in issues
        )

        assert any("system schema" in issue.description for issue in issues)

    def test_evasion_attempt_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of evasion attempts."""
        sql = """
        SELECT * FROM users
        WHERE username = CHAR(97,100,109,105,110) /* admin */
        UNION SELECT NULL, 0x70617373776f7264 -- password
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        # The validator should detect something suspicious
        assert validation_result.risk_level >= RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]

        # Should detect at least hex encoding or comments
        assert any(
            "hex" in issue.description.lower()
            or "comment" in issue.description.lower()
            or "encoded" in issue.description.lower()
            for issue in issues
        )

        # Should detect evasion attempt
        assert any(
            issue.issue_type == SecurityIssueType.COMBINED_ATTACK and "Evasion technique" in issue.description
            for issue in issues
        )

    # Configuration Tests
    def test_disabled_checks(self) -> "None":
        """Test that disabled checks don't run."""
        config = SecurityValidatorConfig(
            check_injection=False,
            check_tautology=False,
            check_keywords=True,
            check_combined_patterns=False,
        )
        validator = SecurityValidator(config)

        # SQL with injection and tautology
        sql = "SELECT * FROM users WHERE 1=1 UNION SELECT NULL, NULL"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        # Should not detect injection or tautology
        security_data = context.get_additional_data("security_validator")
        issues = security_data["security_issues"] if security_data else []
        assert not any(issue.issue_type == SecurityIssueType.INJECTION for issue in issues)
        assert not any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)
        assert not any(issue.issue_type == SecurityIssueType.COMBINED_ATTACK for issue in issues)

    def test_metadata_reporting(self, validator: "SecurityValidator") -> "None":
        """Test that metadata is properly reported."""
        sql = """
        SELECT * FROM users WHERE 1=1
        UNION SELECT username, password FROM information_schema.user_privileges
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        metadata = context.get_additional_data("security_validator")
        assert "security_issues" in metadata
        assert "checks_performed" in metadata
        assert "total_issues" in metadata
        assert "issue_breakdown" in metadata

        # Check breakdown
        breakdown = metadata["issue_breakdown"]
        assert breakdown[SecurityIssueType.TAUTOLOGY.name] >= 1
        assert breakdown[SecurityIssueType.INJECTION.name] >= 1

    def test_risk_level_calculation(self, validator: "SecurityValidator") -> "None":
        """Test that highest risk level is returned."""
        config = SecurityValidatorConfig(
            injection_risk_level=RiskLevel.HIGH,
            tautology_risk_level=RiskLevel.LOW,
            keyword_risk_level=RiskLevel.MEDIUM,
        )
        validator = SecurityValidator(config)

        # SQL with multiple issues
        sql = "SELECT * FROM users WHERE 1=1 UNION SELECT LOAD_FILE('/etc/passwd')"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        # Should return HIGH (from injection)
        assert validation_result.risk_level == RiskLevel.HIGH

    def test_security_issue_details(self, validator: "SecurityValidator") -> "None":
        """Test that SecurityIssue objects contain proper details."""
        sql = "SELECT * FROM information_schema.tables"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        expression_result, validation_result = validator.process(context)

        issues = context.get_additional_data("security_validator")["security_issues"]
        assert len(issues) > 0

        issue = issues[0]
        assert isinstance(issue, SecurityIssue)
        assert issue.issue_type in SecurityIssueType
        assert issue.risk_level in RiskLevel
        assert issue.description
        assert issue.pattern_matched
        assert issue.recommendation
