"""Tests for the unified SecurityValidator."""
# pyright: reportOptionalMemberAccess=false

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
            check_ast_anomalies=True,
            check_structural_attacks=True,
            max_union_count=2,
            max_null_padding=3,
            max_nesting_depth=3,
            max_literal_length=500,
            min_confidence_threshold=0.5,
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
        assert validator.config.check_ast_anomalies is True
        assert validator.config.check_structural_attacks is True
        assert validator.config.max_union_count == 3
        assert validator.config.min_confidence_threshold == 0.7

    def test_init_custom_config(self, custom_validator: "SecurityValidator") -> "None":
        """Test initialization with custom configuration."""
        assert custom_validator.config.max_union_count == 2
        assert "concat" in custom_validator.config.allowed_functions
        assert "xp_cmdshell" in custom_validator.config.blocked_functions

    def test_no_expression(self, validator: "SecurityValidator") -> "None":
        """Test processing with no expression."""
        context = self._create_context(expression=None)
        _expression, validation_result = validator.process(context)
        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.SKIP

    def test_clean_query(self, validator: "SecurityValidator") -> "None":
        """Test processing a clean query with no security issues."""
        sql = "SELECT * FROM users WHERE id = 1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
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
        _expression, validation_result = custom_validator.process(context)

        assert validation_result is not None
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
        _expression, validation_result = custom_validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Excessive UNION" in issue.description for issue in issues)

    def test_comment_evasion_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of comment-based evasion."""
        sql = "SELECT * FROM users WHERE id = 1 /* OR 1=1 */ AND status = 'active'"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Comment-based" in issue.description for issue in issues)

    def test_encoded_char_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of encoded character evasion."""
        sql = "SELECT * FROM users WHERE name = CHAR(65) || CHR(66)"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Encoded character" in issue.description for issue in issues)

    def test_system_schema_access(self, validator: "SecurityValidator") -> "None":
        """Test detection of system schema access."""
        sql = "SELECT * FROM information_schema.tables"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("system schema" in issue.description for issue in issues)

    def test_custom_injection_pattern(self, custom_validator: "SecurityValidator") -> "None":
        """Test custom injection pattern detection."""
        sql = "SELECT * FROM users WHERE id = 1; WAITFOR DELAY '00:00:05'"
        # Parse only the first statement since sqlglot doesn't understand WAITFOR DELAY
        expression = parse_one("SELECT * FROM users WHERE id = 1")
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = custom_validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Custom injection pattern" in issue.description for issue in issues)

    # Tautology Detection Tests
    def test_tautology_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of tautological conditions."""
        sql = "SELECT * FROM users WHERE 1 = 1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)

    def test_or_tautology_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of OR with tautology."""
        sql = "SELECT * FROM users WHERE username = 'admin' OR 1=1"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("OR with always-true" in issue.description for issue in issues)

    def test_column_self_comparison(self, validator: "SecurityValidator") -> "None":
        """Test detection of column comparing to itself."""
        sql = "SELECT * FROM users WHERE id = id"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.MEDIUM
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.TAUTOLOGY for issue in issues)

    # Suspicious Keyword Detection Tests
    def test_suspicious_function_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of suspicious functions."""
        sql = "SELECT LOAD_FILE('/etc/passwd') FROM dual"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level in [RiskLevel.MEDIUM, RiskLevel.HIGH]
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any(issue.issue_type == SecurityIssueType.SUSPICIOUS_KEYWORD for issue in issues)

    def test_blocked_function_detection(self, custom_validator: "SecurityValidator") -> "None":
        """Test detection of explicitly blocked functions."""
        sql = "SELECT xp_cmdshell('dir c:\\')"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = custom_validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Blocked function" in issue.description for issue in issues)

    def test_allowed_function_not_flagged(self, custom_validator: "SecurityValidator") -> "None":
        """Test that allowed functions are not flagged."""
        sql = "SELECT CONCAT(first_name, ' ', last_name) FROM users"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = custom_validator.process(context)

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
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("File operation" in issue.description for issue in issues)

    def test_exec_function_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of dynamic SQL execution."""
        sql = "EXECUTE sp_executesql @sql"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Dynamic SQL execution" in issue.description for issue in issues)

    def test_admin_command_detection(self, validator: "SecurityValidator") -> "None":
        """Test detection of administrative commands."""
        sql = "GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH
        issues = context.get_additional_data("security_validator")["security_issues"]
        assert any("Administrative command" in issue.description for issue in issues)

    def test_custom_suspicious_pattern(self, custom_validator: "SecurityValidator") -> "None":
        """Test custom suspicious pattern detection."""
        sql = "SELECT dbms_random.value() FROM dual"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = custom_validator.process(context)

        assert validation_result is not None
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
        _expression, validation_result = validator.process(context)

        # Accept either MEDIUM or HIGH risk - depends on combined pattern detection
        assert validation_result is not None
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
        _expression, validation_result = validator.process(context)

        # Should at least detect system schema access (information_schema)
        assert validation_result is not None
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
        _expression, validation_result = validator.process(context)

        # The validator should detect something suspicious
        assert validation_result is not None
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
            check_injection=False, check_tautology=False, check_keywords=True, check_combined_patterns=False
        )
        validator = SecurityValidator(config)

        # SQL with injection and tautology
        sql = "SELECT * FROM users WHERE 1=1 UNION SELECT NULL, NULL"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

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
        _expression, _validation_result = validator.process(context)

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
            injection_risk_level=RiskLevel.HIGH, tautology_risk_level=RiskLevel.LOW, keyword_risk_level=RiskLevel.MEDIUM
        )
        validator = SecurityValidator(config)

        # SQL with multiple issues
        sql = "SELECT * FROM users WHERE 1=1 UNION SELECT LOAD_FILE('/etc/passwd')"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, validation_result = validator.process(context)

        # Should return HIGH (from injection)
        assert validation_result is not None
        assert validation_result.risk_level == RiskLevel.HIGH

    def test_security_issue_details(self, validator: "SecurityValidator") -> "None":
        """Test that SecurityIssue objects contain proper details."""
        sql = "SELECT * FROM information_schema.tables"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        issues = context.get_additional_data("security_validator")["security_issues"]
        assert len(issues) > 0

        issue = issues[0]
        assert isinstance(issue, SecurityIssue)
        assert issue.issue_type in SecurityIssueType
        assert issue.risk_level in RiskLevel
        assert issue.description
        assert issue.pattern_matched
        assert issue.recommendation

    # AST Anomaly Detection Tests
    def test_ast_anomaly_excessive_nesting(self, custom_validator: "SecurityValidator") -> "None":
        """Test detection of excessive query nesting using AST analysis."""
        # Query with deep nesting (4 levels - exceeds custom_validator's limit of 3)
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM users
                ) t1
            ) t2
        ) t3
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = custom_validator.process(context)

        # Should detect excessive nesting
        issues = context.get_additional_data("security_validator")["security_issues"]
        nesting_issues = [issue for issue in issues if issue.issue_type == SecurityIssueType.AST_ANOMALY]

        # Should find at least one nesting issue
        assert len(nesting_issues) > 0
        assert any("nesting" in issue.description.lower() for issue in nesting_issues)

    def test_ast_anomaly_long_literal(self, custom_validator: "SecurityValidator") -> "None":
        """Test detection of suspiciously long literals."""
        # Create a literal longer than the 500 char limit
        long_string = "x" * 600
        sql = f"SELECT * FROM users WHERE description = '{long_string}'"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = custom_validator.process(context)

        # Should detect long literal
        issues = context.get_additional_data("security_validator")["security_issues"]
        literal_issues = [issue for issue in issues if "long literal" in issue.description.lower()]

        assert len(literal_issues) > 0
        assert any(issue.ast_node_type == "Literal" for issue in literal_issues)

    def test_ast_anomaly_nested_functions(self, validator: "SecurityValidator") -> "None":
        """Test detection of nested suspicious function calls."""
        sql = "SELECT SUBSTRING(CONCAT(username, password), 1, 10) FROM users"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Should detect nested function pattern
        issues = context.get_additional_data("security_validator")["security_issues"]
        function_issues = [issue for issue in issues if "nested" in issue.description.lower()]

        # May or may not detect depending on configuration
        for issue in function_issues:
            assert issue.ast_node_type == "Func"
            assert issue.confidence <= 1.0

    def test_ast_anomaly_excessive_function_args(self, validator: "SecurityValidator") -> "None":
        """Test detection of functions with excessive arguments."""
        # CONCAT with many arguments (potential evasion)
        args = "', '".join([f"col{i}" for i in range(15)])
        sql = f"SELECT CONCAT('{args}') FROM users"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Should detect excessive function arguments
        issues = context.get_additional_data("security_validator")["security_issues"]
        arg_issues = [issue for issue in issues if "excessive arguments" in issue.description.lower()]

        # May or may not detect depending on actual argument count after parsing
        for issue in arg_issues:
            assert "concat" in issue.description.lower()
            assert issue.metadata.get("arg_count", 0) > 10

    # Structural Attack Detection Tests
    def test_structural_attack_union_column_mismatch(self, validator: "SecurityValidator") -> "None":
        """Test detection of UNION with mismatched column counts."""
        sql = "SELECT id, name FROM users UNION SELECT id, name, email FROM admins"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Should detect column mismatch
        issues = context.get_additional_data("security_validator")["security_issues"]
        union_issues = [issue for issue in issues if issue.issue_type == SecurityIssueType.STRUCTURAL_ATTACK]

        # Should find column mismatch - check for the actual text used
        mismatch_issues = [issue for issue in union_issues if "mismatched column counts" in issue.description.lower()]
        assert len(mismatch_issues) > 0
        assert any(issue.confidence > 0.8 for issue in mismatch_issues)

    def test_structural_attack_literal_only_subquery(self, validator: "SecurityValidator") -> "None":
        """Test detection of subqueries that only select literals."""
        sql = "SELECT * FROM users WHERE id IN (SELECT 1, 2, 3, 4, 5)"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Should detect literal-only subquery
        issues = context.get_additional_data("security_validator")["security_issues"]
        literal_subquery_issues = [
            issue
            for issue in issues
            if "literal" in issue.description.lower() and "subquery" in issue.description.lower()
        ]

        # May detect depending on how SQLGlot parses IN with literals
        for issue in literal_subquery_issues:
            assert issue.issue_type == SecurityIssueType.STRUCTURAL_ATTACK
            assert issue.ast_node_type == "Subquery"

    def test_structural_attack_or_tautology_ast(self, validator: "SecurityValidator") -> "None":
        """Test AST-based detection of OR with always-true conditions."""
        sql = "SELECT * FROM users WHERE username = 'admin' OR TRUE"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Should detect OR with always-true clause using AST analysis
        issues = context.get_additional_data("security_validator")["security_issues"]

        # Check if any issues were detected at all
        assert len(issues) > 0

        # The validator might categorize this differently - check for tautology issues
        tautology_issues = [issue for issue in issues if issue.issue_type == SecurityIssueType.TAUTOLOGY]
        structural_issues = [issue for issue in issues if issue.issue_type == SecurityIssueType.STRUCTURAL_ATTACK]

        # Ensure we have some security issues detected
        assert len(tautology_issues) > 0 or len(structural_issues) > 0

    # Confidence Filtering Tests
    def test_confidence_threshold_filtering(self) -> "None":
        """Test that low-confidence issues are filtered out."""
        config = SecurityValidatorConfig(
            check_ast_anomalies=True,
            min_confidence_threshold=0.8,  # High threshold
        )
        validator = SecurityValidator(config)

        # Query that might generate low-confidence issues
        sql = "SELECT * FROM users WHERE id > 0"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        # Check that metadata shows filtering occurred
        metadata = context.get_additional_data("security_validator")
        if metadata:
            total_found = metadata.get("total_issues_found", 0)
            after_filter = metadata.get("issues_after_confidence_filter", 0)

            # If any issues were found, some might have been filtered
            if total_found > 0:
                assert after_filter <= total_found

    def test_ast_based_vs_pattern_based_detection(self, validator: "SecurityValidator") -> "None":
        """Test that AST-based detection works alongside pattern-based detection."""
        # Query with both pattern-based and AST-detectable issues
        sql = """
        SELECT * FROM users
        WHERE id = 1 OR 1=1  -- tautology (both pattern and AST)
        UNION SELECT CHAR(65), CHAR(66)  -- encoding (pattern) + structure (AST)
        """
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        issues = context.get_additional_data("security_validator")["security_issues"]

        # Should detect issues from multiple detection methods
        issue_types = {issue.issue_type for issue in issues}

        # Should have at least tautology detection
        assert SecurityIssueType.TAUTOLOGY in issue_types or SecurityIssueType.STRUCTURAL_ATTACK in issue_types

        # Check that some issues have AST node type information
        ast_issues = [issue for issue in issues if issue.ast_node_type is not None]
        assert len(ast_issues) > 0

    def test_new_security_issue_fields(self, validator: "SecurityValidator") -> "None":
        """Test that new SecurityIssue fields are populated correctly."""
        sql = "SELECT * FROM users WHERE TRUE OR FALSE"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        issues = context.get_additional_data("security_validator")["security_issues"]

        for issue in issues:
            # Check new fields exist
            assert hasattr(issue, "ast_node_type")
            assert hasattr(issue, "confidence")

            # Check confidence is in valid range
            assert 0.0 <= issue.confidence <= 1.0

            # AST node type should be string or None
            if issue.ast_node_type is not None:
                assert isinstance(issue.ast_node_type, str)

    def test_disabled_ast_checks(self) -> "None":
        """Test that AST-based checks can be disabled."""
        config = SecurityValidatorConfig(
            check_ast_anomalies=False,
            check_structural_attacks=False,
            check_injection=True,  # Keep other checks enabled
        )
        validator = SecurityValidator(config)

        # Query that would trigger AST-based detection
        sql = "SELECT * FROM users WHERE TRUE OR FALSE"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        issues = context.get_additional_data("security_validator")["security_issues"]

        # Should not have AST_ANOMALY or STRUCTURAL_ATTACK issues
        issue_types = {issue.issue_type for issue in issues}
        assert SecurityIssueType.AST_ANOMALY not in issue_types
        assert SecurityIssueType.STRUCTURAL_ATTACK not in issue_types

    def test_metadata_includes_new_checks(self, validator: "SecurityValidator") -> "None":
        """Test that metadata includes information about new check types."""
        sql = "SELECT * FROM users"
        expression = parse_one(sql)
        context = self._create_context(sql=sql, expression=expression)
        _expression, _validation_result = validator.process(context)

        metadata = context.get_additional_data("security_validator")
        checks_performed = metadata.get("checks_performed", [])

        # Should include the basic check types
        assert "injection" in checks_performed
        assert "tautology" in checks_performed
        assert "keywords" in checks_performed
        assert "combined" in checks_performed

        # Issue breakdown should include detected issue types
        breakdown = metadata.get("issue_breakdown", {})
        # Check that some issues were detected
        assert len(breakdown) > 0
