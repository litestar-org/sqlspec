"""SQL Processing Pipeline Base.

This module defines the core framework for constructing and executing a series of
SQL processing steps, such as transformations and validations.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

import sqlglot  # Added
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError  # Added

from sqlspec.exceptions import RiskLevel, SQLValidationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.sql import SQLConfig, Statement

__all__ = (
    "AnalysisResult",
    "ProcessorProtocol",
    "SQLAnalysis",
    "SQLTransformation",
    "SQLTransformer",
    "SQLValidation",
    "SQLValidator",
    "TransformationResult",
    "TransformerPipeline",
    "UnifiedProcessor",
    "UsesExpression",
    "ValidationResult",
)


logger = logging.getLogger("sqlspec")

ExpressionT = TypeVar("ExpressionT", bound="exp.Expression")


# Copied UsesExpression class here
class UsesExpression:
    """Utility mixin class to get a sqlglot expression from various inputs."""

    @staticmethod
    def get_expression(statement: "Statement", dialect: "DialectType" = None) -> "exp.Expression":
        """Convert SQL input to expression.

        Args:
            statement: The SQL statement to convert to an expression.
            dialect: The SQL dialect.

        Raises:
            SQLValidationError: If the SQL parsing fails.

        Returns:
            An exp.Expression.
        """
        if isinstance(statement, exp.Expression):
            return statement

        # Local import to avoid circular dependency at module level
        from sqlspec.statement.sql import SQL

        if isinstance(statement, SQL):
            expr = statement.expression
            if expr is not None:
                return expr
            # If SQL object has no expression (e.g. parsing disabled), parse its .sql string
            return sqlglot.parse_one(statement.sql, read=dialect)

        # Assuming statement is str hereafter
        sql_str = str(statement)
        if not sql_str or not sql_str.strip():
            return exp.Select()  # Return a neutral, empty expression for empty strings

        try:
            return sqlglot.parse_one(sql_str, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            # Provide context (sql_str, risk_level) for the error
            raise SQLValidationError(msg, sql_str, RiskLevel.HIGH) from e


class ProcessorProtocol(ABC, Generic[ExpressionT]):
    """Protocol for a processing step in the SQL pipeline.

    A processor can either transform an expression, validate it, or both.
    """

    @abstractmethod
    def process(
        self,
        expression: "ExpressionT",
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[ExpressionT, Optional[ValidationResult]]":
        """Process the SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect.
            config: The SQLConfig, providing context like strict_mode.

        Returns:
            A tuple containing the (potentially modified) expression
            and an optional ValidationResult if this processor performs validation.
        """
        raise NotImplementedError


@dataclass
class TransformerPipeline:
    """Orchestrates a sequence of SQL processing steps."""

    components: "list[ProcessorProtocol[exp.Expression]]" = field(default_factory=list)

    def execute(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, ValidationResult]":
        """Executes all components in the pipeline.

        Args:
            expression: The initial SQL expression.
            dialect: The SQL dialect.
            config: The SQLConfig to provide context to processors and for final validation aggregation.

        Returns:
            A tuple containing the final transformed expression and an aggregated ValidationResult.
        """
        # Need to import ValidationResult here for instantiation if it's not at top level due to TYPE_CHECKING

        current_expression = expression
        aggregated_issues: list[str] = []
        aggregated_warnings: list[str] = []
        # Ensure RiskLevel.SAFE is used correctly
        overall_risk_level = RiskLevel.SAFE
        final_is_safe = True

        if not self.components and config is not None and config.enable_validation:
            # This case implies default validation might be needed,
            # which SQLConfig/SQLStatement should set up by providing default components.
            pass  # SQLStatement handles adding default validator if components list is empty

        for component in self.components:
            current_expression, validation_result_component = component.process(current_expression, dialect, config)
            if validation_result_component is not None:
                aggregated_issues.extend(validation_result_component.issues)
                aggregated_warnings.extend(validation_result_component.warnings)
                if validation_result_component.risk_level.value > overall_risk_level.value:
                    overall_risk_level = validation_result_component.risk_level
                if not validation_result_component.is_safe:
                    final_is_safe = False

        return current_expression, ValidationResult(
            is_safe=final_is_safe,
            risk_level=overall_risk_level,
            issues=aggregated_issues,
            warnings=aggregated_warnings,
        )


class SQLValidation(ABC):
    """Abstract base class for individual SQL validation checks."""

    def __init__(self, risk_level: "RiskLevel", min_risk_to_raise: "Optional[RiskLevel]" = None) -> None:
        self.risk_level = risk_level
        self.min_risk_to_raise = min_risk_to_raise

    @abstractmethod
    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> "ValidationResult":
        """Validate the SQL expression.

        :param expression: The SQL expression to validate.
        :param dialect: The SQL dialect.
        :param config: The SQL configuration.
        :param kwargs: Additional keyword arguments.
        :return: A ValidationResult.
        """
        raise NotImplementedError


class SQLValidator(ProcessorProtocol[exp.Expression]):
    """Main SQL validator that orchestrates multiple validation checks.
    This class functions as a validation pipeline runner.
    """

    def __init__(
        self,
        validators: "Optional[Sequence[SQLValidation]]" = None,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,
    ) -> None:
        self.validators: list[SQLValidation] = list(validators) if validators is not None else []
        self.min_risk_to_raise = min_risk_to_raise
        # Ensure BaseSQLValidator is imported for the type hint at runtime if needed

    def add_validator(self, validator: "SQLValidation") -> None:  # Type hint should now work
        """Add a validator to the pipeline."""
        self.validators.append(validator)

    def process(
        self,
        expression: "exp.Expression",
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[exp.Expression, ValidationResult]":
        """Process the SQL expression through all configured validators.

        Args:
            expression: The SQL expression to validate.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A tuple containing the final transformed expression and an aggregated ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        active_config = config if config is not None else SQLConfig()

        if not active_config.enable_validation:
            return expression, ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)

        aggregated_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

        for validator_instance in self.validators:
            result = validator_instance.validate(expression, dialect, active_config, **kwargs)
            aggregated_result.merge(result)

        return expression, aggregated_result

    def validate(
        self,
        sql: "Statement",
        dialect: "DialectType",
        config: "Optional[SQLConfig]" = None,
    ) -> "ValidationResult":
        """Convenience method to validate a raw SQL string or expression.

        Args:
            sql: The SQL statement to validate.
            dialect: The SQL dialect.
            config: The SQL configuration.

        Returns:
            A ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        current_config = config or SQLConfig()
        expression_to_validate = UsesExpression.get_expression(sql, dialect=dialect)

        _, validation_result = self.process(expression_to_validate, dialect, current_config)
        return validation_result


class ValidationResult:
    """Result of SQL validation with detailed information."""

    __slots__ = (
        "is_safe",
        "issues",
        "risk_level",
        "transformed_sql",
        "warnings",
    )

    def __init__(
        self,
        is_safe: bool,
        risk_level: "RiskLevel",
        issues: "Optional[list[str]]" = None,
        warnings: "Optional[list[str]]" = None,
        transformed_sql: "Optional[str]" = None,
    ) -> None:
        self.is_safe = is_safe
        self.risk_level = risk_level
        self.issues = list(issues) if issues is not None else []
        self.warnings = list(warnings) if warnings is not None else []
        self.transformed_sql = transformed_sql  # Though likely not used by validators

    def merge(self, other: "ValidationResult") -> None:
        """Merge another ValidationResult into this one."""
        if not other.is_safe:
            self.is_safe = False
        self.issues.extend(other.issues)
        self.warnings.extend(other.warnings)
        # Set risk level to the higher of the two
        if other.risk_level.value > self.risk_level.value:
            self.risk_level = other.risk_level

    def __bool__(self) -> bool:
        return self.is_safe


class TransformationResult:
    """Result of SQL transformation with detailed information."""

    __slots__ = (
        "expression",
        "modified",
        "notes",
    )

    def __init__(
        self,
        expression: exp.Expression,
        modified: bool,
        notes: "Optional[list[str]]" = None,
    ) -> None:
        self.expression = expression
        self.modified = modified
        self.notes = notes if notes is not None else []

    def __bool__(self) -> bool:
        return self.modified


class SQLTransformer(ProcessorProtocol[exp.Expression], ABC):
    """Base class for SQL transformers that implement ProcessorProtocol.

    This provides a bridge between the old SQLTransformation pattern and
    the new ProcessorProtocol pipeline architecture.
    """

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the expression using the transform method.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.

        Returns:
            A tuple containing the transformed expression and None for ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        active_config = config if config is not None else SQLConfig()

        if not active_config.enable_transformations:
            return expression, None

        try:
            result = self.transform(expression, dialect, active_config)
        except Exception as e:  # noqa: BLE001
            # If transformation fails, return original expression
            logger.warning("Transformation failed: %s", e)
            return expression, None
        else:
            return result.expression, None

    @abstractmethod
    def transform(
        self,
        expression: exp.Expression,
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: Any,
    ) -> TransformationResult:
        """Transform the SQL expression.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A TransformationResult.
        """
        raise NotImplementedError


class AnalysisResult:
    """Result of SQL analysis with detailed metrics and insights."""

    __slots__ = (
        "issues",
        "metrics",
        "notes",
        "warnings",
    )

    def __init__(
        self,
        metrics: "Optional[dict[str, Any]]" = None,
        warnings: "Optional[list[str]]" = None,
        issues: "Optional[list[str]]" = None,
        notes: "Optional[list[str]]" = None,
    ) -> None:
        self.metrics = metrics if metrics is not None else {}
        self.warnings = warnings if warnings is not None else []
        self.issues = issues if issues is not None else []
        self.notes = notes if notes is not None else []

    def __bool__(self) -> bool:
        return len(self.issues) == 0


class SQLTransformation(ABC):
    """Abstract base class for SQL transformations."""

    @abstractmethod
    def transform(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> TransformationResult:
        """Transform the SQL expression.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A TransformationResult.
        """
        raise NotImplementedError


class SQLAnalysis(ABC):
    """Abstract base class for SQL analysis."""

    @abstractmethod
    def analyze(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> AnalysisResult:
        """Analyze the SQL expression.

        Args:
            expression: The SQL expression to analyze.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            An AnalysisResult.
        """
        raise NotImplementedError


class UnifiedProcessor(ProcessorProtocol[exp.Expression]):
    """Unified processor that combines analysis, transformation, and validation.

    This processor performs analysis once and shares the results with all
    transformers and validators to avoid redundant parsing and processing.
    """

    def __init__(
        self,
        analyzers: "Optional[Sequence[SQLAnalysis]]" = None,
        transformers: "Optional[Sequence[SQLTransformer]]" = None,
        validators: "Optional[Sequence[SQLValidation]]" = None,
        cache_analysis: bool = True,
    ) -> None:
        """Initialize the unified processor.

        Args:
            analyzers: List of analysis components to run
            transformers: List of transformation components to run
            validators: List of validation components to run
            cache_analysis: Whether to cache analysis results
        """
        self.analyzers = list(analyzers) if analyzers else []
        self.transformers = list(transformers) if transformers else []
        self.validators = list(validators) if validators else []
        self.cache_analysis = cache_analysis
        self._analysis_cache: dict[str, AnalysisResult] = {}

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the expression through unified analysis, transformation, and validation.

        Args:
            expression: The SQL expression to process
            dialect: The SQL dialect
            config: The SQL configuration

        Returns:
            Tuple of (transformed expression, validation result)
        """
        from sqlspec.statement.sql import SQLConfig

        active_config = config if config is not None else SQLConfig()

        # Step 1: Perform unified analysis once
        analysis_result = self._perform_unified_analysis(expression, dialect, active_config)

        # Step 2: Apply transformations (if enabled)
        current_expression = expression
        if active_config.enable_transformations:
            current_expression = self._apply_transformations(
                current_expression, analysis_result, dialect, active_config
            )

        # Step 3: Apply validations (if enabled)
        validation_result = None
        if active_config.enable_validation:
            validation_result = self._apply_validations(current_expression, analysis_result, dialect, active_config)

        return current_expression, validation_result

    def _perform_unified_analysis(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
    ) -> AnalysisResult:
        """Perform comprehensive analysis once and cache results."""
        # Check cache first
        cache_key = expression.sql() if self.cache_analysis else None
        if cache_key and cache_key in self._analysis_cache:
            return self._analysis_cache[cache_key]

        # Perform comprehensive analysis
        analysis = AnalysisResult()

        # Basic structural analysis
        analysis.metrics.update(self._analyze_structure(expression))

        # Join analysis (shared by multiple validators)
        analysis.metrics.update(self._analyze_joins(expression))

        # Subquery analysis
        analysis.metrics.update(self._analyze_subqueries(expression))

        # Function analysis
        analysis.metrics.update(self._analyze_functions(expression))

        # Table and column analysis
        analysis.metrics.update(self._analyze_tables_and_columns(expression))

        # Complexity scoring
        analysis.metrics["complexity_score"] = self._calculate_complexity_score(analysis.metrics)

        # Run custom analyzers
        for analyzer in self.analyzers:
            self._run_single_analyzer(analyzer, expression, dialect, config, analysis)

        # Cache the result
        if cache_key:
            self._analysis_cache[cache_key] = analysis

        return analysis

    @staticmethod
    def _run_single_analyzer(
        analyzer: "SQLAnalysis",
        expression: exp.Expression,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
        analysis: AnalysisResult,
    ) -> None:
        """Run a single analyzer and update the analysis result."""
        try:
            custom_result = analyzer.analyze(expression, dialect, config)
            analysis.metrics.update(custom_result.metrics)
            analysis.warnings.extend(custom_result.warnings)
            analysis.issues.extend(custom_result.issues)
            analysis.notes.extend(custom_result.notes)
        except Exception as e:  # noqa: BLE001
            logger.warning("Analysis component failed: %s", e)
            analysis.warnings.append(f"Analysis component failed: {e}")

    def _apply_transformations(
        self,
        expression: exp.Expression,
        analysis: AnalysisResult,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
    ) -> exp.Expression:
        """Apply transformations using shared analysis results."""
        current_expression = expression

        for transformer in self.transformers:
            current_expression = self._run_single_transformer(
                transformer, current_expression, analysis, dialect, config
            )

        return current_expression

    @staticmethod
    def _run_single_transformer(
        transformer: "SQLTransformer",
        expression: exp.Expression,
        analysis: AnalysisResult,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
    ) -> exp.Expression:
        """Run a single transformer and return the transformed expression."""
        try:
            # Pass analysis results to transformer if it supports it
            transform_with_analysis = getattr(transformer, "transform_with_analysis", None)
            if transform_with_analysis is not None:
                result = transform_with_analysis(expression, analysis, dialect, config)
            else:
                result = transformer.transform(expression, dialect, config)
        except Exception as e:  # noqa: BLE001
            logger.warning("Transformation component failed: %s", e)
            return expression
        else:
            if result.modified:
                return result.expression
            return expression

    def _apply_validations(
        self,
        expression: exp.Expression,
        analysis: AnalysisResult,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
    ) -> ValidationResult:
        """Apply validations using shared analysis results."""
        aggregated_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

        for validator in self.validators:
            self._run_single_validator(validator, expression, analysis, dialect, config, aggregated_result)

        return aggregated_result

    @staticmethod
    def _run_single_validator(
        validator: "SQLValidation",
        expression: exp.Expression,
        analysis: AnalysisResult,
        dialect: "Optional[DialectType]",
        config: "SQLConfig",
        aggregated_result: ValidationResult,
    ) -> None:
        """Run a single validator and update the aggregated result."""
        try:
            # Pass analysis results to validator if it supports it
            validate_with_analysis = getattr(validator, "validate_with_analysis", None)
            if validate_with_analysis is not None:
                result = validate_with_analysis(expression, analysis, dialect, config)
            else:
                result = validator.validate(expression, dialect, config)

            aggregated_result.merge(result)

        except Exception as e:  # noqa: BLE001
            logger.warning("Validation component failed: %s", e)
            aggregated_result.warnings.append(f"Validation component failed: {e}")

    @staticmethod
    def _analyze_structure(expression: exp.Expression) -> dict[str, Any]:
        """Analyze basic SQL structure."""
        return {
            "statement_type": type(expression).__name__,
            "has_subqueries": bool(expression.find(exp.Subquery)),
            "has_cte": bool(expression.find(exp.CTE)),
            "has_window_functions": bool(expression.find(exp.Window)),
            "has_aggregates": bool(expression.find(exp.AggFunc)),
        }

    def _analyze_joins(self, expression: exp.Expression) -> dict[str, Any]:
        """Comprehensive join analysis shared by multiple components."""
        joins = list(expression.find_all(exp.Join))
        join_count = len(joins)

        join_types = {}
        cross_joins = 0
        joins_without_conditions = 0

        for join in joins:
            # Analyze join type
            join_type = self._get_join_type(join)
            join_types[join_type] = join_types.get(join_type, 0) + 1

            # Check for cross joins
            if join_type == "CROSS":
                cross_joins += 1

            # Check for joins without conditions
            if not join.on and not join.using and join_type != "CROSS":
                joins_without_conditions += 1

        # Analyze potential cartesian products
        cartesian_risk = cross_joins + joins_without_conditions

        return {
            "join_count": join_count,
            "join_types": join_types,
            "cross_joins": cross_joins,
            "joins_without_conditions": joins_without_conditions,
            "cartesian_risk": cartesian_risk,
        }

    def _analyze_subqueries(self, expression: exp.Expression) -> dict[str, Any]:
        """Analyze subquery complexity."""
        subqueries = list(expression.find_all(exp.Subquery))
        subquery_count = len(subqueries)

        max_depth = self._calculate_subquery_depth(expression)
        correlated_count = self._count_correlated_subqueries(subqueries)

        return {
            "subquery_count": subquery_count,
            "max_subquery_depth": max_depth,
            "correlated_subquery_count": correlated_count,
        }

    @staticmethod
    def _analyze_functions(expression: exp.Expression) -> dict[str, Any]:
        """Analyze function usage."""
        functions = list(expression.find_all(exp.Func))
        function_count = len(functions)

        function_types = {}
        expensive_functions = 0

        expensive_func_names = {"regexp", "regex", "like", "concat_ws", "group_concat"}

        for func in functions:
            func_name = func.name.lower() if func.name else "unknown"
            function_types[func_name] = function_types.get(func_name, 0) + 1

            if func_name in expensive_func_names:
                expensive_functions += 1

        return {
            "function_count": function_count,
            "function_types": function_types,
            "expensive_functions": expensive_functions,
        }

    @staticmethod
    def _analyze_tables_and_columns(expression: exp.Expression) -> dict[str, Any]:
        """Analyze table and column usage."""
        tables = []
        for table in expression.find_all(exp.Table):
            if table.name and table.name not in tables:
                tables.append(table.name)

        columns = []
        for column in expression.find_all(exp.Column):
            if column.name and column.name not in columns:
                columns.append(column.name)

        return {
            "table_count": len(tables),
            "tables": tables,
            "column_count": len(columns),
            "columns": columns,
        }

    @staticmethod
    def _calculate_complexity_score(metrics: dict[str, Any]) -> int:
        """Calculate overall complexity score from metrics."""
        score = 0

        # Join complexity
        score += metrics.get("join_count", 0) * 3
        score += metrics.get("cartesian_risk", 0) * 20

        # Subquery complexity
        score += metrics.get("subquery_count", 0) * 5
        score += metrics.get("max_subquery_depth", 0) * 10
        score += metrics.get("correlated_subquery_count", 0) * 8

        # Function complexity
        score += metrics.get("function_count", 0) * 1
        score += metrics.get("expensive_functions", 0) * 5

        return score

    @staticmethod
    def _get_join_type(join: exp.Join) -> str:
        """Determine the type of join."""
        if join.side and join.side.upper() == "LEFT":
            return "LEFT"
        if join.side and join.side.upper() == "RIGHT":
            return "RIGHT"
        if join.side and join.side.upper() == "FULL":
            return "FULL"
        if join.kind and join.kind.upper() == "CROSS":
            return "CROSS"
        if not join.on and not join.using:
            return "CROSS"  # Implicit cross join
        return "INNER"

    def _calculate_subquery_depth(self, expression: exp.Expression, current_depth: int = 0) -> int:
        """Calculate maximum subquery nesting depth."""
        max_depth = current_depth

        for subquery in expression.find_all(exp.Subquery):
            if subquery.parent == expression:  # Direct child
                depth = self._calculate_subquery_depth(subquery, current_depth + 1)
                max_depth = max(max_depth, depth)

        return max_depth

    @staticmethod
    def _count_correlated_subqueries(subqueries: list[exp.Subquery]) -> int:
        """Count correlated subqueries (simplified heuristic)."""
        correlated_count = 0

        for subquery in subqueries:
            # Simple heuristic: check for EXISTS patterns
            subquery_sql = subquery.sql().lower()
            if any(keyword in subquery_sql for keyword in ["exists", "not exists"]):
                correlated_count += 1

        return correlated_count

    def clear_cache(self) -> None:
        """Clear the analysis cache."""
        self._analysis_cache.clear()
