"""Shared statement pipeline registry and instrumentation."""

import os
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.compiler import CompiledSQL, SQLProcessor

if TYPE_CHECKING:
    import sqlglot.expressions as exp

    from sqlspec.core.statement import StatementConfig

DEBUG_ENV_FLAG: Final[str] = "SQLSPEC_DEBUG_PIPELINE_CACHE"
DEFAULT_PIPELINE_CACHE_SIZE: Final[int] = 1000
DEFAULT_PIPELINE_PARSE_CACHE_SIZE: Final[int] = 5000
DEFAULT_PIPELINE_COUNT: Final[int] = 32


def _is_truthy(value: "str | None") -> bool:
    return bool(value and value.strip().lower() in {"1", "true", "yes", "on"})


@mypyc_attr(allow_interpreted_subclasses=False)
class _PipelineMetrics:
    __slots__ = (
        "hits",
        "max_size",
        "misses",
        "parameter_hits",
        "parameter_max_size",
        "parameter_misses",
        "parameter_size",
        "parse_hits",
        "parse_max_size",
        "parse_misses",
        "parse_size",
        "size",
        "validator_hits",
        "validator_max_size",
        "validator_misses",
        "validator_size",
    )

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.size = 0
        self.max_size = 0
        self.parse_hits = 0
        self.parse_misses = 0
        self.parse_size = 0
        self.parse_max_size = 0
        self.parameter_hits = 0
        self.parameter_misses = 0
        self.parameter_size = 0
        self.parameter_max_size = 0
        self.validator_hits = 0
        self.validator_misses = 0
        self.validator_size = 0
        self.validator_max_size = 0

    def update(self, stats: "dict[str, int]") -> None:
        self.hits = stats.get("hits", 0)
        self.misses = stats.get("misses", 0)
        self.size = stats.get("size", 0)
        self.max_size = stats.get("max_size", 0)
        self.parse_hits = stats.get("parse_hits", 0)
        self.parse_misses = stats.get("parse_misses", 0)
        self.parse_size = stats.get("parse_size", 0)
        self.parse_max_size = stats.get("parse_max_size", 0)
        self.parameter_hits = stats.get("parameter_hits", 0)
        self.parameter_misses = stats.get("parameter_misses", 0)
        self.parameter_size = stats.get("parameter_size", 0)
        self.parameter_max_size = stats.get("parameter_max_size", 0)
        self.validator_hits = stats.get("validator_hits", 0)
        self.validator_misses = stats.get("validator_misses", 0)
        self.validator_size = stats.get("validator_size", 0)
        self.validator_max_size = stats.get("validator_max_size", 0)

    def snapshot(self) -> "dict[str, int]":
        return {
            "hits": self.hits,
            "misses": self.misses,
            "size": self.size,
            "max_size": self.max_size,
            "parse_hits": self.parse_hits,
            "parse_misses": self.parse_misses,
            "parse_size": self.parse_size,
            "parse_max_size": self.parse_max_size,
            "parameter_hits": self.parameter_hits,
            "parameter_misses": self.parameter_misses,
            "parameter_size": self.parameter_size,
            "parameter_max_size": self.parameter_max_size,
            "validator_hits": self.validator_hits,
            "validator_misses": self.validator_misses,
            "validator_size": self.validator_size,
            "validator_max_size": self.validator_max_size,
        }

    def reset(self) -> None:
        self.hits = 0
        self.misses = 0
        self.size = 0
        self.max_size = 0
        self.parse_hits = 0
        self.parse_misses = 0
        self.parse_size = 0
        self.parse_max_size = 0
        self.parameter_hits = 0
        self.parameter_misses = 0
        self.parameter_size = 0
        self.parameter_max_size = 0
        self.validator_hits = 0
        self.validator_misses = 0
        self.validator_size = 0
        self.validator_max_size = 0


@mypyc_attr(allow_interpreted_subclasses=False)
class _StatementPipeline:
    __slots__ = ("_metrics", "_processor", "dialect", "parameter_style")

    def __init__(
        self,
        config: "StatementConfig",
        cache_size: int,
        parse_cache_size: int,
        cache_enabled: bool,
        record_metrics: bool,
    ) -> None:
        self._processor = SQLProcessor(
            config,
            max_cache_size=cache_size,
            parse_cache_size=parse_cache_size,
            parameter_cache_size=parse_cache_size,
            validator_cache_size=parse_cache_size,
            cache_enabled=cache_enabled,
        )
        self.dialect = str(config.dialect) if config.dialect else "default"
        parameter_style = config.parameter_config.default_parameter_style
        self.parameter_style = parameter_style.value if parameter_style else "unknown"
        self._metrics = _PipelineMetrics() if record_metrics else None

    def compile(
        self, sql: str, parameters: Any, is_many: bool, record_metrics: bool, expression: "exp.Expression | None" = None
    ) -> "CompiledSQL":
        result = self._processor.compile(sql, parameters, is_many=is_many, expression=expression)
        if record_metrics and self._metrics is not None:
            self._metrics.update(self._processor.cache_stats)
        return result

    def reset(self) -> None:
        self._processor.clear_cache()
        if self._metrics is not None:
            self._metrics.reset()

    def metrics(self) -> "dict[str, int] | None":
        if self._metrics is None:
            return None
        return self._metrics.snapshot()


@mypyc_attr(allow_interpreted_subclasses=False)
class StatementPipelineRegistry:
    __slots__ = ("_cache_enabled", "_max_pipelines", "_pipeline_cache_size", "_pipeline_parse_cache_size", "_pipelines")

    def __init__(
        self,
        max_pipelines: int = DEFAULT_PIPELINE_COUNT,
        cache_size: int = DEFAULT_PIPELINE_CACHE_SIZE,
        parse_cache_size: int = DEFAULT_PIPELINE_PARSE_CACHE_SIZE,
        cache_enabled: bool = True,
    ) -> None:
        self._pipelines: OrderedDict[str, _StatementPipeline] = OrderedDict()
        self._max_pipelines = max_pipelines
        self._pipeline_cache_size = cache_size
        self._pipeline_parse_cache_size = parse_cache_size
        self._cache_enabled = cache_enabled

    def compile(
        self,
        config: "StatementConfig",
        sql: str,
        parameters: Any,
        is_many: bool = False,
        expression: "exp.Expression | None" = None,
    ) -> "CompiledSQL":
        key = self._fingerprint_config(config)
        pipeline = self._pipelines.get(key)
        record_metrics = _is_truthy(os.getenv(DEBUG_ENV_FLAG))

        if pipeline is not None:
            self._pipelines.move_to_end(key)
        else:
            pipeline = _StatementPipeline(
                config, self._pipeline_cache_size, self._pipeline_parse_cache_size, self._cache_enabled, record_metrics
            )
            if len(self._pipelines) >= self._max_pipelines:
                self._pipelines.popitem(last=False)
            self._pipelines[key] = pipeline

        return pipeline.compile(sql, parameters, is_many, record_metrics, expression=expression)

    def reset(self) -> None:
        for pipeline in self._pipelines.values():
            pipeline.reset()
        self._pipelines.clear()

    def configure_cache(self, cache_size: int, parse_cache_size: int, cache_enabled: bool) -> None:
        self._pipeline_cache_size = max(cache_size, 0)
        self._pipeline_parse_cache_size = max(parse_cache_size, 0)
        self._cache_enabled = cache_enabled
        self.reset()

    def metrics(self) -> "list[dict[str, Any]]":
        if not _is_truthy(os.getenv(DEBUG_ENV_FLAG)):
            return []

        snapshots: list[dict[str, Any]] = []
        for key, pipeline in self._pipelines.items():
            metrics = pipeline.metrics()
            if metrics is None:
                continue
            entry: dict[str, Any] = {
                "config": key,
                "dialect": pipeline.dialect,
                "parameter_style": pipeline.parameter_style,
            }
            entry["hits"] = metrics["hits"]
            entry["misses"] = metrics["misses"]
            entry["size"] = metrics["size"]
            entry["max_size"] = metrics["max_size"]
            entry["parse_hits"] = metrics.get("parse_hits", 0)
            entry["parse_misses"] = metrics.get("parse_misses", 0)
            entry["parse_size"] = metrics.get("parse_size", 0)
            entry["parse_max_size"] = metrics.get("parse_max_size", 0)
            entry["parameter_hits"] = metrics.get("parameter_hits", 0)
            entry["parameter_misses"] = metrics.get("parameter_misses", 0)
            entry["parameter_size"] = metrics.get("parameter_size", 0)
            entry["parameter_max_size"] = metrics.get("parameter_max_size", 0)
            entry["validator_hits"] = metrics.get("validator_hits", 0)
            entry["validator_misses"] = metrics.get("validator_misses", 0)
            entry["validator_size"] = metrics.get("validator_size", 0)
            entry["validator_max_size"] = metrics.get("validator_max_size", 0)
            snapshots.append(entry)
        return snapshots

    def _fingerprint_config(self, config: "Any") -> str:
        param_config = config.parameter_config
        param_config_hash = param_config.hash()
        converter_type = type(config.parameter_converter) if config.parameter_converter else None
        validator_type = type(config.parameter_validator) if config.parameter_validator else None
        output_transformer_id = id(config.output_transformer) if config.output_transformer else None
        statement_transformer_ids = (
            tuple(id(transformer) for transformer in config.statement_transformers)
            if config.statement_transformers
            else ()
        )
        param_output_transformer_id = id(param_config.output_transformer) if param_config.output_transformer else None
        param_ast_transformer_id = id(param_config.ast_transformer) if param_config.ast_transformer else None
        finger_components = (
            bool(config.enable_parsing),
            bool(config.enable_validation),
            bool(config.enable_transformations),
            bool(config.enable_analysis),
            bool(config.enable_expression_simplification),
            bool(config.enable_parameter_type_wrapping),
            bool(config.enable_caching),
            str(config.dialect),
            param_config.default_parameter_style.value,
            param_config.default_execution_parameter_style.value,
            param_config_hash,
            converter_type,
            validator_type,
            output_transformer_id,
            statement_transformer_ids,
            bool(param_config.output_transformer),
            bool(param_config.ast_transformer),
            param_output_transformer_id,
            param_ast_transformer_id,
            param_config.has_native_list_expansion,
            param_config.allow_mixed_parameter_styles,
            param_config.preserve_parameter_format,
            param_config.preserve_original_params_for_many,
        )
        fingerprint = hash(finger_components)
        return f"pipeline::{fingerprint}"


_PIPELINE_REGISTRY: "StatementPipelineRegistry" = StatementPipelineRegistry()


def compile_with_pipeline(
    config: "Any", sql: str, parameters: Any, is_many: bool = False, expression: "exp.Expression | None" = None
) -> "CompiledSQL":
    return _PIPELINE_REGISTRY.compile(config, sql, parameters, is_many=is_many, expression=expression)


def reset_statement_pipeline_cache() -> None:
    _PIPELINE_REGISTRY.reset()


def configure_statement_pipeline_cache(cache_size: int, parse_cache_size: int, cache_enabled: bool) -> None:
    _PIPELINE_REGISTRY.configure_cache(cache_size, parse_cache_size, cache_enabled)


def get_statement_pipeline_metrics() -> "list[dict[str, Any]]":
    return _PIPELINE_REGISTRY.metrics()


__all__ = (
    "StatementPipelineRegistry",
    "compile_with_pipeline",
    "get_statement_pipeline_metrics",
    "reset_statement_pipeline_cache",
)
