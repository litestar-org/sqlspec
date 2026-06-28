"""Shared statement pipeline registry and instrumentation."""

import contextlib
import hashlib
import os
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.compiler import CompiledSQL, SQLProcessor

if TYPE_CHECKING:
    import sqlglot.expressions as exp

    from sqlspec.core.statement import StatementConfig

__all__ = (
    "StatementPipelineRegistry",
    "compile_with_pipeline",
    "get_statement_pipeline_metrics",
    "reset_statement_pipeline_cache",
)

DEBUG_ENV_FLAG: Final[str] = "SQLSPEC_DEBUG_PIPELINE_CACHE"
DEFAULT_PIPELINE_CACHE_SIZE: Final[int] = 1000
DEFAULT_PIPELINE_PARSE_CACHE_SIZE: Final[int] = 5000
DEFAULT_PIPELINE_COUNT: Final[int] = 32


def _is_truthy(value: "str | None") -> bool:
    return bool(value and value.strip().lower() in {"1", "true", "yes", "on"})


_RECORD_PIPELINE_METRICS: Final[bool] = _is_truthy(os.environ.get(DEBUG_ENV_FLAG))

_METRIC_KEYS: Final[tuple[str, ...]] = (
    "hits",
    "misses",
    "size",
    "max_size",
    "parse_hits",
    "parse_misses",
    "parse_size",
    "parse_max_size",
    "parameter_hits",
    "parameter_misses",
    "parameter_size",
    "parameter_max_size",
    "validator_hits",
    "validator_misses",
    "validator_size",
    "validator_max_size",
)


@mypyc_attr(allow_interpreted_subclasses=False)
class _PipelineMetrics:
    __slots__ = ("_values",)

    def __init__(self) -> None:
        self._values = dict.fromkeys(_METRIC_KEYS, 0)

    def update(self, stats: "dict[str, int]") -> None:
        values = self._values
        for key in _METRIC_KEYS:
            values[key] = stats.get(key, 0)

    def snapshot(self) -> "dict[str, int]":
        return self._values.copy()

    def reset(self) -> None:
        values = self._values
        for key in _METRIC_KEYS:
            values[key] = 0


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
        self,
        sql: str,
        parameters: Any,
        is_many: bool,
        record_metrics: bool,
        expression: "exp.Expr | None" = None,
        param_fingerprint: "Any | None" = None,
    ) -> "CompiledSQL":
        result = self._processor.compile(
            sql, parameters, is_many=is_many, expression=expression, param_fingerprint=param_fingerprint
        )
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
        expression: "exp.Expr | None" = None,
        param_fingerprint: "Any | None" = None,
    ) -> "CompiledSQL":
        key = self._fingerprint_config(config)
        pipeline = self._pipelines.get(key)
        record_metrics = _RECORD_PIPELINE_METRICS

        if pipeline is not None:
            self._pipelines.move_to_end(key)
        else:
            pipeline = _StatementPipeline(
                config, self._pipeline_cache_size, self._pipeline_parse_cache_size, self._cache_enabled, record_metrics
            )
            if len(self._pipelines) >= self._max_pipelines:
                self._pipelines.popitem(last=False)
            self._pipelines[key] = pipeline

        return pipeline.compile(
            sql, parameters, is_many, record_metrics, expression=expression, param_fingerprint=param_fingerprint
        )

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
        if not _RECORD_PIPELINE_METRICS:
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
            entry.update(metrics)
            snapshots.append(entry)
        return snapshots

    @staticmethod
    def _fingerprint_config(config: "Any") -> str:
        is_frozen = bool(getattr(config, "_is_frozen", False))
        if is_frozen:
            try:
                cached = config._fingerprint_cache  # pyright: ignore[reportPrivateUsage]
                if isinstance(cached, str):
                    return cached
            except AttributeError:
                pass

        config_hash = hash(config)
        param_config = config.parameter_config
        converter_type = type(config.parameter_converter) if config.parameter_converter else None
        validator_type = type(config.parameter_validator) if config.parameter_validator else None
        param_output_transformer_id = id(param_config.output_transformer) if param_config.output_transformer else None
        param_ast_transformer_id = id(param_config.ast_transformer) if param_config.ast_transformer else None
        supplement = (converter_type, validator_type, param_output_transformer_id, param_ast_transformer_id)
        fingerprint = hashlib.blake2b(repr((config_hash, supplement)).encode(), digest_size=8).hexdigest()
        full_fingerprint = f"pipeline::{fingerprint}"

        if is_frozen:
            with contextlib.suppress(AttributeError):
                config._fingerprint_cache = full_fingerprint  # pyright: ignore[reportPrivateUsage]

        return full_fingerprint


_PIPELINE_REGISTRY: "StatementPipelineRegistry" = StatementPipelineRegistry()


def compile_with_pipeline(
    config: "Any",
    sql: str,
    parameters: Any,
    is_many: bool = False,
    expression: "exp.Expr | None" = None,
    param_fingerprint: "Any | None" = None,
) -> "CompiledSQL":
    return _PIPELINE_REGISTRY.compile(
        config, sql, parameters, is_many=is_many, expression=expression, param_fingerprint=param_fingerprint
    )


def reset_statement_pipeline_cache() -> None:
    _PIPELINE_REGISTRY.reset()


def configure_statement_pipeline_cache(cache_size: int, parse_cache_size: int, cache_enabled: bool) -> None:
    _PIPELINE_REGISTRY.configure_cache(cache_size, parse_cache_size, cache_enabled)


def get_statement_pipeline_metrics() -> "list[dict[str, Any]]":
    return _PIPELINE_REGISTRY.metrics()
