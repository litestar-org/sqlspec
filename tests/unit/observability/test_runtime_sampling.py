"""Regression tests for runtime statement-event sampling."""

from typing import Any, cast

from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, SamplingConfig, StatementEvent
from sqlspec.observability._config import StatementObserver
from sqlspec.utils.correlation import CorrelationContext


def _emit_statement(runtime: ObservabilityRuntime, *, duration_s: float = 0.001) -> None:
    runtime.emit_statement_event(
        sql="SELECT 1",
        parameters=None,
        driver="DummyDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=duration_s,
        storage_backend=None,
    )


def test_emit_statement_event_skips_observers_when_sampling_rejects() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(
            sampling=SamplingConfig(sample_rate=0.0, force_sample_on_error=False, force_sample_slow_queries_ms=None),
            statement_observers=(cast(StatementObserver, observed.append),),
        )
    )

    _emit_statement(runtime)

    assert observed == []


def test_emit_statement_event_marks_sampled_when_sampling_accepts() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(
            sampling=SamplingConfig(sample_rate=1.0), statement_observers=(cast(StatementObserver, observed.append),)
        )
    )

    _emit_statement(runtime)

    assert len(observed) == 1
    assert observed[0].sampled is True


def test_emit_statement_event_force_samples_slow_query() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(
            sampling=SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=10.0),
            statement_observers=(cast(StatementObserver, observed.append),),
        )
    )

    _emit_statement(runtime, duration_s=0.1)

    assert len(observed) == 1
    assert observed[0].sampled is True


def test_emit_statement_event_without_sampling_preserves_previous_behavior() -> None:
    observed: list[dict[str, Any]] = []

    def observer(event: StatementEvent) -> None:
        observed.append(event.as_dict())

    runtime = ObservabilityRuntime(
        ObservabilityConfig(sampling=None, statement_observers=(cast(StatementObserver, observer),))
    )

    with CorrelationContext.context("sample-correlation"):
        _emit_statement(runtime)

    assert len(observed) == 1
    assert observed[0]["correlation_id"] == "sample-correlation"
    assert observed[0]["sampled"] is True
