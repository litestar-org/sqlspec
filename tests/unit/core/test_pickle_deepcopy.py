"""Round-trip tests for copy.deepcopy and pickle on mypyc-compiled slotted classes."""

# Safe: pickle round-trips here serialize objects produced inside the test and
# immediately deserialize them in the same process. No untrusted input crosses
# the pickle boundary.
import copy
import pickle
from datetime import datetime

import pytest
from sqlglot import exp

from sqlspec.core import (
    SQL,
    AnyCollectionFilter,
    BeforeAfterFilter,
    BooleanFilter,
    ChoicesFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    OrderByFilter,
    SearchFilter,
    StatementConfig,
    StatementFilter,
)
from sqlspec.core.filters import NotAnyCollectionFilter, NotInSearchFilter, OnBeforeAfterFilter
from sqlspec.core.parameters._types import (
    DriverParameterProfile,
    ParameterInfo,
    ParameterProcessingResult,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    TypedParameter,
)


def _filter_samples() -> "list[StatementFilter]":
    """Return one representative instance of every concrete filter class."""
    return [
        LimitOffsetFilter(10, 5),
        OrderByFilter("id", "desc"),
        SearchFilter("name", "foo", ignore_case=True),
        SearchFilter({"name", "email"}, "bar"),
        BeforeAfterFilter("created_at", before=datetime(2025, 1, 1)),
        OnBeforeAfterFilter("updated_at", on_or_after=datetime(2025, 1, 1)),
        InCollectionFilter("id", [1, 2, 3]),
        NotInCollectionFilter("id", [4, 5]),
        AnyCollectionFilter("tags", ["a", "b"]),
        NotAnyCollectionFilter("tags", ["c"]),
        NullFilter("deleted_at"),
        NotNullFilter("created_at"),
        NotInSearchFilter("name", "spam"),
        BooleanFilter("is_active", True),
        ChoicesFilter("status", ["active", "pending"]),
    ]


@pytest.mark.parametrize("flt", _filter_samples(), ids=lambda f: type(f).__name__)
def test_filter_deepcopy_roundtrip(flt: "StatementFilter") -> None:
    copied = copy.deepcopy(flt)
    assert copied is not flt
    assert copied == flt
    assert type(copied) is type(flt)


@pytest.mark.parametrize("flt", _filter_samples(), ids=lambda f: type(f).__name__)
def test_filter_pickle_roundtrip(flt: "StatementFilter") -> None:
    restored = pickle.loads(pickle.dumps(flt))
    assert restored is not flt
    assert restored == flt
    assert type(restored) is type(flt)


def test_filter_eq_uses_cache_key() -> None:
    a = LimitOffsetFilter(10, 5)
    b = LimitOffsetFilter(10, 5)
    c = LimitOffsetFilter(10, 6)
    assert a == b
    assert a != c
    assert hash(a) == hash(b)


def test_filter_eq_returns_notimplemented_for_other_types() -> None:
    assert LimitOffsetFilter(1, 0).__eq__("not a filter") is NotImplemented


def test_filter_with_expression_field_name_is_hashable() -> None:
    flt = NullFilter(exp.column("created_at"))
    copy.deepcopy(flt)
    hash(flt)


def test_statement_config_deepcopy_pickle_roundtrip() -> None:
    config = StatementConfig(enable_caching=False, dialect="sqlite")
    deep = copy.deepcopy(config)
    assert deep == config
    restored = pickle.loads(pickle.dumps(config))
    assert restored == config


def test_typed_parameter_deepcopy_pickle_roundtrip() -> None:
    tp = TypedParameter(42, int, "count")
    deep = copy.deepcopy(tp)
    assert deep == tp
    restored = pickle.loads(pickle.dumps(tp))
    assert restored == tp


def test_parameter_info_deepcopy_pickle_roundtrip() -> None:
    pi = ParameterInfo(name="x", style=ParameterStyle.NAMED_COLON, position=0, ordinal=0, placeholder_text=":x")
    deep = copy.deepcopy(pi)
    assert deep.name == pi.name
    assert deep.style == pi.style
    assert deep.ordinal == pi.ordinal
    restored = pickle.loads(pickle.dumps(pi))
    assert restored.placeholder_text == pi.placeholder_text


def test_parameter_style_config_deepcopy_pickle_roundtrip() -> None:
    psc = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    deep = copy.deepcopy(psc)
    assert deep.default_parameter_style == psc.default_parameter_style
    restored = pickle.loads(pickle.dumps(psc))
    assert restored.default_parameter_style == psc.default_parameter_style


def test_parameter_profile_deepcopy_pickle_roundtrip() -> None:
    pi = ParameterInfo(name="x", style=ParameterStyle.NAMED_COLON, position=0, ordinal=0, placeholder_text=":x")
    pp = ParameterProfile([pi])
    deep = copy.deepcopy(pp)
    assert deep.named_parameters == pp.named_parameters
    restored = pickle.loads(pickle.dumps(pp))
    assert restored.named_parameters == pp.named_parameters


def test_driver_parameter_profile_deepcopy_pickle_roundtrip() -> None:
    dpp = DriverParameterProfile(
        name="test",
        default_style=ParameterStyle.QMARK,
        supported_styles=[ParameterStyle.QMARK],
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles=None,
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        extras={"k": "v"},
        statement_kwargs={"a": 1},
    )
    deep = copy.deepcopy(dpp)
    assert deep.name == dpp.name
    assert dict(deep.extras) == {"k": "v"}
    assert dict(deep.statement_kwargs) == {"a": 1}
    restored = pickle.loads(pickle.dumps(dpp))
    assert restored.name == dpp.name
    assert dict(restored.extras) == {"k": "v"}


def test_parameter_processing_result_deepcopy_pickle_roundtrip() -> None:
    pp = ParameterProfile([])
    ppr = ParameterProcessingResult(sql="SELECT 1", parameters=(), parameter_profile=pp)
    deep = copy.deepcopy(ppr)
    assert deep.sql == ppr.sql
    restored = pickle.loads(pickle.dumps(ppr))
    assert restored.sql == ppr.sql


def test_sql_deepcopy_clears_pooled_flag() -> None:
    flt = LimitOffsetFilter(10, 0)
    sql = SQL("SELECT * FROM users WHERE id = :id", flt, id=42)
    sql.compile()

    deep = copy.deepcopy(sql)
    assert deep.raw_sql == sql.raw_sql
    assert deep._pooled is False
    assert deep is not sql


def test_sql_pickle_roundtrip() -> None:
    sql = SQL("SELECT * FROM users WHERE id = :id", id=42)
    sql.compile()

    restored = pickle.loads(pickle.dumps(sql))
    assert restored.raw_sql == sql.raw_sql
    assert restored._pooled is False
