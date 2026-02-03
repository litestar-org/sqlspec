import time

from sqlspec.core.parameters import ParameterProfile, ParameterStyle, ParameterStyleConfig

# Mocking enough state for _transform_cached_parameters
CONFIG = ParameterStyleConfig(ParameterStyle.QMARK)
PROFILE = ParameterProfile([])  # Simplified
PARAMS = ("note",)
INPUT_NAMES = ()


def bench_transform() -> None:
    from sqlspec.core.parameters import ParameterProcessor

    proc = ParameterProcessor()

    start = time.perf_counter()
    for _ in range(10000):
        _ = proc._transform_cached_parameters(
            PARAMS, PROFILE, CONFIG, input_named_parameters=INPUT_NAMES, is_many=False, apply_wrap_types=False
        )
    time.perf_counter() - start


if __name__ == "__main__":
    bench_transform()
