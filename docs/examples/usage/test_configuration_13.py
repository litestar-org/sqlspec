from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig


def test_parameter_style_config():
    param_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,  # $1, $2, ...
        supported_parameter_styles={
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,  # :name
        },
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
    )

    statement_config = StatementConfig(
        dialect="postgres",
        parameter_config=param_config
    )
    assert statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC

