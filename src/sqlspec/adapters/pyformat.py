from sqlspec.adapters.generic import GenericAdapter
from sqlspec.patterns import VAR_REF


def _replacer(match) -> str:
    """Regex hook for named to pyformat conversion."""
    gd = match.groupdict()
    if gd["dquote"] is not None:  # "..."
        return gd["dquote"]
    if gd["squote"] is not None:  # '...'
        return gd["squote"]
    # :something to %(something)s
    return f'{gd["lead"]}%({gd["var_name"]})s'


class PyFormatAdapter(GenericAdapter):
    """Convert from named to pyformat parameter style."""

    def process_sql(self, _query_name: str, _op_type, sql: str):
        """From named to pyformat."""
        return VAR_REF.sub(_replacer, sql)
