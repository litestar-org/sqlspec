"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

This dialect inherits from BigQuery because both use the GoogleSQL standard.
It adds support for Spanner-specific features like INTERLEAVE IN PARENT and TTL.
"""

import logging
from sqlglot import exp
from sqlglot.dialects import BigQuery

logger = logging.getLogger(__name__)

class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    class Tokenizer(BigQuery.Tokenizer):
        """Extend BigQuery tokenizer with Spanner keywords."""
        # We don't add INTERLEAVE/TTL to KEYWORDS because sqlglot TokenType doesn't have them.
        # They will be parsed as identifiers and matched by text in the parser.

    class Parser(BigQuery.Parser):
        """Override parser to handle INTERLEAVE and TTL clauses."""

        def _parse_table_parts(self, schema: bool = False, is_db_reference: bool = False) -> exp.Expression:
            """Override to parse INTERLEAVE IN PARENT clause.

            Syntax:
                INTERLEAVE IN PARENT parent_table [ON DELETE {CASCADE|NO ACTION}]
            """
            # Parse standard table definition first
            table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference)

            # Check for INTERLEAVE clause
            # Debug: print current token
            # print(f"DEBUG: current token: {self._curr.text}, type: {self._curr.token_type}")
            
            if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):
                # Parse parent table name
                parent = self._parse_table_name()
                on_delete = None

                # Parse ON DELETE action
                if self._match_text_seq("ON", "DELETE"):
                    if self._match_text_seq("CASCADE"):
                        on_delete = "CASCADE"
                    elif self._match_text_seq("NO", "ACTION"):
                        on_delete = "NO ACTION"

                # Attach to table expression using custom properties
                table.set("interleave_parent", parent)
                if on_delete:
                    table.set("interleave_on_delete", on_delete)

            return table

        def _parse_property(self) -> exp.Expression:
            """Override to parse TTL property.

            Syntax:
                TTL INTERVAL 'duration' ON column_name
            """
            if self._match_text_seq("TTL"):
                # Parse TTL INTERVAL '30 days' ON column_name
                self._match_text_seq("INTERVAL")
                interval = self._parse_string()
                self._match_text_seq("ON")
                column = self._parse_id_var()

                # Create a Property expression
                return exp.Property(
                    this=exp.Literal.string("TTL"),
                    value=exp.Tuple(expressions=[interval, column]),
                )
            return super()._parse_property()

    class Generator(BigQuery.Generator):
        """Override generator to output INTERLEAVE and TTL syntax."""

        def table_sql(self, expression: exp.Expression) -> str:
            """Override to generate INTERLEAVE clause."""
            sql = super().table_sql(expression)

            # Add INTERLEAVE clause if present
            parent = expression.args.get("interleave_parent")
            if parent:
                sql += f"\nINTERLEAVE IN PARENT {self.sql(parent)}"
                on_delete = expression.args.get("interleave_on_delete")
                if on_delete:
                    sql += f" ON DELETE {on_delete}"

            return sql

        def property_sql(self, expression: exp.Expression) -> str:
            """Override to generate TTL property."""
            if expression.this.name == "TTL":
                # Extract values from Tuple
                values = expression.args.get("value").expressions
                interval = self.sql(values[0])
                column = self.sql(values[1])
                return f"TTL INTERVAL {interval} ON {column}"

            return super().property_sql(expression)