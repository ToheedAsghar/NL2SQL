"""
AGENT 2 -- SCHEMA FORMATTER (LLM)

TAKES TOP-K TABLES AND FORMATS THEM INTO A DDL-STYLE SCHEMA PROMPT
"""

import logging
from models.schemas import TableMetaData, FormattedSchema

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a database schema formatter.
Given raw table metadata, produce a clean, compact DDL-style schema that will be used as context for SQL generation.
Include table names, column names, data types, primary keys, foreign keys, and comments. Be concise but complete.
Output ONLY the formatted schema — no explanation."""

class SchemaFormatterAgent(): # BaseAgent
    def build_prompt(
            self, tables: list[TableMetaData], **_
    ) -> list[dict[str, str]]:
        table_dumps = "\n\n".join(
            self._table_to_raw_text(t) for t in tables
        )

        USER_PROMPT = f"Format these tables:\n\n{table_dumps}"

        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT}
        ]
    
    def _table_to_raw_text(self, table: TableMetaData) -> str:
        lines = [f"TABLE: {table.schema_name}.{table.table_name}"]
        if table.comments:
            lines.append(f" COMMENT: {table.comments}")
        
        for col in table.columns:
            flags = []
            if col.is_primary_key:
                flags.append("PK")
            if col.is_foreign_key:
                flags.append(f"FK+{col.reference_table}.{col.reference_column}")
            if not col.nullable:
                flags.append("NOT NULL")
        
            flag_str = f"   [{', '.join(flags)}]" if flags else ""
            lines.append(f" {col.column_name} {col.data_type} {flag_str}")
        return "\n".join(lines)

    def parse_response(self, raw: str) -> FormattedSchema:
        return FormattedSchema(
            content=raw,
            table_names=[]
            token_estimate=len(raw.split())
        )
    
    async def format(self, tables: list[TableMetaData]) -> FormattedSchema:
        logger.info("SchemaFormatterAgent: formatting %d tables", len(tables))

        result: FormattedSchema = await self.execute(
            tables=tables,
            temperature=0.1
        )

        result.table_names = [t.table_name for t in tables]
        logger.info("SchemaFormatterAgent: ~%d tokens", result.token_estimate)

        return result
