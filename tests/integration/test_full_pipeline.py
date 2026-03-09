"""Integration test: full pipeline end-to-end (all agents mocked)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from nl2sql_agents.models.schemas import (
    ColumnMetaData,
    FinalOutput,
    GraphState,
    TableMetaData,
)
from tests.conftest import make_table, make_column


def _fake_llm(response: str):
    """Return a context manager that patches ChatOpenAI.ainvoke."""
    fake = MagicMock()
    fake.content = response
    fake.usage_metadata = {"input_tokens": 10, "output_tokens": 5}
    return patch(
        "langchain_openai.ChatOpenAI.ainvoke",
        new_callable=AsyncMock,
        return_value=fake,
    )


def _fake_embeddings():
    """Return a context manager that patches embedding calls."""
    mock_emb = MagicMock()

    async def _embed(texts):
        return [[0.5] * 4 for _ in texts]

    mock_emb.aembed_documents = AsyncMock(side_effect=_embed)
    return mock_emb


# ---------------------------------------------------------------------------
# Node-level integration tests (avoid compiling the full LangGraph)
# ---------------------------------------------------------------------------

class TestNodeIntegrations:
    """Test individual pipeline nodes with mocked dependencies."""

    @pytest.mark.asyncio
    async def test_load_schema_with_real_db(self, tmp_sqlite_db, monkeypatch):
        """load_schema node reads from a real SQLite database."""
        from nl2sql_agents.orchestrator import nodes

        monkeypatch.setattr(nodes, "DB_PATH", tmp_sqlite_db)
        # Replace the module-level connector with one pointing to our test DB
        from nl2sql_agents.db.connector import DatabaseConnector

        test_connector = DatabaseConnector(db_path=tmp_sqlite_db)
        monkeypatch.setattr(nodes, "connector", test_connector)

        # Clear cache to force introspection
        nodes.cache.invalidate(tmp_sqlite_db)

        state: GraphState = {"user_query": "test"}  # type: ignore[typeddict-item]
        result = await nodes.load_schema(state)

        assert "tables" in result
        assert len(result["tables"]) >= 3  # singer, concert, stadium

    @pytest.mark.asyncio
    async def test_security_filter_node(self, sample_tables, monkeypatch):
        from nl2sql_agents.orchestrator import nodes

        # Patch DB_TYPE on the security_filter module
        monkeypatch.setattr(
            "nl2sql_agents.filters.security_filter.DB_TYPE", "sqlite"
        )

        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "test",
            "tables": sample_tables,
        }
        result = await nodes.security_filter_node(state)

        assert "security_passed" in result
        assert len(result["security_passed"]) == len(sample_tables)

    @pytest.mark.asyncio
    async def test_gate_node_passes(self, sample_tables, sample_scored_tables):
        from nl2sql_agents.orchestrator import nodes
        from nl2sql_agents.models.schemas import DiscoveryResult

        security_set = {t.table_name for t in sample_tables}
        discovery = DiscoveryResult(
            top_tables=sample_tables[:2],
            scored_tables=sample_scored_tables,
        )

        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "test",
            "tables": sample_tables,
            "security_passed": security_set,
            "discovery_result": discovery,
        }
        result = await nodes.gate_node(state)

        assert "gated_tables" in result
        assert len(result["gated_tables"]) > 0

    @pytest.mark.asyncio
    async def test_format_schema_node(self, sample_tables):
        from nl2sql_agents.orchestrator import nodes

        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "test",
            "gated_tables": sample_tables[:2],
        }

        with _fake_llm("CREATE TABLE singer (Singer_ID INTEGER PRIMARY KEY);\n"):
            result = await nodes.format_schema_node(state)

        assert "formatted_schema" in result
        assert result["formatted_schema"].content != ""

    @pytest.mark.asyncio
    async def test_generate_and_validate_nodes(self, sample_tables):
        """generate_sql → validate round-trip with mocked LLM."""
        from nl2sql_agents.orchestrator import nodes
        from nl2sql_agents.models.schemas import FormattedSchema

        schema = FormattedSchema(
            content="CREATE TABLE singer (Singer_ID INT PK, Name TEXT);",
            table_names=["singer"],
            token_estimate=10,
        )
        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "show me all singers",
            "formatted_schema": schema,
            "attempt": 1,
        }

        with _fake_llm("SELECT * FROM singer"):
            gen_result = await nodes.generate_sql_node(state)

        assert "generation" in gen_result
        assert len(gen_result["generation"].candidates) > 0

        # Feed into validator
        state["generation"] = gen_result["generation"]

        with _fake_llm("PASS"):
            val_result = await nodes.validate_node(state)

        assert "validation" in val_result
