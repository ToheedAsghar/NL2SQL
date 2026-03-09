"""Integration test for the Discovery pipeline (keyword + semantic + FK agents)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from nl2sql_agents.agents.discovery.discovery_agent import DiscoveryAgent
from nl2sql_agents.models.schemas import DiscoveryResult, TableMetaData
from tests.conftest import make_table, make_column


@pytest.fixture
def tables() -> list[TableMetaData]:
    """Concert-singer schema with FK relationships."""
    singer = make_table("singer", columns=[
        make_column("Singer_ID", pk=True),
        make_column("Name"),
        make_column("Country"),
    ])
    concert = make_table("concert", columns=[
        make_column("concert_ID", pk=True),
        make_column("Stadium_ID", fk=True, ref_table="stadium", ref_col="Stadium_ID"),
    ])
    stadium = make_table("stadium", columns=[
        make_column("Stadium_ID", pk=True),
        make_column("Location"),
        make_column("Capacity"),
    ])
    singer_in_concert = make_table("singer_in_concert", columns=[
        make_column("concert_ID", fk=True, ref_table="concert", ref_col="concert_ID"),
        make_column("Singer_ID", fk=True, ref_table="singer", ref_col="Singer_ID"),
    ])
    return [singer, concert, stadium, singer_in_concert]


def _mock_embed_documents(texts: list[str]) -> list[list[float]]:
    """Return deterministic fake embeddings: query gets [1,0,...], tables get varying."""
    import random
    result = []
    for i, text in enumerate(texts):
        # First text is the query
        if i == 0:
            result.append([1.0, 0.0, 0.0, 0.0])
        else:
            # Give each table a different "similarity" to the query
            random.seed(hash(text) % 2**32)
            result.append([random.random() for _ in range(4)])
    return result


class TestDiscoveryPipeline:
    @pytest.mark.asyncio
    async def test_discovery_returns_discovery_result(self, tables):
        """Mocked semantic agent → the pipeline produces a DiscoveryResult."""
        mock_embeddings = MagicMock()
        mock_embeddings.aembed_documents = AsyncMock(side_effect=_mock_embed_documents)

        with patch(
            "nl2sql_agents.agents.discovery.semantic_agent.EMBEDDING_PROVIDER"
        ) as mock_provider:
            mock_provider.embeddings_model.return_value = mock_embeddings

            agent = DiscoveryAgent()
            agent.semantic_agent.embeddings = mock_embeddings

            result = await agent.run(tables, "show me all singers", pre_filter_n=10)

        assert isinstance(result, DiscoveryResult)
        assert len(result.scored_tables) > 0
        assert len(result.top_tables) > 0

    @pytest.mark.asyncio
    async def test_singer_ranked_high_for_singer_query(self, tables):
        mock_embeddings = MagicMock()
        mock_embeddings.aembed_documents = AsyncMock(side_effect=_mock_embed_documents)

        with patch(
            "nl2sql_agents.agents.discovery.semantic_agent.EMBEDDING_PROVIDER"
        ) as mock_provider:
            mock_provider.embeddings_model.return_value = mock_embeddings

            agent = DiscoveryAgent()
            agent.semantic_agent.embeddings = mock_embeddings

            result = await agent.run(tables, "show me all singers", pre_filter_n=10)

        # singer should be among the top tables (keyword + FK boost)
        top_names = [t.table_name for t in result.top_tables]
        assert "singer" in top_names

    @pytest.mark.asyncio
    async def test_pre_filter_reduces_tables(self, tables):
        mock_embeddings = MagicMock()
        mock_embeddings.aembed_documents = AsyncMock(side_effect=_mock_embed_documents)

        with patch(
            "nl2sql_agents.agents.discovery.semantic_agent.EMBEDDING_PROVIDER"
        ) as mock_provider:
            mock_provider.embeddings_model.return_value = mock_embeddings

            agent = DiscoveryAgent()
            agent.semantic_agent.embeddings = mock_embeddings

            # pre_filter_n=2 → only top 2 keyword matches pass to semantic+FK
            result = await agent.run(tables, "show me all singers", pre_filter_n=2)

        # Should still produce results (<=2 tables sent to semantic)
        assert len(result.scored_tables) <= 2

    @pytest.mark.asyncio
    async def test_all_scored_tables_have_scores(self, tables):
        mock_embeddings = MagicMock()
        mock_embeddings.aembed_documents = AsyncMock(side_effect=_mock_embed_documents)

        with patch(
            "nl2sql_agents.agents.discovery.semantic_agent.EMBEDDING_PROVIDER"
        ) as mock_provider:
            mock_provider.embeddings_model.return_value = mock_embeddings

            agent = DiscoveryAgent()
            agent.semantic_agent.embeddings = mock_embeddings

            result = await agent.run(tables, "concerts in stadiums", pre_filter_n=10)

        for st in result.scored_tables:
            assert st.score >= 0.0
            assert isinstance(st.table, TableMetaData)
