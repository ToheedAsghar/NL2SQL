"""Tests for KeywordAgent (nl2sql_agents/agents/discovery/keyword_agent.py)."""

from __future__ import annotations

import pytest

from nl2sql_agents.agents.discovery.keyword_agent import KeywordAgent, STOP_WORDS
from tests.conftest import make_table, make_column


@pytest.fixture
def agent() -> KeywordAgent:
    return KeywordAgent()


@pytest.fixture
def tables():
    return [
        make_table("singer", columns=[
            make_column("Singer_ID"), make_column("Name"), make_column("Country"),
        ]),
        make_table("concert", columns=[
            make_column("concert_ID"), make_column("concert_Name"), make_column("Theme"),
        ]),
        make_table("stadium", columns=[
            make_column("Stadium_ID"), make_column("Location"), make_column("Capacity"),
        ]),
        make_table("singer_in_concert", columns=[
            make_column("concert_ID"), make_column("Singer_ID"),
        ]),
    ]


class TestKeywordExtraction:
    def test_removes_stop_words(self, agent):
        kws = agent._extract_keywords("show me all singers from France")
        for sw in ("show", "me", "all", "from"):
            assert sw not in kws

    def test_keeps_meaningful_words(self, agent):
        kws = agent._extract_keywords("show me all singers from France")
        assert "singers" in kws
        assert "france" in kws

    def test_filters_short_tokens(self, agent):
        kws = agent._extract_keywords("an a is ok no ID")
        # "ok" and "ID" have len <= 2 and should be filtered out
        for tok in kws:
            assert len(tok) > 2

    def test_empty_query(self, agent):
        assert agent._extract_keywords("") == []

    def test_numeric_tokens_ignored(self, agent):
        kws = agent._extract_keywords("top 5 stadiums by capacity")
        # Only alpha tokens are extracted
        assert "5" not in kws


class TestKeywordScoring:
    @pytest.mark.asyncio
    async def test_exact_match_scores_high(self, agent, tables):
        scores = await agent.score(tables, "singer")
        # "singer" table should score highest
        assert scores["singer"] >= scores["concert"]
        assert scores["singer"] >= scores["stadium"]

    @pytest.mark.asyncio
    async def test_column_match_boosts_table(self, agent, tables):
        scores = await agent.score(tables, "what is the capacity of stadiums")
        # "capacity" is a column in stadium → stadium should score well
        assert scores["stadium"] > 0.0

    @pytest.mark.asyncio
    async def test_no_match_gives_low_score(self, agent, tables):
        scores = await agent.score(tables, "completely unrelated astrophysics query")
        # All scores should be low (fuzzy might give small nonzero)
        for s in scores.values():
            assert s < 0.6

    @pytest.mark.asyncio
    async def test_empty_query_all_zero(self, agent, tables):
        scores = await agent.score(tables, "the a an")
        # All stopwords → no keywords → all 0
        for s in scores.values():
            assert s == 0.0

    @pytest.mark.asyncio
    async def test_partial_match(self, agent, tables):
        scores = await agent.score(tables, "concerts held last year")
        # "concerts" fuzzy-matches "concert" table (fuzzy score ~0.49)
        assert scores["concert"] > 0.4

    @pytest.mark.asyncio
    async def test_returns_dict_for_all_tables(self, agent, tables):
        scores = await agent.score(tables, "singer")
        assert set(scores.keys()) == {t.table_name for t in tables}


class TestFuzzyScore:
    def test_substring_match_returns_1(self, agent):
        assert agent._fuzzy_score("singer", "singer") == 1.0

    def test_substring_in_longer_string(self, agent):
        assert agent._fuzzy_score("singer", "singer_in_concert") == 1.0

    def test_no_match_returns_low(self, agent):
        score = agent._fuzzy_score("xyz", "abc")
        assert score < 0.5

    def test_partial_overlap(self, agent):
        score = agent._fuzzy_score("concert", "concerts")
        assert score > 0.7
