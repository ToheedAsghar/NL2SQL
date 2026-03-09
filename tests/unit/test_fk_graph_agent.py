"""Tests for FKGraphAgent (nl2sql_agents/agents/discovery/fk_graph_agent.py)."""

from __future__ import annotations

import pytest

from nl2sql_agents.agents.discovery.fk_graph_agent import FKGraphAgent
from tests.conftest import make_table, make_column


@pytest.fixture
def agent() -> FKGraphAgent:
    return FKGraphAgent()


@pytest.fixture
def tables():
    """Concert-singer schema with foreign keys."""
    singer = make_table("singer", columns=[
        make_column("Singer_ID", pk=True),
        make_column("Name"),
    ])
    concert = make_table("concert", columns=[
        make_column("concert_ID", pk=True),
        make_column("Stadium_ID", fk=True, ref_table="stadium", ref_col="Stadium_ID"),
    ])
    stadium = make_table("stadium", columns=[
        make_column("Stadium_ID", pk=True),
        make_column("Location"),
    ])
    singer_in_concert = make_table("singer_in_concert", columns=[
        make_column("concert_ID", fk=True, ref_table="concert", ref_col="concert_ID"),
        make_column("Singer_ID", fk=True, ref_table="singer", ref_col="Singer_ID"),
    ])
    return [singer, concert, stadium, singer_in_concert]


# ---------------------------------------------------------------------------
# Graph building
# ---------------------------------------------------------------------------

class TestBuildFKGraph:
    def test_all_tables_present(self, agent, tables):
        graph = agent._build_fk_graph(tables)
        assert set(graph.keys()) == {"singer", "concert", "stadium", "singer_in_concert"}

    def test_bidirectional_edges(self, agent, tables):
        graph = agent._build_fk_graph(tables)
        # concert → stadium, so stadium should also → concert
        assert "stadium" in graph["concert"]
        assert "concert" in graph["stadium"]

    def test_singer_in_concert_links(self, agent, tables):
        graph = agent._build_fk_graph(tables)
        assert "concert" in graph["singer_in_concert"]
        assert "singer" in graph["singer_in_concert"]
        # bidirectional
        assert "singer_in_concert" in graph["concert"]
        assert "singer_in_concert" in graph["singer"]

    def test_no_fk_means_no_edges(self, agent):
        isolated = [make_table("lone", columns=[make_column("id", pk=True)])]
        graph = agent._build_fk_graph(isolated)
        assert graph["lone"] == set()


# ---------------------------------------------------------------------------
# Seed finding
# ---------------------------------------------------------------------------

class TestFindSeeds:
    def test_finds_singer_seed(self, agent, tables):
        seeds = agent._find_seeds(tables, "show me all singers")
        assert "singer" in seeds

    def test_finds_concert_seed(self, agent, tables):
        seeds = agent._find_seeds(tables, "list concerts")
        assert "concert" in seeds

    def test_finds_multiple_seeds(self, agent, tables):
        seeds = agent._find_seeds(tables, "singers and concerts")
        assert "singer" in seeds
        assert "concert" in seeds

    def test_no_seeds_for_unrelated_query(self, agent, tables):
        seeds = agent._find_seeds(tables, "astrophysics equations")
        assert seeds == []

    def test_short_parts_ignored(self, agent):
        """Table name parts <= 2 chars should not seed."""
        t = make_table("ab_something", columns=[])
        seeds = agent._find_seeds([t], "something about ab")
        # "ab" is too short, but "something" should match
        assert "ab_something" in seeds


# ---------------------------------------------------------------------------
# BFS scoring
# ---------------------------------------------------------------------------

class TestBFSScore:
    def test_seed_gets_score_1(self, agent):
        graph = {"a": {"b"}, "b": {"a"}}
        scores = agent._bfs_score(["a"], graph, max_depth=2)
        assert scores["a"] == 1.0

    def test_neighbor_gets_half(self, agent):
        graph = {"a": {"b"}, "b": {"a"}}
        scores = agent._bfs_score(["a"], graph, max_depth=2)
        assert scores["b"] == 0.5

    def test_two_hops_gets_quarter(self, agent):
        graph = {"a": {"b"}, "b": {"a", "c"}, "c": {"b"}}
        scores = agent._bfs_score(["a"], graph, max_depth=2)
        assert scores["c"] == 0.25

    def test_beyond_max_depth_not_scored(self, agent):
        graph = {"a": {"b"}, "b": {"a", "c"}, "c": {"b", "d"}, "d": {"c"}}
        scores = agent._bfs_score(["a"], graph, max_depth=2)
        assert "d" not in scores

    def test_multiple_seeds_take_max(self, agent):
        graph = {"a": {"b"}, "b": {"a", "c"}, "c": {"b"}}
        # Both a and c are seeds; b is 1-hop from both → score 0.5
        # But if c is a seed too, c gets 1.0
        scores = agent._bfs_score(["a", "c"], graph, max_depth=2)
        assert scores["a"] == 1.0
        assert scores["c"] == 1.0
        assert scores["b"] == 0.5

    def test_empty_seeds(self, agent):
        graph = {"a": set()}
        scores = agent._bfs_score([], graph, max_depth=2)
        assert scores == {}


# ---------------------------------------------------------------------------
# End-to-end score()
# ---------------------------------------------------------------------------

class TestFKGraphScore:
    @pytest.mark.asyncio
    async def test_singer_query_scores_singer_highest(self, agent, tables):
        scores = await agent.score(tables, "show me all singers")
        assert scores.get("singer", 0) >= scores.get("concert", 0)
        assert scores.get("singer", 0) >= scores.get("stadium", 0)

    @pytest.mark.asyncio
    async def test_concert_query_includes_stadium(self, agent, tables):
        scores = await agent.score(tables, "list all concerts")
        # concert is seed (1.0), stadium is 1-hop via FK (0.5)
        assert scores.get("concert", 0) == 1.0
        assert scores.get("stadium", 0) == 0.5

    @pytest.mark.asyncio
    async def test_unrelated_query_empty_scores(self, agent, tables):
        scores = await agent.score(tables, "quantum physics research")
        # No seeds → no scores
        assert all(v == 0 for v in scores.values()) or len(scores) == 0
