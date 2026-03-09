"""Tests for GateLayer (nl2sql_agents/filters/gate.py)."""

from __future__ import annotations

import pytest

from nl2sql_agents.filters.gate import GateLayer
from nl2sql_agents.models.schemas import ScoredTable, GateResult
from tests.conftest import make_table, make_column


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scored(name: str, score: float) -> ScoredTable:
    return ScoredTable(
        table=make_table(name, columns=[make_column("id", pk=True)]),
        score=score,
        found_by=["keyword"],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGateLayer:
    def setup_method(self):
        self.gate = GateLayer()

    # ── Security error ────────────────────────────────────────────────────
    def test_security_error_blocks(self):
        result = self.gate.evaluate(
            scored_tables=[_scored("t1", 0.9)],
            security_approved={"t1"},
            security_error=RuntimeError("boom"),
        )
        assert result.passed is False
        assert "Security Filter Error" in result.reason
        assert result.tables == []

    # ── Normal flow ───────────────────────────────────────────────────────
    def test_returns_top_k(self):
        scored = [_scored("a", 0.9), _scored("b", 0.7), _scored("c", 0.5)]
        result = self.gate.evaluate(
            scored_tables=scored,
            security_approved={"a", "b", "c"},
            top_k=2,
        )
        assert result.passed is True
        assert len(result.tables) == 2
        assert result.tables[0].table_name == "a"
        assert result.tables[1].table_name == "b"

    def test_filters_by_security_approved(self):
        scored = [_scored("a", 0.9), _scored("b", 0.7), _scored("c", 0.5)]
        result = self.gate.evaluate(
            scored_tables=scored,
            security_approved={"a", "c"},  # "b" not approved
            top_k=10,
        )
        assert result.passed is True
        names = [t.table_name for t in result.tables]
        assert "b" not in names
        assert "a" in names and "c" in names

    def test_no_accessible_tables(self):
        scored = [_scored("a", 0.9)]
        result = self.gate.evaluate(
            scored_tables=scored,
            security_approved=set(),  # nothing approved
            top_k=5,
        )
        assert result.passed is False
        assert "No Accessible tables" in result.reason

    def test_empty_scored_list(self):
        result = self.gate.evaluate(
            scored_tables=[],
            security_approved={"whatever"},
            top_k=5,
        )
        assert result.passed is False

    def test_top_k_larger_than_list(self):
        scored = [_scored("x", 0.8)]
        result = self.gate.evaluate(
            scored_tables=scored,
            security_approved={"x"},
            top_k=100,
        )
        assert result.passed is True
        assert len(result.tables) == 1

    def test_preserves_ranked_order(self):
        """Tables should come out in the same ranked order they were given."""
        scored = [_scored("first", 0.9), _scored("second", 0.5), _scored("third", 0.1)]
        result = self.gate.evaluate(
            scored_tables=scored,
            security_approved={"first", "second", "third"},
            top_k=3,
        )
        assert [t.table_name for t in result.tables] == ["first", "second", "third"]

    def test_default_top_k_uses_setting(self):
        """When top_k is not passed, it uses DISCOVERY_TOP_K from settings."""
        scored = [_scored(f"t{i}", 1.0 - i * 0.1) for i in range(20)]
        approved = {f"t{i}" for i in range(20)}
        result = self.gate.evaluate(scored_tables=scored, security_approved=approved)
        # DISCOVERY_TOP_K default is 10
        assert result.passed is True
        assert len(result.tables) <= 10
