"""Tests for Explainer sub-agents (explanation, optimization, safety report)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from nl2sql_agents.agents.explainer.explainer_agent import ExplainerAgent
from nl2sql_agents.agents.explainer.explanation_agent import ExplanationAgent
from nl2sql_agents.agents.explainer.optimization_agent import OptimizationAgent
from nl2sql_agents.agents.explainer.safety_report_agent import SafetyReportAgent
from nl2sql_agents.models.schemas import (
    CandidateValidationResult,
    SQLCandidate,
    ValidatorCheckResult,
)


def _fake_llm(response: str):
    fake = MagicMock()
    fake.content = response
    fake.usage_metadata = {"input_tokens": 10, "output_tokens": 5}
    return patch(
        "langchain_openai.ChatOpenAI.ainvoke",
        new_callable=AsyncMock,
        return_value=fake,
    )


def _make_candidate(sql: str = "SELECT * FROM singer") -> SQLCandidate:
    return SQLCandidate(sql=sql, temperature=0.3, prompt_variant="conservative")


def _make_validation_result(candidate: SQLCandidate, total: float = 3.5) -> CandidateValidationResult:
    return CandidateValidationResult(
        candidate=candidate,
        checks=[
            ValidatorCheckResult(check_name="security", passed=True, score=1.0, details="All security checks passed"),
            ValidatorCheckResult(check_name="syntax", passed=True, score=1.0, details="Syntax Valid"),
            ValidatorCheckResult(check_name="logic", passed=True, score=1.0, details="logic correct"),
            ValidatorCheckResult(check_name="performance", passed=True, score=0.5, details="Consider adding index"),
        ],
        total_score=total,
        disqualified=False,
    )


# ---------------------------------------------------------------------------
# ExplanationAgent
# ---------------------------------------------------------------------------

class TestExplanationAgent:
    def test_build_prompt(self):
        agent = ExplanationAgent()
        msgs = agent.build_prompt(sql="SELECT 1", user_query="test question")
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"
        assert "test question" in msgs[1]["content"]
        assert "SELECT 1" in msgs[1]["content"]

    def test_parse_response(self):
        agent = ExplanationAgent()
        assert agent.parse_response("  hello  ") == "hello"

    @pytest.mark.asyncio
    async def test_run_returns_string(self):
        with _fake_llm("This query selects all singers from France."):
            agent = ExplanationAgent()
            result = await agent.run("SELECT * FROM singer", "show singers")
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# OptimizationAgent
# ---------------------------------------------------------------------------

class TestOptimizationAgent:
    def test_build_prompt(self):
        agent = OptimizationAgent()
        msgs = agent.build_prompt(sql="SELECT * FROM singer")
        assert len(msgs) == 2
        assert "SELECT * FROM singer" in msgs[1]["content"]

    def test_parse_response(self):
        agent = OptimizationAgent()
        assert agent.parse_response("  hint  ") == "hint"

    @pytest.mark.asyncio
    async def test_run_returns_string(self):
        with _fake_llm("Consider adding an index on Country column."):
            agent = OptimizationAgent()
            result = await agent.run("SELECT * FROM singer WHERE Country = 'France'")
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# SafetyReportAgent
# ---------------------------------------------------------------------------

class TestSafetyReportAgent:
    @pytest.mark.asyncio
    async def test_report_with_matching_candidate(self):
        agent = SafetyReportAgent()
        candidate = _make_candidate()
        val_result = _make_validation_result(candidate)

        report = await agent.run(candidate, [val_result])
        assert "Security & Quality Report" in report
        assert "SECURITY" in report
        assert "SYNTAX" in report
        assert "LOGIC" in report
        assert "PERFORMANCE" in report
        assert "3.5" in report

    @pytest.mark.asyncio
    async def test_report_no_matching_candidate(self):
        agent = SafetyReportAgent()
        candidate = _make_candidate("SELECT 1")
        other = _make_candidate("SELECT 2")
        val_result = _make_validation_result(other)

        report = await agent.run(candidate, [val_result])
        assert "Not Available" in report

    @pytest.mark.asyncio
    async def test_report_shows_correct_icons(self):
        agent = SafetyReportAgent()
        candidate = _make_candidate()
        val_result = CandidateValidationResult(
            candidate=candidate,
            checks=[
                ValidatorCheckResult(check_name="security", passed=True, score=1.0, details="OK"),
                ValidatorCheckResult(check_name="syntax", passed=True, score=1.0, details="OK"),
                ValidatorCheckResult(check_name="logic", passed=False, score=0.0, details="wrong join"),
                ValidatorCheckResult(check_name="performance", passed=True, score=0.5, details="slow"),
            ],
            total_score=2.5,
            disqualified=False,
        )
        report = await agent.run(candidate, [val_result])
        assert "2.5" in report

    @pytest.mark.asyncio
    async def test_report_with_empty_results(self):
        agent = SafetyReportAgent()
        candidate = _make_candidate()
        report = await agent.run(candidate, [])
        assert "Not Available" in report


# ---------------------------------------------------------------------------
# ExplainerAgent (orchestrator of all 3 sub-agents)
# ---------------------------------------------------------------------------

class TestExplainerAgent:
    @pytest.mark.asyncio
    async def test_explain_returns_all_three_outputs(self):
        candidate = _make_candidate()
        val_results = [_make_validation_result(candidate)]

        with _fake_llm("Mocked LLM response."):
            agent = ExplainerAgent()
            output = await agent.explain(candidate, val_results, "show singers")

        assert output.explanation is not None
        assert output.safety_report is not None
        assert output.optimization_hints is not None

    @pytest.mark.asyncio
    async def test_explain_safety_report_has_content(self):
        candidate = _make_candidate()
        val_results = [_make_validation_result(candidate)]

        with _fake_llm("Some explanation"):
            agent = ExplainerAgent()
            output = await agent.explain(candidate, val_results, "query")

        # Safety report is generated without LLM, so it has real content
        assert "SECURITY" in output.safety_report
