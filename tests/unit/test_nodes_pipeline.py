"""Tests for orchestrator nodes and pipeline wiring."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from nl2sql_agents.models.schemas import (
    CandidateValidationResult,
    FinalOutput,
    FormattedSchema,
    GenerationResult,
    GraphState,
    SQLCandidate,
    ValidationResult,
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


def _make_check(name: str, passed: bool = True, score: float = 1.0) -> ValidatorCheckResult:
    return ValidatorCheckResult(check_name=name, passed=passed, score=score, details="ok")


def _make_validation(passed: bool, sql: str = "SELECT * FROM singer") -> ValidationResult:
    candidate = _make_candidate(sql)
    checks = [
        _make_check("security"), _make_check("syntax"),
        _make_check("logic"), _make_check("performance"),
    ]
    cvr = CandidateValidationResult(
        candidate=candidate, checks=checks, total_score=4.0, disqualified=False,
    )
    return ValidationResult(
        passed=passed,
        best_candidate=candidate if passed else None,
        all_results=[cvr],
        retry_context=None if passed else "All candidates failed.",
    )


# ---------------------------------------------------------------------------
# should_retry
# ---------------------------------------------------------------------------

class TestShouldRetry:
    def test_returns_explain_when_passed(self):
        from nl2sql_agents.orchestrator.nodes import should_retry

        state: GraphState = {  # type: ignore[typeddict-item]
            "validation": _make_validation(passed=True),
            "attempt": 1,
        }
        assert should_retry(state) == "explain"

    def test_returns_generate_sql_when_failed_and_retries_left(self):
        from nl2sql_agents.orchestrator.nodes import should_retry, MAX_RETRIES

        state: GraphState = {  # type: ignore[typeddict-item]
            "validation": _make_validation(passed=False),
            "attempt": 1,
            "retry_context": "fix these",
        }
        assert should_retry(state) == "generate_sql"

    def test_returns_explain_when_retries_exhausted(self):
        from nl2sql_agents.orchestrator.nodes import should_retry, MAX_RETRIES

        state: GraphState = {  # type: ignore[typeddict-item]
            "validation": _make_validation(passed=False),
            "attempt": MAX_RETRIES + 2,  # exceed limit
            "retry_context": "fix these",
        }
        assert should_retry(state) == "explain"


# ---------------------------------------------------------------------------
# explain_node
# ---------------------------------------------------------------------------

class TestExplainNode:
    @pytest.mark.asyncio
    async def test_explain_node_builds_final_output(self):
        from nl2sql_agents.orchestrator.nodes import explain_node

        validation = _make_validation(passed=True)
        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "show singers",
            "validation": validation,
            "chat_history": [],
        }

        with _fake_llm("Mocked explanation"):
            result = await explain_node(state)

        assert "output" in result
        assert isinstance(result["output"], FinalOutput)
        assert result["output"].sql == "SELECT * FROM singer"
        assert "chat_history" in result
        assert len(result["chat_history"]) == 2  # user + assistant

    @pytest.mark.asyncio
    async def test_explain_node_appends_to_existing_history(self):
        from nl2sql_agents.orchestrator.nodes import explain_node

        validation = _make_validation(passed=True)
        existing_history = [
            {"role": "user", "content": "previous question"},
            {"role": "assistant", "content": "SELECT 1"},
        ]
        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "new question",
            "validation": validation,
            "chat_history": existing_history,
        }

        with _fake_llm("Explanation"):
            result = await explain_node(state)

        assert len(result["chat_history"]) == 4

    @pytest.mark.asyncio
    async def test_explain_node_no_prior_history(self):
        from nl2sql_agents.orchestrator.nodes import explain_node

        validation = _make_validation(passed=True)
        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "test",
            "validation": validation,
        }

        with _fake_llm("Answer"):
            result = await explain_node(state)

        assert len(result["chat_history"]) == 2


# ---------------------------------------------------------------------------
# validate_node (retry path)
# ---------------------------------------------------------------------------

class TestValidateNodeRetry:
    @pytest.mark.asyncio
    async def test_validate_failure_increments_attempt(self):
        from nl2sql_agents.orchestrator.nodes import validate_node

        failing_gen = GenerationResult(
            candidates=[_make_candidate("DROP TABLE singer")]
        )
        state: GraphState = {  # type: ignore[typeddict-item]
            "user_query": "drop stuff",
            "generation": failing_gen,
            "attempt": 1,
        }

        with _fake_llm("PASS"):
            result = await validate_node(state)

        assert result["validation"].passed is False
        assert result["attempt"] == 2
        assert result["retry_context"] is not None


# ---------------------------------------------------------------------------
# Pipeline graph structure
# ---------------------------------------------------------------------------

class TestPipelineStructure:
    def test_graph_compiles(self):
        """The LangGraph pipeline should compile without errors."""
        from nl2sql_agents.orchestrator.pipeline import build_graph

        graph = build_graph()
        assert graph is not None

    def test_graph_has_expected_nodes(self):
        from nl2sql_agents.orchestrator.pipeline import build_graph

        graph = build_graph()
        node_names = set(graph.get_graph().nodes.keys())
        expected = {
            "load_schema", "security_filter", "discovery",
            "gate", "format_schema", "generate_sql",
            "validate", "explain",
        }
        # LangGraph also adds __start__ and __end__
        for name in expected:
            assert name in node_names, f"Missing node: {name}"
