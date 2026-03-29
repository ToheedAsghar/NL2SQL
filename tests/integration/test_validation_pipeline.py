"""Integration test for the Validation pipeline (4-stage validator)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from nl2sql_agents.agents.validator.validator_agent import ValidatorAgent
from nl2sql_agents.models.schemas import (
    GenerationResult,
    SQLCandidate,
    ValidationResult,
)


def _make_candidate(sql: str, variant: str = "conservative", temp: float = 0.3) -> SQLCandidate:
    return SQLCandidate(sql=sql, temperature=temp, prompt_variant=variant)


def _make_generation(*sqls: str) -> GenerationResult:
    variants = ["conservative", "creative", "rephrased"]
    temps = [0.3, 0.7, 0.5]
    candidates = [
        _make_candidate(sql, variants[i % 3], temps[i % 3])
        for i, sql in enumerate(sqls)
    ]
    return GenerationResult(candidates=candidates)


def _mock_llm_pass(response: str = "PASS"):
    """Patch ChatOpenAI.ainvoke to return a canned PASS response."""
    fake = MagicMock()
    fake.content = response
    fake.usage_metadata = {"input_tokens": 10, "output_tokens": 5}
    return patch("langchain_openai.ChatOpenAI.ainvoke", new_callable=AsyncMock, return_value=fake)


class TestValidationPipeline:
    @pytest.mark.asyncio
    async def test_valid_select_passes(self):
        generation = _make_generation("SELECT * FROM singer WHERE Country = 'France'")

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "singers from France")

        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert result.best_candidate is not None
        assert "singer" in result.best_candidate.sql.lower()

    @pytest.mark.asyncio
    async def test_multiple_candidates_selects_best(self):
        generation = _make_generation(
            "SELECT * FROM singer WHERE Country = 'France'",
            "SELECT Name FROM singer WHERE Country = 'France'",
            "SELECT s.Name FROM singer s WHERE s.Country = 'France'",
        )

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "singers from France")

        assert result.passed is True
        assert len(result.all_results) == 3

    @pytest.mark.asyncio
    async def test_dangerous_sql_blocked(self):
        generation = _make_generation("DROP TABLE singer")

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "delete everything")

        # Security validator should hard-fail this
        assert result.passed is False
        assert result.retry_context is not None

    @pytest.mark.asyncio
    async def test_insert_blocked(self):
        generation = _make_generation("INSERT INTO singer VALUES (99, 'Evil', 'Nowhere', 0)")

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "add a singer")

        assert result.passed is False

    @pytest.mark.asyncio
    async def test_mixed_candidates_picks_safe_one(self):
        """One dangerous + one safe → should pick the safe one."""
        generation = _make_generation(
            "DROP TABLE singer",
            "SELECT * FROM singer",
        )

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "show singers")

        assert result.passed is True
        assert "SELECT" in result.best_candidate.sql.upper()
        assert "DROP" not in result.best_candidate.sql.upper()

    @pytest.mark.asyncio
    async def test_all_results_tracked(self):
        generation = _make_generation(
            "SELECT 1",
            "SELECT 2",
        )

        with _mock_llm_pass("PASS"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "anything")

        assert len(result.all_results) == 2
        for r in result.all_results:
            assert len(r.checks) == 4  # security, syntax, logic, performance

    @pytest.mark.asyncio
    async def test_llm_fail_does_not_crash(self):
        """When LLM says FAIL, soft-fail validators degrade gracefully."""
        generation = _make_generation("SELECT * FROM singer")

        with _mock_llm_pass("FAIL: logic looks wrong"):
            validator = ValidatorAgent()
            result = await validator.validate(generation, "show singers")

        # Security (no-LLM) should still pass; syntax structural check passes
        # Logic and performance will FAIL but they're soft-fail
        # The candidate shouldn't be disqualified since security+syntax pass
        assert isinstance(result, ValidationResult)
