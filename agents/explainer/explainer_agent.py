"""
AGENT 5 - EXPLAINER AGENT

runs PARALLEL - 3 output tasks concurrently:
- Explanation (Plain English)
- Safety Report (audit from validation report)
- Optimization Hints (LLM)
"""

import asyncio
import logging

from agents.explainer.explanation_agent import ExplanationAgent
from agents.explainer.optimization_agent import OptimizationAgent
from agents.explainer.safety_report_agent import SafetyReportAgent
from models.schemas import SQLCandidate, CandidateValidationResult, ExplainerOutput

logger = logging.getLogger(__name__)

class ExplainerAgent:
    def __init__(self) -> None:
        self.explanation = ExplanationAgent()
        self.safety_report = SafetyReportAgent()
        self.optimization = OptimizationAgent()

    async def explain(
            self,
            candidate: SQLCandidate,
            validation_results: list[CandidateValidationResult],
            user_query: str = ""
    ) -> ExplainerOutput:
        """PARALLEL"""
        logger.info("ExplainerAgent: 3 output taaks in PARALLEL")

        explanation, safety, hints = await asyncio.gather(
            self.explanation.run(candidate.sql, user_query),
            self.safety_report.run(candidate, validation_results),
            self.optimization.run(candidate.sql)
        )

        return ExplainerOutput(
            explanation=explanation,
            safety_report=safety,
            optimization_hints = hints
        )
