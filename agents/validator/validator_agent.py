"""
Agent 4: Validator Agent

- For each SQL Candidate, 4 validators execute concurrently in parallel.
- Scores candidates, disqualifies hard-failures (security/syntax)
- selects the best passing candidate or return retry context
"""

import asyncio
import logging

from config.settings import VALIDATION_PROVIDER
from agents.validator.logic_validator import LogicValidator
from agents.validator.syntax_validator import SyntaxValidator
from agents.validator.security_validator import SecurityValidator
from agents.validator.performance_validator import PerformanceValidator

from models.schemas import (
    SQLCandidate, GenerationResult, CandidateValidationResult, ValidationResult
)


logger = logging.getLogger(__name__)
HARD_FAIL_CHECKS = {"security", "syntax"}

class ValidatorAgent:
    def __init__(self) -> None:
        self.security = SecurityValidator() # no llm
        self.syntax = SyntaxValidator(provider=VALIDATION_PROVIDER)
        self.logic = LogicValidator(provider=VALIDATION_PROVIDER)
        self.performance=PerformanceValidator(provider=VALIDATION_PROVIDER)

    async def validate(self, generation: GenerationResult, user_query: str) -> ValidationResult:
        logger.info("ValidatorAgent: %d candidates x 4 checks", len(generation.candidates))

        all_results = await asyncio.gather(
            *[
                self._validate_candidate(c, user_query) for c in generation.candidates
            ]
        )

        return self._select_best(list(all_results))
    
    async def _validate_candidate(self, candidate: SQLCandidate, user_query: str) -> CandidateValidationResult:
        # 4. checks in parallel for this one candidate

        sec, syn, logic, perf = await asyncio.gather(
            self.security.check(candidate.sql),
            self.syntax.check(candidate.sql),
            self.logic.check(candidate.sql, user_query),
            self.performance.check(candidate.sql)
        )

        checks = [sec, syn, logic, perf]

        disqualified = any(
            not c.passed for c in checks if c.check_name in HARD_FAIL_CHECKS
        )

        total = sum(c.score for c in checks) if not disqualified else 0.0

        return CandidateValidationResult(
            candidate=candidate,
            checks=checks,
            total_score=round(total, 4),
            disqualified=disqualified
        )

    def _select_best(self, results: list[CandidateValidationResult]) -> ValidationResult:
        eligible = [r for r in results if not r.disqualified]

        if not eligible:
            # build retry context from all failure reasons
            failures = []
            for r in results:
                for check in r.checks:
                    if not check.passed:
                        failures.append(
                            f"[{r.candidate.prompt_variant}] "
                            f"{check.check_name}: {check.details}" 
                        )
            retry_ctx = (
                "All Candidates failed. Fix these: \n"+"\n".join(failures)
            )

            logger.warning(
                "ValidatorAgent: All Candidates Failed ... Retry Needed"
            )

            return ValidationResult(
                passed=False,
                all_results = results,
                retry_ctx = retry_ctx
            )
        
        best = max(eligible, key=lambda r: r.total_score)

        logger.info("ValidatorAgent: winner %s score %.2f", best.candidate.prompt_variant, best.total_score)

        return ValidationResult(
            passed=True,
            best_candidate=best.candidate,
            all_results=results
        )
