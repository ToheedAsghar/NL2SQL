"""
SAFETY REPORT AGENT - Generates structured audit report from validation results.

No LLM calls - purely derived from check date.
"""

import logging
from models.schemas import SQLCandidate, CandidateValidationResult

logger = logging.getLogger(__name__)

ICONS={"passed": "", "warned": " ", "failed":""}
CHECK_ORDER = ["security", "syntax", "logic", "performance"]

class SafetyReportAgent:
    async def run(self, candidate: SQLCandidate, all_results: list[CandidateValidationResult]) -> str:
        winning = next(
            (r for r in all_results if r.candidate.sql == candidate.sql), None
        )

        if not winning:
            logger.warning("SafetyReportAgent: Safety Report Not Available")
            return "Safety Report Not Available"
        
        lines = ["Security & Quality Report", " "*36]

        for check in sorted(winning.checks, key=lambda c: CHECK_ORDER.index(c.check_name)):
            if check.passed and check.score==1.0:
                icon = ICONS['passed']
            elif check.passed and check.score<1.0:
                icon = ICONS['warned']
            else:
                icon = ICONS["failed"]

            lines.append(
                f"{icon} {check.check_name.upper():12}  {check.details or ''}"
            )

        lines.append(" "*36)

        lines.append(f"Total score: {winning.total_score:.1f} / 4.0")
        return "\n".join(lines)