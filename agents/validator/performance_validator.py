"""
PERFORMANCE VALIDATOR (SOFT FAIL)

LLm flags performance anti-patterns:
- Cartesian joins
- missing where on large tables
- Select *
- unbounded result sets
- Functions on indexed columns in WHERE

Returns PASS / WARN (score 0.5) / FAIL (score 0.0)
"""

import logging
from agents.base_agent import BaseAgent
from models.schemas import ValidatorCheckResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a SQL performance expert.
Review the SQL for performance issues:
- Cartesian joins or missing JOIN conditions
- Missing WHERE filters on large tables
- SELECT * usage
- Unbounded result sets (no LIMIT / FETCH FIRST)
- Functions on indexed columns in WHERE

Respond with ONLY one of:
PASS
WARN: <brief concern>
FAIL: <critical issue>"""

class PerformanceValidator(BaseAgent):
    async def check(self, sql: str) -> ValidatorCheckResult:
        messages = self.build_prompt(sql=sql)
        raw = await self.call_llm(messages, temperature=0.0, max_tokens=150)
        return self.parse_response(raw)

    def build_prompt(self, sql: str = "", **_) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Review for Performance: \n\n{sql}"},
        ]
    
    def parse_response(self, raw: str) -> ValidatorCheckResult:
        raw = raw.strip()
        upper = raw.upper()

        if upper.startswith('PASS'):
            return ValidatorCheckResult(
                check_name='performance',
                passed=True,
                score=1.0,
                details="No performance issue."
            )
        elif upper.startswith("WARN"):
            details = raw[5:].strip() if ":" in raw[:5] else raw

            return ValidatorCheckResult(
                check_name='performance',
                passed=True,
                score=0.5,
                details=details
            )
        else:
            details = raw[5:].strip() if upper.startswith("FAIL:") else raw

            return ValidatorCheckResult(
                check_name='performance',
                passed=False,
                score=0.0,
                details=details
            ) 

