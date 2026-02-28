"""
LOGIC VALIDATOR - SOFT FAIL

LLm checks:
- correct tables joined
- correct filters / conditions
- correct aggregation
- result matches intent.
"""

import logging
from agents.base_agent import BaseAgent
from models.schemas import ValidatorCheckResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a SQL logic reviewer.
Given a user question and a SQL query, determine if the SQL
correctly and completely answers the question.

Respond with ONLY one of:
PASS
FAIL: <brief reason>"""

class LogicValidator(BaseAgent):
    async def check(self, sql: str, user_query: str) -> ValidatorCheckResult:
        messages = self.build_prompt(sql=sql, user_query=user_query)
        raw = await self.call_llm(messages, temperature=0.0, max_tokens=150)
        return self.parse_response(raw)
    
    def build_prompt(self, sql: str = "", user_query: str = "", **_) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": (
                f"User Question: {user_query}\n\n"
                f"SQL: \n{sql}"
            )},
        ]

    def parse_response(self, raw: str) -> ValidatorCheckResult:
        raw = raw.strip()

        if raw.upper().startswith('PASS'):
            return ValidatorCheckResult(
                check_name="logic",
                passed=True,
                score=1.0,
                details="logic correct"
            )
    
        reason = raw[5:].strip() if raw.upper().startswith('FAIL') else raw

        return ValidatorCheckResult(
            check_name="logic",
            passed=False,
            score=0.0,
            details=reason
        )
        