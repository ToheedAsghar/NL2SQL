"""
Syntax Validator

Two-step:
1. Fast structural parse with SQLparse
2. LLM SQL syntax validation
"""

import logging
import sqlparse
from agents.base_agent import BaseAgent
from models.schemas import ValidatorCheckResult

logger = logging.getLogger()

SYSTEM_PROMPT = """You are a SQL syntax expert.
Check if the given SQL is syntactically valid.
Respond with ONLY one of:
PASS
FAIL: <brief reason>"""

class SyntaxValidator(BaseAgent):
    def _structural_check(self, sql: str) -> tuple[bool, str]:
        try:
            stms = sqlparse.parse(sql.strip())
            if not stms or not stms[0].tokens:
                return False, "Empty or unparsable SQL"
            return True, ""
        except Exception as e:
            return False, str(e)

    
    def build_prompt(self, sql: str = "", **_) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Check this SQL: \n\n{sql}"},
        ]
        
    
    async def check(self, sql: str) -> ValidatorCheckResult:
        
        # structual check by parsing the sql statements
        ok, reason = self._structural_check(sql)
        if not ok:
            return ValidatorCheckResult(
                check_name="syntax",
                passed=False,
                score=0.0,
                details=reason
            )
        
        # check through LLM
        messages = self.build_prompt(sql=sql)
        raw = self.call_llm(messages, temperature=0.0, max_tokens=100)
        return self.parse_response(raw)
    
    def parse_response(self, raw: str) -> ValidatorCheckResult:
        raw = raw.strip()
        if raw.upper().startswith('PASS'):
            return ValidatorCheckResult(
                check_name="syntax",
                passed=True,
                score=1.0,
                details="Syntax Valid"
            )
        
        reason = raw[5:].strip() if raw.upper().startswith('FAIL') else raw

        return ValidatorCheckResult(
            check_name="syntax",
            passed=False,
            score=0.0,
            details=reason
        )
