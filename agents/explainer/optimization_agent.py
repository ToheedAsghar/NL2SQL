"""
OPTIMIZATION AGENT - LLM SUGGESTS INDEX/PERFORMANCE IMPROVMENTS
"""

import logging
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a database performance tuning expert.
Given a SQL query, suggest 1-3 concrete optimization hints.
Be specific and actionable. If well-optimized, say so.
Keep each hint to one sentence."""

class OptimizationAgent(BaseAgent):
    async def run(self, sql: str) -> str:
        messages = self.build_prompt(sql=sql)
        return await self.call_llm(
            messages=messages,
            temperature=0.2,
            max_tokens=200
        )
    
    def build_prompt(self, sql: str = "", **_) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Optimize this SQl: \n\n{sql}"}
        ]
    
    def parse_response(self, raw: str) -> str:
        return raw.strip()