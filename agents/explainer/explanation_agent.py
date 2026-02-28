"""
Explanation Agent - Translates SQL into plain English
"""

import logging
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a helpful data analyst explaining SQL to a business user.
Given a SQL query and the original question, explain in 2-4 plain English sentences what the query does. Do not include SQL syntax in your explanation."""


class ExplanationAgent(BaseAgent):
    def build_prompt(self, sql: str = "", user_query: str = "", **_) -> list[dict[str, str]]:
        USER_PROMPT = (
            f"Original question: {user_query}\n\n"
            f"SQL: \n{sql}\n\n"
            "Explain in plain English."
        )
        
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT}
        ]
    
    def parse_response(self, raw: str) -> str:
        return raw.strip()
    
    async def run(self, sql: str, user_query: str) -> str:
        messages = self.build_prompt(sql=sql, user_query=user_query)
        return await self.call_llm(messages, temperature=0.3, max_tokens=300)