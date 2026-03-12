"""
LOGIC VALIDATOR - SOFT FAIL

LLm checks:
- correct tables joined
- correct filters / conditions
- correct aggregation
- result matches intent.
"""

import logging
from nl2sql_agents.agents.base_agent import BaseAgent
from nl2sql_agents.models.schemas import ValidatorCheckResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a STRICT SQL logic reviewer for the Spider evaluation dataset.
Given a database schema, a user question, and a SQL query, determine if the SQL correctly, completely, AND LITERALLY answers the question.

You must evaluate the query in TWO distinct phases:

PHASE 1: LOGICAL CORRECTNESS (The Basics)
- Does the query use the correct tables and columns to answer the user's intent?
- Are the WHERE clauses, JOIN conditions, and aggregations mathematically and logically accurate?
- If the basic logic is wrong, you must FAIL it.

PHASE 2: SPIDER STRICTNESS (The Constraints) 
If the query passes Phase 1, evaluate it against this strict checklist. Reject if it violates ANY rule:
1. Minimal Column Selection: Does the SELECT clause contain ONLY the exact columns asked for?
2. Strict JOIN Logic: Defaults to INNER JOIN? FAIL if it uses LEFT/OUTER JOIN unless prompt explicitly uses inclusive language ("all", "every").
3. No Unprompted Sorting: FAIL if it uses ORDER BY/LIMIT without explicit prompt keywords ("ordered", "top").
4. Superlatives: If prompt asks for "most", "least", "oldest", etc., it MUST use ORDER BY ... DESC/ASC LIMIT 1.
5. Clean Table Names: Avoids schema prefixes? (Use `table_name`, NOT `database_name.table_name`).
6. Strict Column Order: Columns in the SELECT clause MUST appear in the exact order requested in the prompt.
7. No Aliases: FAIL if the query uses ANY decorative aliases (e.g., AS name) in the SELECT clause.

OUTPUT FORMAT:
You MUST structure your response exactly like this to save tokens. Do not add conversational filler.

[REASONING]
Phase 1: <If correct, write exactly "Correct" and absolutely NOTHING else. If incorrect, briefly state the error.>
Phase 2: <List ONLY the violated rules and a 1-sentence reason. If no rules are violated, write exactly "All strictness rules passed.">
[VERDICT]
<Respond with strictly "PASS" or "FAIL: [reason]">"""

class LogicValidator(BaseAgent):
    async def check(self, sql: str, user_query: str) -> ValidatorCheckResult:
        messages = self.build_prompt(sql=sql, user_query=user_query)
        raw = await self.call_llm(messages, temperature=0.0, max_tokens=500)
        parsed_raw = self.parse_response(raw)
        
        return parsed_raw
    
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
        
        # 1. Isolate the verdict section from the reasoning block
        if "[VERDICT]" in raw:
            verdict_section = raw.split("[VERDICT]")[-1].strip()
        else:
            # Fallback just in case the LLM misses the formatting tag
            verdict_section = raw  
            
        # 2. Check for a passing grade
        if verdict_section.upper().startswith('PASS'):
            return ValidatorCheckResult(
                check_name="logic",
                passed=True,
                score=1.0,
                # Store the FULL raw text so you can read the reasoning in your logs
                details=raw 
            )
            
        # 3. Handle a failing grade
        # Extract the specific failure reason strictly from the verdict line
        if verdict_section.upper().startswith('FAIL'):
            reason = verdict_section[5:].strip() 
        else:
            reason = verdict_section
        
        return ValidatorCheckResult(
            check_name="logic",
            passed=False,
            score=0.0,
            # Pass the FULL raw text back so the Generator knows EXACTLY why it failed!
            details=raw 
        )