"""
Agent 3 - QUERY GENERATOR (LLM)

- Generates N SQL Candidates in parallel
- Each candidate uses a different temperature / prompt vairent to maximise diversity (best-of-N pattern).
"""

import asyncio
import logging
from agents.base_agent import BaseAgent
from config.settings import N_CANDIDATES, CANDIDATE_TEMPERATURES
from models.schemas import FormattedSchema, GenerationResult, SQLCandidate

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an expert SQL developer.
Given a database schema and a natural language question, write a single SQL SELECT query that answers the question.
Rules:
- Output ONLY the SQL query, no explanation.
- Use standard SQL syntax compatible with the target database.
- Only SELECT — no INSERT, UPDATE, DELETE, DROP, or DDL.
- Use table aliases for readability.
- Handle NULLs appropriately."""

PROMPT_VARIENT = [
    "Write a SQL Query to answer:",
    "Generate a precise SQL SELECT statement for:",
    "Produce a SQL query that accurately answers:"
]

class QueryGeneratorAgent(BaseAgent):
    def build_prompt(
            self,
            schema: FormattedSchema,
            user_query: str,
            prompt_varient: str = PROMPT_VARIENT[0],
            retry_content: str | None = None,
            **_,
        ) -> list[dict[str, str]]:
        user_content = (
            f"Schema: \n{schema.content}\n\n"
            f"{prompt_varient}\n{user_query}"
        )

        if retry_content:
            user_content += (
                f"\n\nPrevious attempt failed. Fix these : \n{retry_content}"
            )
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]
    
    def parse_response(self, raw: str) -> str:
        raw = raw.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(
                l for l in lines if not l.startswith('```')
            ).strip()

        return raw
    
    async def generate(
            self,
            schema: FormattedSchema,
            user_query: str,
            n_candidates: int = N_CANDIDATES,
            retry_context: str | None = None,
    ) -> GenerationResult:
        """FIRE N LLM CALLS PARALLELY"""

        logger.info(
            "QueryGeneatorAgent: %d candidates in paralle", n_candidates
        )

        temps = CANDIDATE_TEMPERATURES[:n_candidates]
        varients = PROMPT_VARIENT[:n_candidates]

        candidates = await asyncio.gather(
            *[
                self._generate_one(schema, user_query, t, v, retry_context) for t, v in zip(temps, varients)
            ]
        )

        logger.info("QueryGeneratorAggent: %d candidates ready", len(candidates))

        return GenerationResult(
            candidates=list(candidates)
        )

    async def _generator_one(
            self,
            schema: FormattedSchema,
            user_query: str,
            temperature: float,
            prompt_varient: str,
            retry_context: str | None,
    ) -> SQLCandidate:
        messages = self.build_prompt(
            schema=schema, user_query=user_query, prompt_varient=prompt_varient, retry_content=retry_context
        )

        raw = await self.call_llm(messages=messages, temperature=temperature)
        sql=self.parse_response(raw)

        label = {
            PROMPT_VARIENT[0]: "conservative",
            PROMPT_VARIENT[1]: "creative",
            PROMPT_VARIENT[2]: "rephrased",
        }.get(prompt_varient, "unknown")

        return SQLCandidate(
            sql=sql,
            temperature=temperature,
            prompt_variant=label
        )
