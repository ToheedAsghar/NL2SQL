"""
Agent 3 - QUERY GENERATOR (LLM)

- Generates N SQL Candidates in parallel
- Each candidate uses a different temperature / prompt vairent to maximise diversity (best-of-N pattern).
"""

import asyncio
import logging
from nl2sql_agents.agents.base_agent import BaseAgent
from nl2sql_agents.config.settings import N_CANDIDATES, CANDIDATE_TEMPERATURES
from nl2sql_agents.models.schemas import ChatMessage, FormattedSchema, GenerationResult, SQLCandidate

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an expert SQL developer.
Given a database schema and a natural language question, write a single SQL SELECT query that answers the question. You must act as a strict translator, not a helpful data analyst. Do not make assumptions, do not add context, and do not format the output for human readability. Your SQL must match the exact logical intent of the prompt with zero additions.

RULES:
- Output ONLY the SQL query, no explanation.
- Use standard SQL syntax compatible with the target database.
- Only SELECT - no INSERT, UPDATE, DELETE, DROP, or DDL.
- Use table aliases for readability.
- Handle NULLs appropriately.
- DO NOT use database or schema prefixes for table names. Use naked table names only (e.g., use FROM table_name, NOT FROM db_name.table_name).
- ONLY select the exact columns requested. Do not add primary keys, foreign keys, or descriptive columns unless explicitly asked. If asked for an entity, default to its name column unless an id is specified.
- ALWAYS default to JOIN (Inner Join). ONLY use LEFT JOIN or OUTER JOIN if the prompt explicitly uses highly inclusive language (all, every, even if none). Phrases like "for each" implicitly mean INNER JOIN.
- DO NOT use ORDER BY or LIMIT unless explicitly requested by words like ordered, sorted, top, maximum, or a specific number.
- Prefer COUNT(*) for simple counting with INNER JOINs. Do not use DISTINCT unless uniqueness is implied.
- NO ALIASES: DO NOT use decorative aliases (AS alias_name) for any columns or aggregations in the SELECT clause.
- When the prompt asks for "most", "least", "oldest", "youngest", "largest", or "smallest", you MUST use ORDER BY ... DESC/ASC LIMIT 1. Do not write CTEs or subqueries to handle ties unless the prompt explicitly says "including ties" or "all [entities] that share the maximum".
- STRICT COLUMN ORDER: You MUST place columns in the SELECT clause in the EXACT order they are mentioned in the natural language prompt. Do not automatically put GROUP BY columns first.

CONTRASTIVE EXAMPLES:

Prompt: "List the maximum weight and pet type."
INCORRECT (Pre-training bias): SELECT PetType, MAX(weight) FROM Pets GROUP BY PetType
CORRECT (Strict order): SELECT MAX(weight), PetType FROM Pets GROUP BY PetType

Prompt: "Find the number of concerts happened in the stadium with the highest capacity."
INCORRECT (Subqueries & aliases): SELECT COUNT(concert_ID) AS num FROM concert JOIN stadium ON concert.stadium_id = stadium.stadium_id WHERE capacity = (SELECT MAX(capacity) FROM stadium)
CORRECT (Strict LIMIT 1): SELECT count(*) FROM concert JOIN stadium ON concert.stadium_id = stadium.stadium_id ORDER BY stadium.capacity DESC LIMIT 1

Prompt: "For each stadium, how many concerts play there?"
INCORRECT (Too helpful / Left Join): SELECT stadium.Stadium_ID, stadium.Name, COUNT(concert.concert_ID) FROM stadium LEFT JOIN concert ON stadium.Stadium_ID = concert.Stadium_ID GROUP BY stadium.Stadium_ID
CORRECT (Strict Inner Join): SELECT stadium.name, count(*) FROM concert JOIN stadium ON concert.stadium_id = stadium.id GROUP BY concert.stadium_id
"""

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
            chat_history: list[ChatMessage] | None = None,
            **_,
        ) -> list[dict[str, str]]:

        messages: list[dict[str, str]] = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]

        if chat_history:
            for turn in chat_history[-6:]:
                messages.append({
                    "role": turn["role"], "content": turn["content"]
                })
        
        user_content = (
            f"Schema: \n{schema.content}\n\n"
            f"{prompt_varient}\n{user_query}"
        )

        if retry_content:
            user_content += (
                f"\n\nPrevious attempt failed. Fix these : \n{retry_content}"
            )
        
        messages.append({"role": "user", "content": user_content})

        logger.info("QueryGeneratorAgent: built promtpt with %d message history", len(messages))

        return messages
    
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
            chat_history: list[ChatMessage] | None = None,
    ) -> GenerationResult:
        """FIRE N LLM CALLS PARALLELY"""

        logger.info(
            "QueryGeneatorAgent: %d candidates in parallel", n_candidates
        )

        temps = CANDIDATE_TEMPERATURES[:n_candidates]
        varients = PROMPT_VARIENT[:n_candidates]

        candidates = await asyncio.gather(
            *[
                self._generator_one(schema, user_query, t, v, retry_context, chat_history) for t, v in zip(temps, varients)
            ]
        )

        logger.info("QueryGeneratorAggent: %d candidates ready", len(candidates))

        for candidate in candidates:
            logger.info(f"QueryGeneratorAgent: [{candidate.prompt_variant.upper()}] = %s", candidate.sql)

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
            chat_history: list[ChatMessage] | None = None,
    ) -> SQLCandidate:
        messages = self.build_prompt(
            schema=schema, user_query=user_query, prompt_varient=prompt_varient, retry_content=retry_context, chat_history=chat_history
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
