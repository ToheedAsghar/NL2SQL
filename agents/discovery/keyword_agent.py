"""
Sub-Agent 1a - Keyword Agent (No LLM)

Extract Meaningful tokens from user query and fuzzy-matches aginst table names and column names. Returns {tablename: score}

"""

import re
import logging
from difflib import SequenceMatcher
from models.schemas import TableMetaData

logger = logging.getLogger(__name__)

STOP_WORDS = {
    "show", "me", "get", "find", "list", "give", "the", "a", "an",
    "of", "for", "in", "on", "by", "with", "from", "where", "top",
    "all", "my", "this", "that", "and", "or", "is", "are", "was",
    "how", "many", "much", "what", "which", "who",
}

class KeywordAgent:
    async def score(
            self,
            tables: list[TableMetaData],
            user_query: str
    ) -> dict[str, float]:
        keywords = self._extract_keywords(user_query)
        logger.debug('KeywordAgent: Keywords=%s', keywords)

        return {
            t.table_name: self._score_table(t, keywords)
            for t in tables
        }
    
    def _extract_keywords(self, query: str) -> list[str]:
        tokens = re.findall(r"[a-zA-Z]+", query.lower())
        return [t for t in tokens if t not in STOP_WORDS and len(t) > 2]
    
    def _score_table(self, table: TableMetaData, keywords: list[str]) -> float:
        if not keywords:
            return 0.0
        
        candidates = [table.table_name.lower()] + [
            c.column_name.lower() for c in table.columns
        ]

        best_scores = []
        for kw in keywords:
            kw_best = max(
                self._fuzzy_score(kw, candidate)
                for candidate in candidates
            )
            best_scores.append(kw_best)

        return round(sum(best_scores) / len(best_scores), 4)
    
    def _fuzzy_score(self, keyword: str, target: str) -> float:
        if keyword in target:
            return 1.0
        return SequenceMatcher(None, keyword, target).ratio()