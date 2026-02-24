"""
DISCOVERY AGENT

1. Keyword pre-filter (no llm)
    - runs KeywordAgent on all tables
    - Takes top KEYWORD_PRE_FILTER_TOP_N by keyword score
2. PARALLEL - Semantic + FK (on filtered tables only.)
    - runs SemanticAgent + FKGraphAgent in parallel on filtered tables
    - Merges all 3 scores with configurable weights
    - returns full ranked list
"""

import logging
import asyncio
from collections import defaultdict

from discovery.keyword_agent import KeywordAgent
from discovery.fk_graph_agent import FKGraphAgent
from discovery.semantic_agent import SemanticAgent
from config.settings import KEYWORD_PRE_FILTER_TOP_N
from models.schemas import TableMetaData, DiscoveryResult, ScoredTable

logger = logging.getLogger(__name__)

WEIGHTS = {
    "keywords" : 0.35,
    "semantic" : 0.45,
    "fk_graph" : 0.20
}

class DiscoveryAgent:
    def __init__(self) -> None:
        self.keyword_agent = KeywordAgent()
        self.semantic_agent = SemanticAgent() 
        self.fk_graph_agent = FKGraphAgent()

    async def run(
            self,
            tables: list[TableMetaData],
            user_query: str,
            pre_filter_n: int = KEYWORD_PRE_FILTER_TOP_N
    ) -> DiscoveryResult:
        
        # 1. keyword pre-filter
        
        logger.info('DiscoveryAgent: Keyword prefilter on %d tables', len(tables))

        kw_scores = await self.keyword_agent.score(tables, user_query)
        sorted_by_kw = sorted(kw_scores.items(), key=lambda x:x[1], reverse=True)
        top_n_names = {name for name, _ in sorted_by_kw[:pre_filter_n]}
        pre_filtered = [t for t in tables if t.table_name in top_n_names]

        logger.info('DiscoveryAgent phase 1: %d -> %d tables', len(tables), len(pre_filtered))

        #2. semantic + FK in parallel

        logger.info('DiscoveryAgent phase2: Semantic + FK in parallel on %d tables', len(pre_filtered))

        sem_scores, fk_scores = await asyncio.gather(
            self.semantic_agent.score(pre_filtered, user_query),
            self.fk_graph_agent.score(pre_filtered, user_query)
        )

        merged = self._merge_and_rank(pre_filtered, kw_scores, sem_scores, fk_scores)

        logger.info('Discover Agent: ranked %d tables, top-5 = %s', len(merged), [s.table.table_name for s in merged[:5]])

        return DiscoveryResult(
            top_tables=[s.table for s in merged[:5]],
            scored_tables=merged
        )

    def _merge_and_rank(
            self,
            tables: list[TableMetaData],
            kw: dict[str, float],
            sem: dict[str, float],
            fk: dict[str, float]
    ) -> list[ScoredTable]:
        agg: dict[str, dict] = defaultdict(
            lambda: {"score": 0.0, "found_by": []}
        )

        for name, score in kw.items():
            if name not in {t.table_name for t in tables}:
                continue

            agg[name]["score"] += score + WEIGHTS["keyword"]
            if score > 0:
                agg[name]["found_by"].append("keyword")

        for name, score in sem.items():
            agg[name]["score"] += score + WEIGHTS["semantic"]
            if score > 0:
                agg[name]["found_by"].append("semantic")
        
        for name, score in fk.items():
            agg[name]["score"] += score + WEIGHTS["fk_graph"]
            if score > 0:
                agg[name]["found_by"].append("fk_graph")

        table_map = {t.table_name for t in tables}
        
        ranked = sorted(
            [
                ScoredTable(
                    table=table_map[name],
                    score=round(data['score'], 4),
                    found_by=list(set(data['fuond_by'])),
                )
                for name, data in agg.items() if name in table_map
            ],
            key=lambda a: a.score,
            reverse=True
        )

        return ranked