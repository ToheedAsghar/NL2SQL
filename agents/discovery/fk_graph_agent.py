"""
Sub-Agent 1c - Foreign Key Graph Agent

Builds Bi-directional foriegn key graph from table metadata.
BFS-walk from seed tables, and scores by graph distance.

| Distance from Seed         | Score | Interpretation              |
| -------------------------- | ----- | --------------------------- |
| 0 (seed itself)            | 1.0   | Directly mentioned in query |
| 1 (neighbors)              | 0.5   | Directly linked via FK      |
| 2 (neighbors of neighbors) | 0.25  | Two hops away               |

"""

import logging
from collections import deque
from models.schemas import TableMetaData

logger = logging.getLogger(__name__)

MAX_DEPTH = 2

class FKGraphAgent:
    async def score(self, tables: list[TableMetaData], user_query: str) -> dict[str, float]:
        graph = self._build_fk_graph(tables)
        seeds = self._find_seeds(tables, user_query)
        
        logger.debug('FKGraphAgent: seeds=%s', seeds)
        return self._bfs_score(seeds, graph, MAX_DEPTH)

    def _build_fk_graph(self, tables: list[TableMetaData])-> dict[str, set[str]]:

        graph: dict[str, set[str]] = {t.table_name: set() for t in tables}

        for table in tables:
            for col in table.columns:
                if col.is_foreign_key and col.reference_column:
                    graph[table.table_name].add(col.reference_table)
                    if col.reference_table in graph:
                        graph[col.reference_table].add(table.table_name)
        return graph
    
    def _find_seeds(self, tables: list[TableMetaData], user_query: str) -> list[str]:
        """seed = table whose name-parts appear in the user query"""

        query_lower = user_query.lower()
        seeds = []
        for table in tables:
            parts = table.table_name.lower().replace('_', ' ').split()
            if any(part in query_lower for part in parts if len(part) > 2):
                seeds.append(table.table_name)
        return seeds
    
    def _bfs_score(self, seeds: list[str], graph: dict[str, set[str]], max_depth: int) -> dict[str, float]:
        scores: dict[str, float] = {}
        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque()

        for seed in seeds:
            queue.append((seed, 0))
            visited.add(seed)

        while queue:
            node, depth = queue.popleft()
            if depth > max_depth:
                continue
            score = 1.0 / (2 ** depth)
            scores[node] = max(scores.get(node, 0.0), score)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth+1))
        
        return scores