"""
Sub-Agent 1b - Semantic Agent

Embeds the user query and table descriptions using OpenAIEmbeddings.
Return Cosine similarity scores: {table_name: similarity}
"""

import logging
import numpy as np
from models.schemas import TableMetaData
from config.settings import EMBEDDING_PROVIDER

logger = logging.getLogger(__name__)

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    va, vb = np.array(a), np.array(b)
    # Calculate product of magnitudes (L2 norms)
    # ||a|| * ||b||
    norm = np.linalg.norm(va) * np.linalg.norm(vb)

    """
    # Two similar vectors (both about "sales")
    a = [0.5, 0.8, 0.2, 0.1]   # embedding for "revenue"
    b = [0.6, 0.7, 0.3, 0.2]   # embedding for "sales"

    # Dot product: 0.5*0.6 + 0.8*0.7 + 0.2*0.3 + 0.1*0.2 = 0.88
    # ||a|| = sqrt(0.25 + 0.64 + 0.04 + 0.01) = 0.949
    # ||b|| = sqrt(0.36 + 0.49 + 0.09 + 0.04) = 0.990
    # Result: 0.88 / (0.949 * 0.990) ≈ 0.94  ← very similar!

    # Two unrelated vectors
    c = [0.9, 0.1, 0.0, 0.0]   # embedding for "apple" (fruit)
    d = [0.1, 0.9, 0.8, 0.5]   # embedding for "car" (vehicle)
    # Result: ≈ 0.15  ← not similar
    """

    return float(np.dot(va, vb)/norm) if norm > 0 else 0.0

class SemanticAgent:
    def __init__(self) -> None:
        self.embeddings = EMBEDDING_PROVIDER.embeddings_model()

    async def score(
            self, tables: list[TableMetaData], user_query: str
    ) -> dict[str, float]:
        logger.debug("SemanticAgent: embedding query + %d tables", len(tables))

        texts = [user_query] + [self._table_to_text(t) for t in tables]

        all_embeddings = self.embeddings.aembed_documents(texts)

        query_emb = all_embeddings[0]
        table_embs = all_embeddings[1:]

        return {
            t.table_name: round(_cosine_similarity(query_emb, emb), 4) for t, emb in zip(tables, table_embs)
        }
    
    def _table_to_text(self, table: TableMetaData) -> str:
        col_names = ','.join(c.column_name for c in table.columns[:20])
        return f"Table {table.table_name}: columns {col_names}"
