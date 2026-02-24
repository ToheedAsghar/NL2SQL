"""
GATE LAYER
- Sits after PARALLEL (Security Filter + Discovery Agent)
1. Verify security filter completed without errors
2. INTERSECT discovery's ranked list with security-approved table names
3. Take top-K accessible tables from the ranked list.
4. If top-K is too small, backfill from the ranked list.
"""

import logging
from models.schemas import ScoredTable, GateResult
from config.settings import DISCOVERY_TOP_K

logger = logging.getLogger(__name__)

class GateLayer:
    def evaluate(
            self,
            scored_tables: list[ScoredTable],
            security_approved: set[str],
            security_error: Exception | None = None,
            top_k: int = DISCOVERY_TOP_K
    ) -> GateResult:
        
        # 1. Security Check
        if security_error is not None:
            logger.error("Gate BLOCKED: %s", security_error)
            return GateResult(
                passed=False,
                tables=[],
                reason=f"Security Filter Error: {security_error}"
            )

        # 2. Filter ranked list: keep only the security approved results
        accessible = [
            st for st in scored_tables
            if st.table.table_name in security_approved
        ]

        logger.info("Gate: Discovery_ranked=%d, security_approved=%d, accessible=%d",len(scored_tables), len(security_approved), len(accessible))

        # 3. pick up top-K
        top = accessible[:top_k]

        if not top:
            return GateResult(
                passed=False,
                tables=[],
                reason="No Accessible tables matched your query, check permissions or try again"
            )
        
        logger.info(
            "Gate: top-%d = %s",
            top_k, [st.table.table_name for st in top],
        )

        return GateResult(
            passed=True,
            tables=[st.table for st in top],
            reason=None
        )

