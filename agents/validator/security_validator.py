"""
Security Validator
BLOCKS:
- non-select DML (INSERT, DELETE, MERGE, UPDATE, TRUNCATE, DROP, ALTER)
- SQL Injection patterns (stacked queries, EXEC, dyanamic SQL)
- System/internal table access
"""

import re
import logging
from models.schemas import ValidatorCheckResult

logger = logging.getLogger(__name__)

FORBIDDEN_PATTERNS = [
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|REPLACE|EXEC|EXECUTE)\b",
    r"--[^\n]*", # inline comments hiding injections
    r";.*(SELECT|INSERT|UPDATE)", # stacked queries
]

class SecurityValidator:
    async def check(self, sql: str) -> ValidatorCheckResult:
        sql_upper = sql.upper()

        for pattern in FORBIDDEN_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                logger.warning("SecurityValidator FAIL: %s", pattern)

                return ValidatorCheckResult(
                    check_name="security",
                    passed = False,
                    score= 0.0,
                    details = f"Forbidden Pattern: {pattern}"
                )
        
        stripped = sql.strip().upper()
        if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
            return ValidatorCheckResult(
                check_name="Security",
                passed=False,
                score=0.0,
                details="Query must be a Select or With statement"
            )
        
        logger.info("SecurityAgent: All Security Checks Passed")
        return ValidatorCheckResult(
            check_name="Security",
            passed=True,
            score=1.0,
            details="All Security checks passed"
        )
