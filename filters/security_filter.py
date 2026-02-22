"""
SECURITY FILTER
- selects which tables the current user/connection has SELECt access to.
- SQLite: pass-through (all tables accessible, no privilege system)
- Extensible: override _fetch_privileged_tables() for other DB backends
Runs in PARALLEL alongside Discovery Agent.
"""

import logging
from config.settings import DB_TYPE
from models.schemas import TableMetaData


logger = logging.getLogger(__name__)

class SecurityFilter:
    def __init__(self, connector) -> None:
        self.connector = connector

    async def filter(self, tables: list[TableMetaData]) -> list[TableMetaData]:
        logger.info("Security Filter: checking privileges for %d tables", len(tables))

        if DB_TYPE == 'SQLite':
            logger.info('SecurityFilter: SQLite mode - all %d tables pass', len(tables))
            return tables
    
        # for other DB backends, query privileges
        privileged = await self._fetch_privileged_tables()
        filtered = [t for t in tables if t.table_name in privileged]
        logger.info('SecurityFilter: %d -> %d tables (removed %d)', len(tables), len(filtered), len(tables) - len(filtered))

        return filtered
    
    async def _fetch_privileged_tables(self) -> set[str]:
        raise NotImplementedError(f"Privilege check not implemented for DB_TYPE={DB_TYPE}")
    