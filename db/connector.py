"""
DATABASE CONNECTOR

- Generic asynd DB connector with pluggable backends.
- Ships with SQLite support (for Spider Dataset and local DB)
- provides:
    - fetch_all(query)  -> list of row dicts
    - introspect()      -> list[TableMetaData] (full schema)
"""

import os
import asyncio
import logging
import aiosqlite

from config.settings import DB_PATH, DB_TYPE
from models.schemas import TableMetaData, ColumnMetaData

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Generic Connector - Currently Supports SQLite"""

    def __init__(self, db_path: str = DB_TYPE) -> None:
        self.db_path = db_path

        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
    async def fetch_all(self, query: str) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    # schema introspection
    async def introspect(self) -> list[TableMetaData]:
        logger.info("Database Conenctor: Introspecting %s", self.db_path)

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            tables = await self._fetch_tables(db)
            result: list[TableMetaData] = []

            for table_name in tables:
                columns = await self._fetch_columns(db, table_name)
                fk_map = await self._fetch_foreign_keys(db, table_name)
                pk_cols = await self._fetch_primary_keys(db, table_name)

                build_cols = []
                for col in columns:
                    col_name = col['name']
                    fk_target = fk_map.get(col_name)
                    build_cols.append(
                        ColumnMetaData(
                            column_name=col_name,
                            data_type = col["type"] or "TEXT",
                            nullabe = col['notnull'],
                            is_primary_key=col_name in pk_cols,
                            is_foreign_key=fk_target is not None,
                            reference_table = fk_target[0] if fk_target else None
                            reference_column=fk_target[1] if fk_target else None
                        )
                    )
            
                db_name = os.path.splitext(os.path.basename(self.db_path))[0]
                result.append(
                    TableMetaData(
                        table_name=table_name,
                        schema_name=db_name,
                        columns=build_cols
                    )
                )

        logger.info("DatabaseConnector: introspected %d tables", len(result))
        return result
        
    async def _fetch_tables(self, db) -> list[str]:
        query = """
            SELECT name FROM sqlite_master
            WHERE type = 'table'
                AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """

        async with db.execute(query) as cursor:
            rows = await cursor.fetch_all()
            return [row["name"] for row in rows]
        
    async def _fetch_columns(self, db, table_name: str) -> list[dict]:
        async with db.execute(f"PRAGMA table_info('{table_name}')") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        
    async def _fetch_primary_keys(self, db, table_name: str) -> set[str]:
        async with db.execute(f"PRAGMA table_info('{table_name}')") as cursor:
            rows = await cursor.fetchall()
            return {row["name"] for row in rows if row["pk"]}
        
    async def _fetch_foreign_keys(self, db, table_name: str) -> dict[str, tuple[str, str]]:
        async with db.execute(f"PRAGMA foreign_key_list('{table_name})") as cursor:
            rows = cursor.fetchall()
            return {
                row["from"]: (row["table"], row["to"]) for row in rows
            }
