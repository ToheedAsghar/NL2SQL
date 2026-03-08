"""
SCHEMA CACHE

JSON File cache at ~/.sql_generator/schema_cache.json
key: SHA-256 hash of absolute database path
TTL: configurable (defautl 24 hours)
"""

import os
import time
import json
import logging
import hashlib
from typing import Optional

from nl2sql_agents.models.schemas import TableMetaData
from nl2sql_agents.config.settings import CACHE_TTL_HOURS, CACHE_DIR, CACHE_FILE

logger = logging.getLogger(__name__)

class SchemaCache:
    def __init__(self) -> None:
        os.makedirs(CACHE_DIR, exist_ok=True)

    def _cache_key(self, db_path: str) -> str:
        raw = os.path.abspath(db_path)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    
    def _load_raw(self) -> dict:
        if not os.path.exists(CACHE_FILE):
            return {}
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    
    def _save_raw(self, data: dict) -> None:
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def get(self, db_path: str) -> Optional[list[TableMetaData]]:
        """Return cached tables if valid, else None(cache miss)"""
        key = self._cache_key(db_path=db_path)
        raw = self._load_raw()

        if key not in raw:
            logger.info("Cache MISS for key %s", key)
            return None
        
        entry = raw[key]
        age_hours = (time.time() - entry["timestamp"]) / 3600

        if age_hours > CACHE_TTL_HOURS:
            logger.info("Cache EXPIRED (age=%.1fh, tables=%d)", age_hours, len(entry['tables']))
            return None

        logger.info("Cache HIT for key %s", key)
        return [TableMetaData(**t) for t in entry["tables"]]

    def set(self, db_path: str, tables: list[TableMetaData]) -> None:
        """Persist introspection result into cache"""    
        key = self._cache_key(db_path)
        raw=self._load_raw()

        raw[key] = {
            "timestamp": time.time(),
            "tables": [t.model_dump() for t in tables]
        }

        self._save_raw(raw)
        logger.info("Cache SET: %d tables for key=%s", len(tables), key)

    def invalidate(self, db_path: str) -> None:
        """Force expire a cache entry"""
        key = self._cache_key(db_path)
        raw=self._load_raw()

        if key in raw:
            del raw[key]
            self._save_raw(raw)
            logger.info("Cache INVALIDATED for key=%s", key)