"""Tests for SchemaCache (nl2sql_agents/cache/schema_cache.py)."""

from __future__ import annotations

import json
import os
import time

import pytest

from nl2sql_agents.cache.schema_cache import SchemaCache
from nl2sql_agents.models.schemas import TableMetaData
from tests.conftest import make_table, make_column


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchemaCache:
    """All tests use the tmp_cache_dir fixture to avoid polluting real cache."""

    def _make_cache(self) -> SchemaCache:
        return SchemaCache()

    # ── Basic get/set ─────────────────────────────────────────────────────

    def test_miss_on_empty_cache(self, tmp_cache_dir):
        cache = self._make_cache()
        assert cache.get("/fake/db.sqlite") is None

    def test_set_then_get(self, tmp_cache_dir):
        cache = self._make_cache()
        tables = [
            make_table("singer", columns=[make_column("Singer_ID", "INTEGER", pk=True)]),
            make_table("concert", columns=[make_column("concert_ID", "INTEGER", pk=True)]),
        ]
        cache.set("/my/db.sqlite", tables)
        result = cache.get("/my/db.sqlite")

        assert result is not None
        assert len(result) == 2
        assert result[0].table_name == "singer"
        assert result[1].table_name == "concert"

    def test_get_returns_table_metadata_instances(self, tmp_cache_dir):
        cache = self._make_cache()
        tables = [make_table("t1", columns=[make_column("id", pk=True)])]
        cache.set("/db.sqlite", tables)
        result = cache.get("/db.sqlite")

        assert all(isinstance(t, TableMetaData) for t in result)

    # ── Cache key determinism ─────────────────────────────────────────────

    def test_same_path_same_key(self, tmp_cache_dir):
        cache = self._make_cache()
        assert cache._cache_key("/a/b.db") == cache._cache_key("/a/b.db")

    def test_different_path_different_key(self, tmp_cache_dir):
        cache = self._make_cache()
        assert cache._cache_key("/a/b.db") != cache._cache_key("/c/d.db")

    # ── Expiration ────────────────────────────────────────────────────────

    def test_expired_entry_returns_none(self, tmp_cache_dir, monkeypatch):
        cache = self._make_cache()
        tables = [make_table("t")]
        cache.set("/db.sqlite", tables)

        # Simulate TTL expiration by patching CACHE_TTL_HOURS to 0
        monkeypatch.setattr("nl2sql_agents.cache.schema_cache.CACHE_TTL_HOURS", 0)
        # Also backdate the timestamp
        _, cache_file = tmp_cache_dir
        with open(cache_file) as f:
            data = json.load(f)
        for k in data:
            data[k]["timestamp"] = time.time() - 3600 * 25  # 25h ago
        with open(cache_file, "w") as f:
            json.dump(data, f)

        result = cache.get("/db.sqlite")
        assert result is None

    def test_fresh_entry_within_ttl(self, tmp_cache_dir, monkeypatch):
        monkeypatch.setattr("nl2sql_agents.cache.schema_cache.CACHE_TTL_HOURS", 48)
        cache = self._make_cache()
        tables = [make_table("t")]
        cache.set("/db.sqlite", tables)
        assert cache.get("/db.sqlite") is not None

    # ── Invalidation ──────────────────────────────────────────────────────

    def test_invalidate_removes_entry(self, tmp_cache_dir):
        cache = self._make_cache()
        tables = [make_table("t")]
        cache.set("/db.sqlite", tables)
        assert cache.get("/db.sqlite") is not None

        cache.invalidate("/db.sqlite")
        assert cache.get("/db.sqlite") is None

    def test_invalidate_nonexistent_is_noop(self, tmp_cache_dir):
        cache = self._make_cache()
        cache.invalidate("/nonexistent.db")  # should not raise

    # ── Multiple databases ────────────────────────────────────────────────

    def test_multiple_dbs_independent(self, tmp_cache_dir):
        cache = self._make_cache()
        t1 = [make_table("a")]
        t2 = [make_table("b"), make_table("c")]

        cache.set("/db1.sqlite", t1)
        cache.set("/db2.sqlite", t2)

        r1 = cache.get("/db1.sqlite")
        r2 = cache.get("/db2.sqlite")

        assert len(r1) == 1
        assert len(r2) == 2

    # ── Persistence ───────────────────────────────────────────────────────

    def test_cache_persists_to_disk(self, tmp_cache_dir):
        cache1 = self._make_cache()
        cache1.set("/x.db", [make_table("x")])

        # New instance reads from disk
        cache2 = self._make_cache()
        assert cache2.get("/x.db") is not None

    # ── Creates directory ─────────────────────────────────────────────────

    def test_creates_cache_dir(self, tmp_path, monkeypatch):
        new_dir = str(tmp_path / "brand_new")
        monkeypatch.setattr("nl2sql_agents.cache.schema_cache.CACHE_DIR", new_dir)
        monkeypatch.setattr(
            "nl2sql_agents.cache.schema_cache.CACHE_FILE",
            os.path.join(new_dir, "schema_cache.json"),
        )
        cache = self._make_cache()
        assert os.path.isdir(new_dir)
