"""Tests for DatabaseConnector (nl2sql_agents/db/connector.py)."""

from __future__ import annotations

import pytest

from nl2sql_agents.db.connector import DatabaseConnector


class TestDatabaseConnector:
    """Integration-style unit tests using a real temp SQLite file."""

    # ── Construction ──────────────────────────────────────────────────────

    def test_raises_on_missing_db(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Database not found"):
            DatabaseConnector(db_path=str(tmp_path / "nope.sqlite"))

    def test_accepts_valid_path(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        assert conn.db_path == tmp_sqlite_db

    # ── fetch_all ─────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_fetch_all_returns_dicts(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        rows = await conn.fetch_all("SELECT * FROM singer ORDER BY Singer_ID")

        assert len(rows) == 2
        assert isinstance(rows[0], dict)
        assert rows[0]["Name"] == "Joe"
        assert rows[1]["Name"] == "Jane"

    @pytest.mark.asyncio
    async def test_fetch_all_with_where(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        rows = await conn.fetch_all("SELECT Name FROM singer WHERE Country = 'France'")

        assert len(rows) == 1
        assert rows[0]["Name"] == "Joe"

    @pytest.mark.asyncio
    async def test_fetch_all_empty_result(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        rows = await conn.fetch_all("SELECT * FROM singer WHERE Country = 'Mars'")
        assert rows == []

    # ── introspect ────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_introspect_returns_tables(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        table_names = {t.table_name for t in tables}
        assert "singer" in table_names
        assert "concert" in table_names
        assert "stadium" in table_names

    @pytest.mark.asyncio
    async def test_introspect_columns(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        singer = next(t for t in tables if t.table_name == "singer")
        col_names = {c.column_name for c in singer.columns}
        assert "Singer_ID" in col_names
        assert "Name" in col_names
        assert "Country" in col_names

    @pytest.mark.asyncio
    async def test_introspect_primary_key(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        singer = next(t for t in tables if t.table_name == "singer")
        pk_cols = [c for c in singer.columns if c.is_primary_key]
        assert len(pk_cols) == 1
        assert pk_cols[0].column_name == "Singer_ID"

    @pytest.mark.asyncio
    async def test_introspect_foreign_key(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        concert = next(t for t in tables if t.table_name == "concert")
        fk_cols = [c for c in concert.columns if c.is_foreign_key]
        assert len(fk_cols) == 1
        assert fk_cols[0].column_name == "Stadium_ID"
        assert fk_cols[0].reference_table == "stadium"
        assert fk_cols[0].reference_column == "Stadium_ID"

    @pytest.mark.asyncio
    async def test_introspect_schema_name_is_db_name(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        # The schema_name should be the filename without extension
        assert all(t.schema_name == "test" for t in tables)

    @pytest.mark.asyncio
    async def test_introspect_excludes_sqlite_internal_tables(self, tmp_sqlite_db):
        conn = DatabaseConnector(db_path=tmp_sqlite_db)
        tables = await conn.introspect()

        for t in tables:
            assert not t.table_name.startswith("sqlite_")
