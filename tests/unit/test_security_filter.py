"""Tests for SecurityFilter (nl2sql_agents/filters/security_filter.py)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch

from nl2sql_agents.models.schemas import TableMetaData
from tests.conftest import make_table, make_column


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tables() -> list[TableMetaData]:
    return [
        make_table("singer", columns=[make_column("Singer_ID", pk=True)]),
        make_table("concert", columns=[make_column("concert_ID", pk=True)]),
    ]


# ---------------------------------------------------------------------------
# SQLite mode — pass-through
# ---------------------------------------------------------------------------

class TestSecurityFilterSQLite:
    """In SQLite mode every table should pass through unchanged."""

    @pytest.mark.asyncio
    async def test_sqlite_returns_all_tables(self, tables):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "sqlite"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)
            result = await sf.filter(tables)

        assert len(result) == len(tables)
        assert {t.table_name for t in result} == {"singer", "concert"}

    @pytest.mark.asyncio
    async def test_sqlite_case_insensitive(self, tables):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "SQLite"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)
            result = await sf.filter(tables)

        assert len(result) == len(tables)

    @pytest.mark.asyncio
    async def test_sqlite_empty_tables(self):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "sqlite"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)
            result = await sf.filter([])

        assert result == []


# ---------------------------------------------------------------------------
# Non-SQLite mode — privilege filtering
# ---------------------------------------------------------------------------

class TestSecurityFilterPrivileges:
    """Non-SQLite backends should call _fetch_privileged_tables."""

    @pytest.mark.asyncio
    async def test_non_sqlite_raises_not_implemented(self, tables):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "postgres"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)

            with pytest.raises(NotImplementedError, match="postgres"):
                await sf.filter(tables)

    @pytest.mark.asyncio
    async def test_non_sqlite_filters_by_privileges(self, tables):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "postgres"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)
            # Simulate only "singer" being privileged
            sf._fetch_privileged_tables = AsyncMock(return_value={"singer"})

            result = await sf.filter(tables)

        assert len(result) == 1
        assert result[0].table_name == "singer"

    @pytest.mark.asyncio
    async def test_non_sqlite_no_privileges(self, tables):
        with patch("nl2sql_agents.filters.security_filter.DB_TYPE", "postgres"):
            from nl2sql_agents.filters.security_filter import SecurityFilter

            sf = SecurityFilter(connector=None)
            sf._fetch_privileged_tables = AsyncMock(return_value=set())

            result = await sf.filter(tables)

        assert result == []
