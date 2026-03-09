"""
Shared fixtures for nl2sql-agents test suite.

Provides:
- Sample TableMetaData / ColumnMetaData / ScoredTable objects
- Mock LLM that returns canned responses
- Temporary SQLite database for connector tests
- Helpers for building test schemas
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from nl2sql_agents.models.schemas import (
    CandidateValidationResult,
    ColumnMetaData,
    DiscoveryResult,
    FormattedSchema,
    GateResult,
    GenerationResult,
    ScoredTable,
    SQLCandidate,
    TableMetaData,
    ValidationResult,
    ValidatorCheckResult,
)


# ---------------------------------------------------------------------------
# Column / Table builders
# ---------------------------------------------------------------------------

def make_column(
    name: str,
    dtype: str = "TEXT",
    pk: bool = False,
    fk: bool = False,
    ref_table: str | None = None,
    ref_col: str | None = None,
    nullable: bool = True,
) -> ColumnMetaData:
    return ColumnMetaData(
        column_name=name,
        data_type=dtype,
        nullable=nullable,
        is_primary_key=pk,
        is_foreign_key=fk,
        reference_table=ref_table,
        reference_column=ref_col,
    )


def make_table(name: str, schema: str = "test_db", columns: list[ColumnMetaData] | None = None) -> TableMetaData:
    return TableMetaData(
        table_name=name,
        schema_name=schema,
        columns=columns or [],
    )


# ---------------------------------------------------------------------------
# Reusable sample data
# ---------------------------------------------------------------------------

@pytest.fixture
def singer_columns() -> list[ColumnMetaData]:
    return [
        make_column("Singer_ID", "INTEGER", pk=True, nullable=False),
        make_column("Name", "TEXT"),
        make_column("Country", "TEXT"),
        make_column("Song_Name", "TEXT"),
        make_column("Song_release_year", "TEXT"),
        make_column("Age", "INTEGER"),
        make_column("Is_male", "TEXT"),
    ]


@pytest.fixture
def concert_columns() -> list[ColumnMetaData]:
    return [
        make_column("concert_ID", "INTEGER", pk=True, nullable=False),
        make_column("concert_Name", "TEXT"),
        make_column("Theme", "TEXT"),
        make_column("Stadium_ID", "INTEGER", fk=True, ref_table="stadium", ref_col="Stadium_ID"),
        make_column("Year", "TEXT"),
    ]


@pytest.fixture
def stadium_columns() -> list[ColumnMetaData]:
    return [
        make_column("Stadium_ID", "INTEGER", pk=True, nullable=False),
        make_column("Location", "TEXT"),
        make_column("Name", "TEXT"),
        make_column("Capacity", "INTEGER"),
        make_column("Highest", "INTEGER"),
        make_column("Lowest", "INTEGER"),
        make_column("Average", "INTEGER"),
    ]


@pytest.fixture
def singer_in_concert_columns() -> list[ColumnMetaData]:
    return [
        make_column("concert_ID", "INTEGER", pk=True, fk=True, ref_table="concert", ref_col="concert_ID"),
        make_column("Singer_ID", "INTEGER", pk=True, fk=True, ref_table="singer", ref_col="Singer_ID"),
    ]


@pytest.fixture
def sample_tables(
    singer_columns, concert_columns, stadium_columns, singer_in_concert_columns
) -> list[TableMetaData]:
    """Concert-singer dataset: singer, concert, stadium, singer_in_concert."""
    return [
        make_table("singer", columns=singer_columns),
        make_table("concert", columns=concert_columns),
        make_table("stadium", columns=stadium_columns),
        make_table("singer_in_concert", columns=singer_in_concert_columns),
    ]


@pytest.fixture
def sample_scored_tables(sample_tables: list[TableMetaData]) -> list[ScoredTable]:
    scores = [0.9, 0.7, 0.5, 0.3]
    return [
        ScoredTable(table=t, score=s, found_by=["keyword"])
        for t, s in zip(sample_tables, scores)
    ]


# ---------------------------------------------------------------------------
# Temporary SQLite database (for connector / cache tests)
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_sqlite_db(tmp_path: Path) -> str:
    """Create a minimal SQLite database and return its path."""
    import sqlite3

    db_path = str(tmp_path / "test.sqlite")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE singer (
            Singer_ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL,
            Country TEXT,
            Age INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE concert (
            concert_ID INTEGER PRIMARY KEY,
            concert_Name TEXT,
            Theme TEXT,
            Stadium_ID INTEGER,
            Year TEXT,
            FOREIGN KEY (Stadium_ID) REFERENCES stadium(Stadium_ID)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE stadium (
            Stadium_ID INTEGER PRIMARY KEY,
            Location TEXT,
            Name TEXT,
            Capacity INTEGER
        )
        """
    )
    cur.execute("INSERT INTO singer VALUES (1, 'Joe', 'France', 30)")
    cur.execute("INSERT INTO singer VALUES (2, 'Jane', 'USA', 25)")
    cur.execute("INSERT INTO stadium VALUES (1, 'Paris', 'Stade A', 50000)")
    cur.execute("INSERT INTO concert VALUES (1, 'Live Aid', 'Charity', 1, '2024')")
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Mock LLM helper
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_llm_response():
    """
    Factory fixture: returns a context-manager that patches ChatOpenAI.ainvoke
    to return a canned string.

    Usage:
        with mock_llm_response("SELECT 1"):
            result = await some_agent.execute(...)
    """
    from contextlib import contextmanager

    @contextmanager
    def _mock(response_text: str):
        fake_response = MagicMock()
        fake_response.content = response_text
        fake_response.usage_metadata = {"input_tokens": 10, "output_tokens": 5}

        with patch("langchain_openai.ChatOpenAI.ainvoke", new_callable=AsyncMock, return_value=fake_response):
            yield

    return _mock


# ---------------------------------------------------------------------------
# Temporary cache directory (avoid polluting real cache)
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_cache_dir(tmp_path: Path, monkeypatch):
    """Redirect SchemaCache to a temp directory."""
    cache_dir = str(tmp_path / "cache")
    cache_file = str(tmp_path / "cache" / "schema_cache.json")
    monkeypatch.setattr("nl2sql_agents.cache.schema_cache.CACHE_DIR", cache_dir)
    monkeypatch.setattr("nl2sql_agents.cache.schema_cache.CACHE_FILE", cache_file)
    return cache_dir, cache_file
