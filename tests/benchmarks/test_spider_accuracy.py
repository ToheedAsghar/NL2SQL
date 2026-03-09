"""
Spider Dataset Accuracy Benchmark
==================================

Evaluates the NL2SQL pipeline against real Spider dataset queries.

Supports three datasets:
  - dev.json            (1034 queries — standard evaluation split)
  - train_spider.json   (7000 queries — Spider training set)
  - train_others.json   (1659 queries — additional training domains)

Each JSON entry contains:
  { "db_id": "...", "question": "...", "query": "SELECT ..." }

Metrics:
  - Execution Accuracy (EX):  generated SQL produces the same result set as gold
  - Exact Match (EM):         normalised SQL strings are identical

Requirements:
  - OPENAI_API_KEY set (real LLM calls)
  - Spider databases under spider/database/<db_id>/<db_id>.sqlite

Run:
    pytest tests/benchmarks/ -m benchmark -s --timeout 600

Or use the standalone CLI runner:
    python tests/benchmarks/run_spider_bench.py --dataset dev --limit 20
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import pytest
from dotenv import load_dotenv

# Load .env early so OPENAI_API_KEY is available at collection time
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SPIDER_DIR = Path(__file__).resolve().parents[2] / "spider"
DB_DIR = SPIDER_DIR / "database"

DATASET_FILES: dict[str, Path] = {
    "dev": SPIDER_DIR / "dev.json",
    "train_spider": SPIDER_DIR / "train_spider.json",
    "train_others": SPIDER_DIR / "train_others.json",
}

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------
pytestmark = [
    pytest.mark.benchmark,
    pytest.mark.skipif(
        not DATASET_FILES["dev"].exists(),
        reason="Spider dev.json not found — download the dataset first",
    ),
    pytest.mark.skipif(
        not os.getenv("OPENAI_API_KEY"),
        reason="OPENAI_API_KEY not set — LLM benchmarks need API access",
    ),
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_spider_dataset(
    name: str = "dev",
    limit: int | None = None,
) -> list[dict]:
    """
    Load a Spider dataset by name.

    Returns list of dicts with at least: db_id, question, query (gold SQL).
    Skips entries whose database is not present locally.
    """
    path = DATASET_FILES.get(name)
    if path is None or not path.exists():
        raise FileNotFoundError(f"Dataset '{name}' not found at {path}")

    with open(path) as f:
        data = json.load(f)

    # Filter to entries that have all required fields and a local DB
    valid = []
    for entry in data:
        db_id = entry.get("db_id")
        question = entry.get("question")
        gold_sql = entry.get("query")
        if not (db_id and question and gold_sql):
            continue
        db_path = DB_DIR / db_id / f"{db_id}.sqlite"
        if not db_path.exists():
            continue
        entry["_db_path"] = str(db_path)
        valid.append(entry)

    if limit:
        valid = valid[:limit]
    return valid


# ---------------------------------------------------------------------------
# SQL Normalisation (for Exact Match)
# ---------------------------------------------------------------------------

def _normalise_sql(sql: str) -> str:
    """Lowercase, collapse whitespace, strip trailing semicolons."""
    sql = sql.lower().strip().rstrip(";").strip()
    sql = re.sub(r"\s+", " ", sql)
    return sql


# ---------------------------------------------------------------------------
# Execution Accuracy Helper
# ---------------------------------------------------------------------------

def execution_match(predicted_sql: str, gold_sql: str, db_path: str) -> bool:
    """
    Compare execution results of predicted vs gold SQL on a SQLite database.
    Returns True if the result *sets* are identical (order-independent).
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.text_factory = str
        cur = conn.cursor()
        pred_rows = set(map(tuple, cur.execute(predicted_sql).fetchall()))
        gold_rows = set(map(tuple, cur.execute(gold_sql).fetchall()))
        conn.close()
        return pred_rows == gold_rows
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Standalone Pipeline Runner (avoids module-level singletons)
# ---------------------------------------------------------------------------

async def run_single_query(db_path: str, question: str) -> str:
    """
    Execute the full NL2SQL pipeline for one query and return generated SQL.

    Creates *fresh* agent / connector instances so each call can target a
    different Spider database without monkey-patching module globals.
    """
    from nl2sql_agents.db.connector import DatabaseConnector
    from nl2sql_agents.cache.schema_cache import SchemaCache
    from nl2sql_agents.filters.security_filter import SecurityFilter
    from nl2sql_agents.filters.gate import GateLayer
    from nl2sql_agents.agents.discovery.discovery_agent import DiscoveryAgent
    from nl2sql_agents.agents.schema_formatter import SchemaFormatterAgent
    from nl2sql_agents.agents.query_generator import QueryGeneratorAgent
    from nl2sql_agents.agents.validator.validator_agent import ValidatorAgent

    connector = DatabaseConnector(db_path)
    cache = SchemaCache()
    sec_filter = SecurityFilter(connector)
    gate = GateLayer()
    discovery = DiscoveryAgent()
    formatter = SchemaFormatterAgent()
    generator = QueryGeneratorAgent()
    validator = ValidatorAgent()

    # 1. Load schema (with caching)
    tables = cache.get(db_path)
    if tables is None:
        tables = await connector.introspect()
        cache.set(db_path, tables)

    # 2. Security filter + Discovery (parallel)
    approved_task = sec_filter.filter(tables)
    discovery_task = discovery.run(tables, question)
    approved, disc_result = await asyncio.gather(approved_task, discovery_task)

    approved_names = {t.table_name for t in approved}

    # 3. Gate
    gate_result = gate.evaluate(disc_result.scored_tables, approved_names)
    if not gate_result.passed:
        return ""

    # 4. Format schema
    formatted = await formatter.format(gate_result.tables)

    # 5. Generate SQL candidates
    generation = await generator.generate(formatted, question)

    # 6. Validate & pick best
    validation = await validator.validate(generation, question)

    if validation.best_candidate:
        return validation.best_candidate.sql
    return ""


# ---------------------------------------------------------------------------
# Benchmark Runner
# ---------------------------------------------------------------------------

class BenchmarkResult:
    """Accumulates per-query results and prints a summary."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.total = 0
        self.exec_correct = 0
        self.exact_correct = 0
        self.errors = 0
        self.results: list[dict] = []
        self._start = time.time()

    def record(
        self,
        *,
        question: str,
        db_id: str,
        gold_sql: str,
        predicted_sql: str,
        exec_match: bool,
        exact_match: bool,
        error: str | None = None,
    ) -> None:
        self.total += 1
        self.exec_correct += int(exec_match)
        self.exact_correct += int(exact_match)
        if error:
            self.errors += 1
        self.results.append(
            {
                "db_id": db_id,
                "question": question,
                "gold_sql": gold_sql,
                "predicted_sql": predicted_sql,
                "exec_match": exec_match,
                "exact_match": exact_match,
                "error": error,
            }
        )

    @property
    def exec_accuracy(self) -> float:
        return self.exec_correct / self.total if self.total else 0.0

    @property
    def exact_accuracy(self) -> float:
        return self.exact_correct / self.total if self.total else 0.0

    def summary(self) -> str:
        elapsed = time.time() - self._start
        lines = [
            "",
            "=" * 64,
            f"  Spider Benchmark — {self.dataset_name}",
            "=" * 64,
            f"  Queries evaluated : {self.total}",
            f"  Errors / timeouts : {self.errors}",
            f"  Execution Accuracy: {self.exec_correct}/{self.total}"
            f"  = {self.exec_accuracy:.1%}",
            f"  Exact Match       : {self.exact_correct}/{self.total}"
            f"  = {self.exact_accuracy:.1%}",
            f"  Wall time         : {elapsed:.1f}s",
            "=" * 64,
            "",
        ]
        return "\n".join(lines)

    def save_json(self, path: Path) -> None:
        """Persist detailed per-query results."""
        payload = {
            "dataset": self.dataset_name,
            "total": self.total,
            "exec_accuracy": round(self.exec_accuracy, 4),
            "exact_accuracy": round(self.exact_accuracy, 4),
            "errors": self.errors,
            "results": self.results,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)


async def run_benchmark(
    dataset: str = "dev",
    limit: int | None = None,
    save_to: Path | None = None,
) -> BenchmarkResult:
    """
    Main benchmark driver.

    Loads the specified Spider dataset, runs each query through the pipeline,
    and compares against the gold SQL using both Execution Accuracy and Exact Match.
    """
    data = load_spider_dataset(dataset, limit=limit)
    bench = BenchmarkResult(dataset)

    total_queries = len(data)
    print(f"\n  Starting benchmark: {dataset} ({total_queries} queries)")
    print(f"  {'─' * 56}")
    sys.stdout.flush()

    for i, entry in enumerate(data):
        db_id = entry["db_id"]
        question = entry["question"]
        gold_sql = entry["query"]
        db_path = entry["_db_path"]

        predicted_sql = ""
        error: str | None = None

        try:
            predicted_sql = await run_single_query(db_path, question)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            logger.warning("Query %d failed: %s", i, error)

        ex = execution_match(predicted_sql, gold_sql, db_path)
        em = _normalise_sql(predicted_sql) == _normalise_sql(gold_sql)

        bench.record(
            question=question,
            db_id=db_id,
            gold_sql=gold_sql,
            predicted_sql=predicted_sql,
            exec_match=ex,
            exact_match=em,
            error=error,
        )

        # Live progress — prints every query so it's clear it's working
        elapsed = time.time() - bench._start
        status = "✓" if ex else "✗"
        eta = (elapsed / (i + 1)) * (total_queries - i - 1)
        eta_min, eta_sec = divmod(int(eta), 60)
        eta_hr, eta_min = divmod(eta_min, 60)
        eta_str = f"{eta_hr}h{eta_min:02d}m" if eta_hr else f"{eta_min}m{eta_sec:02d}s"

        print(
            f"  [{i+1:>{len(str(total_queries))}}/{total_queries}] "
            f"{status} EX={bench.exec_accuracy:.1%}  "
            f"db={db_id:<25s}  "
            f"ETA={eta_str}  "
            f"Q={question[:40]}",
            flush=True,
        )

    if save_to:
        bench.save_json(save_to)

    return bench


# ===================================================================
# Pytest benchmark tests
# ===================================================================

class TestSpiderAccuracy:
    """
    Run NL2SQL against Spider datasets and measure accuracy.

    These are meant to be run manually or in a dedicated CI job,
    NOT as part of the regular unit test suite.
    """

    @pytest.mark.asyncio
    async def test_smoke_single_query(self):
        """Smoke test: run one Spider dev query end-to-end."""
        data = load_spider_dataset("dev", limit=1)
        assert data, "No valid dev queries found"

        entry = data[0]
        predicted = await run_single_query(entry["_db_path"], entry["question"])

        assert predicted, "Pipeline returned empty SQL"
        # Verify it at least executes without error
        assert execution_match(predicted, predicted, entry["_db_path"]), \
            f"Generated SQL does not execute: {predicted}"

    @pytest.mark.asyncio
    async def test_dev_sample_20(self):
        """Run 20 dev queries — quick accuracy sanity check."""
        result = await run_benchmark("dev", limit=20)
        print(result.summary())
        assert result.total > 0, "No queries were evaluated"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_dev_full(self):
        """Full dev set evaluation (~1034 queries). Expensive."""
        out_path = Path("logs/bench_dev_full.json")
        result = await run_benchmark("dev", save_to=out_path)
        print(result.summary())
        assert result.total > 0

    @pytest.mark.asyncio
    async def test_train_spider_sample_20(self):
        """Run 20 train_spider queries."""
        result = await run_benchmark("train_spider", limit=20)
        print(result.summary())
        assert result.total > 0

    @pytest.mark.asyncio
    async def test_train_others_sample_20(self):
        """Run 20 train_others queries."""
        result = await run_benchmark("train_others", limit=20)
        print(result.summary())
        assert result.total > 0

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_train_spider_full(self):
        """Full train_spider evaluation. Very expensive."""
        out_path = Path("logs/bench_train_spider_full.json")
        result = await run_benchmark("train_spider", save_to=out_path)
        print(result.summary())
        assert result.total > 0

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_train_others_full(self):
        """Full train_others evaluation. Very expensive."""
        out_path = Path("logs/bench_train_others_full.json")
        result = await run_benchmark("train_others", save_to=out_path)
        print(result.summary())
        assert result.total > 0
