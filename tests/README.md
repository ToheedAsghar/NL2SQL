# Tests — NL2SQL Agents

Comprehensive test suite covering unit tests, integration tests, and Spider dataset benchmarks for the NL2SQL multi-agent pipeline.

---

## Directory Structure

```
tests/
├── README.md                  ← You are here
├── conftest.py                ← Shared fixtures & helpers
├── unit/                      ← Fast, isolated unit tests (122 tests)
│   ├── test_cli.py            ← CLI argument parsing & output formatting (13 tests)
│   ├── test_connector.py      ← DatabaseConnector: read, introspect, errors (11 tests)
│   ├── test_explainer_agents.py ← Explainer sub-agents: explanation, optimization, safety (12 tests)
│   ├── test_fk_graph_agent.py ← Foreign-key graph building & BFS traversal (18 tests)
│   ├── test_gate.py           ← GateLayer: scoring, filtering, security blocking (8 tests)
│   ├── test_keyword_agent.py  ← Keyword extraction & table matching (15 tests)
│   ├── test_nodes_pipeline.py ← Orchestrator nodes & pipeline wiring (9 tests)
│   ├── test_schema_cache.py   ← SchemaCache: get/set, TTL, invalidation (12 tests)
│   ├── test_security_filter.py ← SecurityFilter: SQLite pass-through, permission checks (6 tests)
│   └── test_security_validator.py ← SQL injection detection (regex-based, no LLM) (18 tests)
├── integration/               ← Cross-component integration tests (16 tests)
│   ├── test_discovery_pipeline.py ← Discovery pipeline: keyword + semantic + FK agents (4 tests)
│   ├── test_full_pipeline.py      ← Full pipeline end-to-end with mocked LLM (5 tests)
│   └── test_validation_pipeline.py ← Validation pipeline: 4-stage validator (7 tests)
└── benchmarks/                ← Spider dataset accuracy benchmarks (7 tests)
    ├── test_spider_accuracy.py    ← Pytest benchmark tests & core benchmark engine
    └── run_spider_bench.py        ← Standalone CLI benchmark runner
```

**Total: 145 tests** (122 unit + 16 integration + 7 benchmark)

---

## Quick Start

### Prerequisites

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Ensure .env has a valid OPENAI_API_KEY (required for benchmark tests only)
```

### Run All Unit & Integration Tests

```bash
# From project root
pytest tests/unit/ tests/integration/ -v

# With coverage report
pytest tests/unit/ tests/integration/ --cov=nl2sql_agents --cov-report=term-missing
```

### Run Only Unit Tests

```bash
pytest tests/unit/ -v
```

### Run Only Integration Tests

```bash
pytest tests/integration/ -v
```

---

## Test Categories

### Unit Tests (`tests/unit/`)

Fast, isolated tests that mock all external dependencies (LLM calls, database I/O). Each file targets a single module:

| File | Module Under Test | What It Covers |
|------|------------------|----------------|
| `test_cli.py` | `nl2sql_agents/cli.py` | Argument parser, output formatting, `--version` flag, main entry point (without launching the pipeline) |
| `test_connector.py` | `nl2sql_agents/db/connector.py` | `fetch_all()` with WHERE/empty results, `introspect()` schema extraction, foreign key detection, error handling for missing DBs |
| `test_explainer_agents.py` | `nl2sql_agents/agents/explainer/` | `ExplanationAgent`, `OptimizationAgent`, `SafetyReportAgent` — each tested with mocked LLM responses |
| `test_fk_graph_agent.py` | `nl2sql_agents/agents/discovery/fk_graph_agent.py` | FK graph construction, bidirectional edge detection, BFS neighbor discovery, isolated tables, self-referencing FKs |
| `test_gate.py` | `nl2sql_agents/filters/gate.py` | Top-K selection, security error blocking, empty table handling, score-based ordering |
| `test_keyword_agent.py` | `nl2sql_agents/agents/discovery/keyword_agent.py` | Stop-word removal, keyword extraction, table/column name matching, scoring logic |
| `test_nodes_pipeline.py` | `nl2sql_agents/orchestrator/` | Individual pipeline node functions (`load_schema`, `discover`, `gate`, `format`, `generate`, `validate`, `explain`) |
| `test_schema_cache.py` | `nl2sql_agents/cache/schema_cache.py` | Cache miss/hit, TTL expiration, invalidation, JSON serialization round-trip |
| `test_security_filter.py` | `nl2sql_agents/filters/security_filter.py` | SQLite pass-through mode, permission-based table filtering |
| `test_security_validator.py` | `nl2sql_agents/agents/validator/security_validator.py` | Regex-based SQL injection detection — tests safe SELECTs, JOINs, CTEs, subqueries, and blocks DROP/DELETE/INSERT/UPDATE/UNION injection, tautologies, stacked queries |

### Integration Tests (`tests/integration/`)

Test multiple components working together. LLM calls are mocked, but real data flows through the full pipeline path:

| File | What It Tests |
|------|---------------|
| `test_discovery_pipeline.py` | `DiscoveryAgent` orchestrating keyword, semantic (mocked embeddings), and FK graph agents together |
| `test_full_pipeline.py` | Node-level integration: `load_schema` with a real SQLite DB, schema formatting with real table metadata, full pipeline graph compilation |
| `test_validation_pipeline.py` | `ValidatorAgent` running all 4 validation stages (security, syntax, logic, performance) on generated candidates; tests passing, failing, and disqualified scenarios |

### Benchmark Tests (`tests/benchmarks/`)

Real-world accuracy evaluation against the [Spider](https://yale-lily.github.io/spider) text-to-SQL benchmark. These make **live LLM API calls** and require an `OPENAI_API_KEY`.

**Metrics:**
- **Execution Accuracy (EX):** Generated SQL produces the same result set as the gold SQL when executed on the target database
- **Exact Match (EM):** Normalised SQL strings are identical (stricter)

**Datasets:**

| Dataset | File | Queries | Description |
|---------|------|---------|-------------|
| `dev` | `spider/dev.json` | ~1,034 | Standard evaluation split |
| `train_spider` | `spider/train_spider.json` | ~7,000 | Spider training set |
| `train_others` | `spider/train_others.json` | ~1,659 | Additional training domains |

**7 Benchmark Tests:**

| Test | Description |
|------|-------------|
| `test_smoke_single_query` | Run 1 dev query end-to-end (verify pipeline doesn't crash) |
| `test_dev_sample_20` | 20 dev queries — quick accuracy sanity check |
| `test_dev_full` | Full dev set (~1,034 queries). Marked `@slow` |
| `test_train_spider_sample_20` | 20 train_spider queries |
| `test_train_others_sample_20` | 20 train_others queries |
| `test_train_spider_full` | Full train_spider set. Marked `@slow` |
| `test_train_others_full` | Full train_others set. Marked `@slow` |

---

## Running Benchmarks

### Via Pytest

```bash
# Smoke test (1 query)
pytest tests/benchmarks/ -k "smoke" -s

# 20-query samples
pytest tests/benchmarks/ -k "sample_20" -s

# Full dev set (expensive — ~1,034 API calls)
pytest tests/benchmarks/ -k "dev_full" -s --timeout 7200
```

### Via Standalone CLI Runner

The CLI runner (`run_spider_bench.py`) provides more control and is recommended for full evaluations:

```bash
# Quick smoke test
python tests/benchmarks/run_spider_bench.py --dataset dev --limit 5

# 20-query sample
python tests/benchmarks/run_spider_bench.py --dataset dev --limit 20

# Full dev set with JSON output
python tests/benchmarks/run_spider_bench.py --dataset dev -o logs/bench_dev_full.json

# Verbose mode (shows generated vs gold SQL per query)
python tests/benchmarks/run_spider_bench.py --dataset dev --limit 10 --verbose

# Force fresh start (ignore previous results)
python tests/benchmarks/run_spider_bench.py --dataset dev -o logs/bench_dev_full.json --no-resume
```

### Resume Support

Benchmarks save results incrementally after every query. If a run is interrupted (Ctrl+C, crash, API errors), simply re-run the same command — it will automatically skip already-evaluated queries and continue from where it left off.

```bash
# First run — gets through 200 queries before interruption
python tests/benchmarks/run_spider_bench.py --dataset dev -o logs/bench_dev_full.json
# ^C

# Resume — skips the 200 completed queries, continues from #201
python tests/benchmarks/run_spider_bench.py --dataset dev -o logs/bench_dev_full.json
```

### Benchmark Output

Live progress is printed per query:

```
  Resuming benchmark: dev — skipping 200 already-completed queries
  Current EX: 72.5%  (145/200)
  ────────────────────────────────────────────────────────────
  [201/1034] ✓ EX=72.6%  db=concert_singer            ETA=1h23m  Q=How many singers are there?
  [202/1034] ✗ EX=72.3%  db=world_1                   ETA=1h22m  Q=Which countries have population ove
```

JSON results are saved to the output path with this structure:

```json
{
  "dataset": "dev",
  "total": 1034,
  "exec_accuracy": 0.725,
  "exact_accuracy": 0.31,
  "errors": 12,
  "results": [
    {
      "db_id": "concert_singer",
      "question": "How many singers do we have?",
      "gold_sql": "SELECT count(*) FROM singer",
      "predicted_sql": "SELECT COUNT(*) FROM singer",
      "exec_match": true,
      "exact_match": true,
      "error": null
    }
  ]
}
```

---

## Shared Fixtures (`conftest.py`)

The `conftest.py` file provides reusable fixtures available to all test files:

| Fixture | Description |
|---------|-------------|
| `make_column()` | Builder function for `ColumnMetaData` objects |
| `make_table()` | Builder function for `TableMetaData` objects |
| `singer_columns` | 7-column singer table definition |
| `concert_columns` | 5-column concert table definition |
| `stadium_columns` | 7-column stadium table definition |
| `singer_in_concert_columns` | 2-column junction table definition |
| `sample_tables` | Full concert-singer schema (4 tables) |
| `sample_scored_tables` | Scored table list with decreasing relevance scores |
| `tmp_sqlite_db` | Creates a temporary SQLite database with singer/concert/stadium data |
| `mock_llm_response` | Factory fixture that patches `ChatOpenAI.ainvoke` to return canned responses |
| `tmp_cache_dir` | Redirects `SchemaCache` to a temp directory to avoid polluting real cache |

---

## Pytest Configuration

Defined in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
    "benchmark: Spider dataset accuracy benchmarks (require API key)",
    "slow: long-running tests (full dataset evaluations)",
]
```

### Custom Markers

- **`@pytest.mark.benchmark`** — All Spider benchmark tests. Skipped when `OPENAI_API_KEY` is not set or Spider data is missing.
- **`@pytest.mark.slow`** — Full-dataset evaluations (1,000+ queries). Use `-m "not slow"` to exclude.

### Useful Commands

```bash
# Run everything except benchmarks
pytest tests/ -m "not benchmark"

# Run everything except slow tests
pytest tests/ -m "not slow"

# Run with coverage and HTML report
pytest tests/unit/ tests/integration/ --cov=nl2sql_agents --cov-report=html

# Run a specific test file
pytest tests/unit/test_security_validator.py -v

# Run a specific test by name
pytest tests/ -k "test_simple_select" -v
```

---

## Writing New Tests

1. **Unit tests** go in `tests/unit/`. Mock all LLM calls using the `mock_llm_response` fixture or the `_fake_llm()` pattern.
2. **Integration tests** go in `tests/integration/`. Mock only external services (LLM API), let internal components interact.
3. **Benchmark tests** go in `tests/benchmarks/`. These use real LLM calls and should be marked with `@pytest.mark.benchmark`.
4. Use `make_table()` and `make_column()` from `conftest.py` to build test schemas consistently.
5. Use `tmp_sqlite_db` for any test that needs a real database.
6. All async tests use `@pytest.mark.asyncio` (auto-mode is enabled, so this is optional but explicit is better).
