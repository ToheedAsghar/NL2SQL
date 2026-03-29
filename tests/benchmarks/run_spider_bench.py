#!/usr/bin/env python3
"""
Standalone Spider Benchmark Runner
====================================

Run your NL2SQL pipeline against real Spider dataset queries and measure
Execution Accuracy (EX) and Exact Match (EM).

Usage:
    # Quick smoke test (5 dev queries)
    python tests/benchmarks/run_spider_bench.py --dataset dev --limit 5

    # 20-query sample from each dataset
    python tests/benchmarks/run_spider_bench.py --dataset dev --limit 20
    python tests/benchmarks/run_spider_bench.py --dataset train_spider --limit 20
    python tests/benchmarks/run_spider_bench.py --dataset train_others --limit 20

    # Full dev set evaluation (~1034 queries — takes time + API credits)
    python tests/benchmarks/run_spider_bench.py --dataset dev

    # Save detailed per-query results to JSON
    python tests/benchmarks/run_spider_bench.py --dataset dev --limit 50 --output logs/bench.json

    # Verbose mode (see generated SQL for each query)
    python tests/benchmarks/run_spider_bench.py --dataset dev --limit 10 --verbose

Requirements:
    - OPENAI_API_KEY must be set (or configured in .env)
    - Spider databases must be present at spider/database/<db_id>/<db_id>.sqlite
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NL2SQL Spider Benchmark Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --dataset dev --limit 5          Quick smoke test
  %(prog)s --dataset dev --limit 20         20-query accuracy sample
  %(prog)s --dataset train_spider --limit 20
  %(prog)s --dataset dev                    Full dev set (expensive!)
  %(prog)s --dataset dev --limit 50 -o logs/bench.json
        """,
    )
    parser.add_argument(
        "--dataset",
        choices=["dev", "train_spider", "train_others"],
        default="dev",
        help="Which Spider split to evaluate (default: dev)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max queries to evaluate (default: all)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Path to save detailed JSON results",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each query's predicted vs gold SQL",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh instead of resuming from previous results",
    )
    args = parser.parse_args()

    # Validate API key
    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not set. Export it or add to .env")
        sys.exit(1)

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Import the benchmark module (after path setup)
    from test_spider_accuracy import (
        run_benchmark,
        load_spider_dataset,
    )

    # Check dataset exists
    try:
        data = load_spider_dataset(args.dataset, limit=args.limit)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    n = len(data)
    print(f"\nSpider Benchmark: {args.dataset}")
    print(f"Queries to evaluate: {n}")
    if args.limit:
        print(f"(limited to first {args.limit})")
    print()

    # Run
    save_to = Path(args.output) if args.output else None
    result = asyncio.run(
        run_benchmark(args.dataset, limit=args.limit, save_to=save_to, resume=not args.no_resume)
    )

    # Print summary
    print(result.summary())

    # Verbose: show each query
    if args.verbose:
        print("\nDetailed Results:")
        print("-" * 64)
        for i, r in enumerate(result.results):
            status = "PASS" if r["exec_match"] else "FAIL"
            print(f"\n[{i+1}] {status} | db={r['db_id']}")
            print(f"  Q: {r['question']}")
            print(f"  Gold:      {r['gold_sql']}")
            print(f"  Predicted: {r['predicted_sql']}")
            if r["error"]:
                print(f"  Error:     {r['error']}")
        print()

    if save_to:
        print(f"Detailed results saved to: {save_to}")


if __name__ == "__main__":
    main()
