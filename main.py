"""
CLI Entry Point

Usage:
    python main.py "Show me top 5 sales reps this quarter"
    python main.py (interactive mode)
"""

import warnings
# Suppress Pydantic V1 compatibility warning
warnings.filterwarnings("ignore", message="Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.")

import asyncio
import sys
import os
import logging
from datetime import datetime
from orchestrator.pipeline import graph

# Logging setup 

# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(logs_dir, exist_ok=True)

# Generate log filename with timestamp
log_filename = os.path.join(logs_dir, f"nl2sql_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configure logging to write to file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-30s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename)
    ]
)

logger = logging.getLogger("main")
logger.info(f"Logging to: {log_filename}")

def print_output(result) -> None:
    """Pretty-print the final output to the terminal."""
    print("\n" + " " * 60)
    print(" SQL QUERY")
    print(" " * 60)
    print(result.sql)
    print("\n" + " " * 60)
    print(" EXPLANATION")
    print(" " * 60)
    print(result.explanation)
    print("\n" + " " * 60)
    print(" SAFETY REPORT")
    print(" " * 60)
    print(result.safety_report)
    print("\n" + " " * 60)
    print(" OPTIMIZATION HINTS")
    print(" " * 60)
    print(result.optimization_hints)
    print(" " * 60 + "\n")

async def run_query(query: str) -> None:
    """Invoke the LangGraph pipeline and print results."""
    final_state = await graph.ainvoke({"user_query": query})
    print_output(final_state["output"])

async def main() -> None:
    # CLI argument mode
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await run_query(query)
        return
    
    # Interactive mode
    print("SQL Generator — Multi-Agent System (LangGraph)")
    print("Type your question (or 'quit' to exit)\n")

    while True:
        try:
            query = input(" ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break
        if not query or query.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break
        try:
            await run_query(query)
        except RuntimeError as e:
            print(f"\n Error: {e}\n")
        except Exception as e:
            logger.exception("Unexpected error")
            print(f"\n Unexpected error: {e}\n")

if __name__ == "__main__":
    asyncio.run(main())