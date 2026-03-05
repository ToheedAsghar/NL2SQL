"""
CLI entry point for the nl2sql-agents package.

Usage:
    nl2sql "Show me top 5 sales reps this quarter"
    nl2sql                              # interactive REPL
    nl2sql --db path/to/db.sqlite       # override database
    python main.py                      # legacy entry point
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import textwrap
import warnings
from datetime import datetime
from pathlib import Path

# Suppress Pydantic V1 compatibility warning before any imports touch it
warnings.filterwarnings(
    "ignore",
    message="Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.",
)

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.theme import Theme
from rich import box

# ── Theme ────────────────────────────────────────────────────────────────────
_THEME = Theme(
    {
        "info": "dim cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "heading": "bold bright_white",
        "prompt": "bold cyan",
        "muted": "dim white",
    }
)

console = Console(theme=_THEME)

# ── Branding ─────────────────────────────────────────────────────────────────

BANNER = r"""[bold cyan]
  _   _ _     ____  ____   ___  _
 | \ | | |   |___ \/ ___| / _ \| |
 |  \| | |     __) \___ \| | | | |
 | |\  | |___ / __/ ___) | |_| | |___
 |_| \_|_____|_____|____/ \__\_\_____|
[/bold cyan]"""

TAGLINE = "[muted]Multi-Agent Natural Language → SQL  •  Powered by LangGraph[/muted]"

VERSION = "0.1.0"


# ── Logging ──────────────────────────────────────────────────────────────────

def _setup_logging() -> str:
    """Configure file-based logging. Returns the log file path."""
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"nl2sql_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-30s  %(levelname)-5s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.FileHandler(log_file)],
    )
    return str(log_file)


# ── Pretty Output ───────────────────────────────────────────────────────────

def _print_sql(sql: str) -> None:
    """Render SQL with syntax highlighting inside a panel."""
    syntax = Syntax(
        sql.strip(),
        "sql",
        theme="monokai",
        line_numbers=False,
        word_wrap=True,
        padding=(1, 2),
    )
    console.print(
        Panel(
            syntax,
            title="[bold bright_white]SQL Query[/bold bright_white]",
            border_style="bright_cyan",
            box=box.ROUNDED,
            expand=True,
        )
    )


def _print_section(title: str, body: str, *, style: str = "white", border: str = "dim") -> None:
    """Render a text section inside a panel."""
    console.print(
        Panel(
            Text(body.strip(), style=style),
            title=f"[bold]{title}[/bold]",
            border_style=border,
            box=box.ROUNDED,
            expand=True,
            padding=(1, 2),
        )
    )


def _print_scores(candidate_scores) -> None:
    """Render validation scores as a compact table."""
    if not candidate_scores:
        return

    table = Table(
        title="Candidate Scores",
        box=box.SIMPLE_HEAVY,
        title_style="bold",
        header_style="bold bright_cyan",
        show_lines=False,
        padding=(0, 1),
    )
    table.add_column("#", style="dim", width=3, justify="right")
    table.add_column("Variant", min_width=14)
    table.add_column("Score", justify="right", min_width=7)
    table.add_column("Status", justify="center", min_width=8)

    for idx, cr in enumerate(candidate_scores, 1):
        score = f"{cr.total_score:.2f}"
        if cr.disqualified:
            status = "[red]✗ DQ[/red]"
            score_style = "dim red"
        elif cr.total_score >= 3.0:
            status = "[green]✓ Pass[/green]"
            score_style = "green"
        else:
            status = "[yellow]~ Warn[/yellow]"
            score_style = "yellow"

        table.add_row(
            str(idx),
            cr.candidate.prompt_variant,
            f"[{score_style}]{score}[/{score_style}]",
            status,
        )

    console.print(table)


def _print_output(result) -> None:
    """Pretty-print the full pipeline result."""
    console.print()
    _print_sql(result.sql)

    _print_section("Explanation", result.explanation, border="bright_green")
    _print_section("Safety Report", result.safety_report, border="bright_yellow")
    _print_section("Optimization Hints", result.optimization_hints, border="bright_magenta")

    if hasattr(result, "candidate_scores") and result.candidate_scores:
        _print_scores(result.candidate_scores)

    console.print()


# ── Core Runner ──────────────────────────────────────────────────────────────

async def _run_query(query: str, graph, config: dict, logger: logging.Logger) -> None:
    """Invoke the LangGraph pipeline and render results."""
    with console.status("[bold cyan]Agents working…[/bold cyan]", spinner="dots"):
        final_state = await graph.ainvoke({"user_query": query}, config)

    _print_output(final_state["output"])


# ── Argument Parser ──────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nl2sql",
        description="Convert natural language questions to SQL using a multi-agent LangGraph pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              nl2sql "Show me all singers from France"
              nl2sql --db ./spider/database/car_1/car_1.sqlite "List all cars"
              nl2sql                          (interactive mode)
        """),
    )
    parser.add_argument(
        "query",
        nargs="*",
        help="Natural language question (omit for interactive mode)",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        help="Override DB_PATH from .env",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {VERSION}",
    )
    return parser


# ── Interactive REPL ─────────────────────────────────────────────────────────

async def _interactive(graph, config: dict, logger: logging.Logger) -> None:
    """Run an interactive REPL session."""
    console.print(BANNER)
    console.print(f"  {TAGLINE}   [dim]v{VERSION}[/dim]\n")

    db_path = os.getenv("DB_PATH", "")
    db_name = Path(db_path).stem if db_path else "none"
    console.print(f"  [muted]Database:[/muted]  [bold]{db_name}[/bold]")
    console.print(f"  [muted]Type [bold white]quit[/bold white] or [bold white]q[/bold white] to exit[/muted]\n")
    console.print(Rule(style="dim"))
    console.print()

    while True:
        try:
            query = console.input("[prompt]❯[/prompt] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[muted]Goodbye![/muted]")
            break

        if not query or query.lower() in ("quit", "exit", "q"):
            console.print("[muted]Goodbye![/muted]")
            break

        try:
            await _run_query(query, graph, config, logger)
        except RuntimeError as exc:
            console.print(f"\n[error]Error:[/error] {exc}\n")
        except Exception as exc:
            logger.exception("Unexpected error")
            console.print(f"\n[error]Unexpected error:[/error] {exc}\n")


# ── Entry Points ─────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    """Async entry point."""
    # Override DB_PATH if --db flag is provided
    if args.db:
        os.environ["DB_PATH"] = args.db

    # Lazy import — keeps startup fast; logging & env are ready first
    from nl2sql_agents.orchestrator.pipeline import graph

    log_file = _setup_logging()
    logger = logging.getLogger("nl2sql.cli")
    logger.info("Logging to: %s", log_file)

    config = {"configurable": {"thread_id": "user"}}

    # Single-shot mode
    if args.query:
        query = " ".join(args.query)
        console.print(f"\n[muted]Question:[/muted]  {query}")
        await _run_query(query, graph, config, logger)
        return

    # Interactive REPL
    await _interactive(graph, config, logger)


def main() -> None:
    """Synchronous wrapper — this is the console_scripts entry point."""
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
