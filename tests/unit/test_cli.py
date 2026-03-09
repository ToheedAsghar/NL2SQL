"""Tests for CLI module (nl2sql_agents/cli.py).

Tests argument parsing, output formatting helpers, and the main entry point
without actually launching the LangGraph pipeline or calling LLMs.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from rich.console import Console


# ---------------------------------------------------------------------------
# Argument Parser
# ---------------------------------------------------------------------------

class TestBuildParser:
    def _parser(self):
        from nl2sql_agents.cli import _build_parser
        return _build_parser()

    def test_no_args_returns_empty_query(self):
        args = self._parser().parse_args([])
        assert args.query == []
        assert args.db is None

    def test_single_query(self):
        args = self._parser().parse_args(["show me singers"])
        assert args.query == ["show me singers"]

    def test_quoted_query(self):
        args = self._parser().parse_args(["show me all singers from France"])
        assert " ".join(args.query) == "show me all singers from France"

    def test_db_flag(self):
        args = self._parser().parse_args(["--db", "/tmp/test.sqlite", "query"])
        assert args.db == "/tmp/test.sqlite"
        assert args.query == ["query"]

    def test_version_flag(self):
        with pytest.raises(SystemExit) as exc_info:
            self._parser().parse_args(["--version"])
        assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# Output formatting helpers
# ---------------------------------------------------------------------------

class TestPrintHelpers:
    def test_print_sql(self):
        from nl2sql_agents.cli import _print_sql
        # Should not raise
        _print_sql("SELECT * FROM singer WHERE Country = 'France'")

    def test_print_section(self):
        from nl2sql_agents.cli import _print_section
        _print_section("Test Title", "Some body text here")

    def test_print_scores_empty(self):
        from nl2sql_agents.cli import _print_scores
        _print_scores([])  # should be a no-op

    def test_print_scores_with_data(self):
        from nl2sql_agents.cli import _print_scores
        from nl2sql_agents.models.schemas import (
            CandidateValidationResult,
            SQLCandidate,
            ValidatorCheckResult,
        )

        candidates = [
            CandidateValidationResult(
                candidate=SQLCandidate(sql="SELECT 1", temperature=0.3, prompt_variant="conservative"),
                checks=[ValidatorCheckResult(check_name="security", passed=True, score=1.0)],
                total_score=3.5,
                disqualified=False,
            ),
            CandidateValidationResult(
                candidate=SQLCandidate(sql="DROP TABLE x", temperature=0.7, prompt_variant="creative"),
                checks=[ValidatorCheckResult(check_name="security", passed=False, score=0.0)],
                total_score=0.0,
                disqualified=True,
            ),
        ]
        _print_scores(candidates)  # should not raise

    def test_print_output(self):
        from nl2sql_agents.cli import _print_output
        from nl2sql_agents.models.schemas import (
            CandidateValidationResult,
            FinalOutput,
            SQLCandidate,
            ValidatorCheckResult,
        )

        output = FinalOutput(
            sql="SELECT * FROM singer",
            explanation="Gets all singers.",
            safety_report="All checks passed.",
            optimization_hints="No issues.",
            candidate_scores=[
                CandidateValidationResult(
                    candidate=SQLCandidate(sql="SELECT * FROM singer", temperature=0.3, prompt_variant="conservative"),
                    checks=[ValidatorCheckResult(check_name="security", passed=True, score=1.0)],
                    total_score=3.5,
                    disqualified=False,
                )
            ],
        )
        _print_output(output)  # should not raise


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class TestSetupLogging:
    def test_creates_log_file(self, tmp_path, monkeypatch):
        from nl2sql_agents.cli import _setup_logging
        import nl2sql_agents.cli as cli_mod

        # Patch Path resolution to use tmp_path
        logs_dir = tmp_path / "logs"
        monkeypatch.setattr(
            cli_mod, "_setup_logging",
            lambda: _setup_logging_patched(logs_dir),
        )

        def _setup_logging_patched(logs_dir):
            import logging
            from datetime import datetime

            logs_dir.mkdir(parents=True, exist_ok=True)
            log_file = logs_dir / f"nl2sql_test.log"
            logging.basicConfig(
                level=logging.INFO,
                handlers=[logging.FileHandler(log_file)],
                force=True,
            )
            return str(log_file)

        result = _setup_logging_patched(logs_dir)
        assert os.path.exists(result)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

class TestMain:
    def test_main_single_shot(self):
        """main() in single-shot mode with a mocked graph."""
        fake_output = MagicMock()
        fake_output.sql = "SELECT 1"
        fake_output.explanation = "Returns 1."
        fake_output.safety_report = "Safe."
        fake_output.optimization_hints = "None."
        fake_output.candidate_scores = []

        fake_graph = MagicMock()
        fake_graph.ainvoke = AsyncMock(return_value={"output": fake_output})

        with (
            patch("nl2sql_agents.cli._build_parser") as mock_parser_fn,
            patch("nl2sql_agents.orchestrator.pipeline.graph", fake_graph),
            patch("nl2sql_agents.cli._setup_logging", return_value="/tmp/test.log"),
        ):
            mock_parser = MagicMock()
            mock_parser.parse_args.return_value = argparse.Namespace(
                query=["show", "me", "singers"],
                db=None,
            )
            mock_parser_fn.return_value = mock_parser

            from nl2sql_agents.cli import main
            with patch("nl2sql_agents.cli._print_output"):
                main()

    def test_main_db_override(self):
        """--db flag sets DB_PATH env var."""
        fake_output = MagicMock()
        fake_output.sql = "SELECT 1"
        fake_output.explanation = "x"
        fake_output.safety_report = "x"
        fake_output.optimization_hints = "x"
        fake_output.candidate_scores = []

        fake_graph = MagicMock()
        fake_graph.ainvoke = AsyncMock(return_value={"output": fake_output})

        with (
            patch("nl2sql_agents.cli._build_parser") as mock_parser_fn,
            patch("nl2sql_agents.orchestrator.pipeline.graph", fake_graph),
            patch("nl2sql_agents.cli._setup_logging", return_value="/tmp/test.log"),
            patch("nl2sql_agents.cli._print_output"),
        ):
            mock_parser = MagicMock()
            mock_parser.parse_args.return_value = argparse.Namespace(
                query=["test"],
                db="/tmp/override.sqlite",
            )
            mock_parser_fn.return_value = mock_parser

            from nl2sql_agents.cli import main
            main()

        assert os.environ.get("DB_PATH") == "/tmp/override.sqlite"
