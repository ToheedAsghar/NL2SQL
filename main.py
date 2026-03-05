"""
CLI Entry Point (legacy)

Delegates to nl2sql_agents.cli so that both work:
    python main.py "Show me top 5 sales reps"
    nl2sql "Show me top 5 sales reps"          # after pip install
"""

from nl2sql_agents.cli import main

if __name__ == "__main__":
    main()