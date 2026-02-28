"""
ORCHESTRATOR - LANGGRAPH

- load schema       -> introspect DB or read from cache
- security_filter 
- discovery         -> PARALLEL
- gate
- format_schema
- generate_sql      -> PARALLEL
- validate          -> PARALLEL
- explain           -> PARALLEL
"""

from models.schemas import GraphState
from langgraph.graph import StateGraph, END

from nodes import gate_node
from nodes import load_schema
from nodes import explain_node
from nodes import should_retry
from nodes import validate_node
from nodes import discovery_node
from nodes import format_schema_node
from nodes import generate_sql_node
from nodes import security_filter_node

def build_graph() -> StateGraph:
    builder = StateGraph(GraphState)

    # - nodes - #
    builder.add_node("load_schema", load_schema)
    builder.add_node("security_filter", security_filter_node)
    builder.add_node("discovery", discovery_node)
    builder.add_node("gate", gate_node)
    builder.add_node("format_schema", format_schema_node)
    builder.add_node("generate_sql", generate_sql_node)
    builder.add_node("validate", validate_node)
    builder.add_node("explain", explain_node)

    # - edges - #
    builder.set_entry_point('load_schema')

    # parallel
    builder.add_edge("load_schema", 'security_filter')
    builder.add_edge("load_schema", 'discovery')

    # both branches converge at gate (LangGraph waits for both)
    builder.add_edge("security_filter", "gate")
    builder.add_edge("discovery", "gate")

    # sequential flow
    builder.add_edge("gate", 'format_schema')
    builder.add_edge("format_schema", 'generate_sql')
    builder.add_edge("generate_sql", 'validate')

    # conditional retyr loop
    builder.add_conditional_edges("validate", should_retry, {
        'generate_sql': "generate_sql",
        "explain": "explain"
    })

    # end
    builder.add_edge("explain", END)

    # compile
    return builder.compile()

graph = build_graph()