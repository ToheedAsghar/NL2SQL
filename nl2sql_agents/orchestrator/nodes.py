import logging

from nl2sql_agents.filters.gate import GateLayer
from nl2sql_agents.db.connector import DatabaseConnector
from nl2sql_agents.cache.schema_cache import SchemaCache
from nl2sql_agents.filters.security_filter import SecurityFilter
from nl2sql_agents.models.schemas import GraphState, FinalOutput
from nl2sql_agents.agents.query_generator import QueryGeneratorAgent
from nl2sql_agents.agents.schema_formatter import SchemaFormatterAgent
from nl2sql_agents.config.settings import DB_PATH, DB_TYPE, MAX_RETRIES
from nl2sql_agents.agents.discovery.discovery_agent import DiscoveryAgent
from nl2sql_agents.agents.validator.validator_agent import ValidatorAgent
from nl2sql_agents.agents.explainer.explainer_agent import ExplainerAgent


logger = logging.getLogger(__name__)

cache = SchemaCache()
gate_layer = GateLayer()
connector = DatabaseConnector()
security_filter=SecurityFilter(connector)

# --- 5 agents --- # 
discovery_agent = DiscoveryAgent()
formatter_agent = SchemaFormatterAgent()
generator_agent = QueryGeneratorAgent()
validator_agent = ValidatorAgent()
explainer_agent = ExplainerAgent()

async def load_schema(state: GraphState) -> dict:
    """Load Tables from cache or Introspect the database"""
    tables = cache.get(DB_PATH)
    
    if tables is None:
        tables = await connector.introspect()
        cache.set(DB_PATH, tables)

    return {"tables": tables, "attempt": 1}

async def security_filter_node(state: GraphState) -> dict:
    approved = await security_filter.filter(state["tables"])
    approved_names = {t.table_name for t in approved}

    logger.info("SecurityFilterNode: %d tables approved", len(approved))

    return {
        "security_passed": approved_names
    }

async def discovery_node(state: GraphState) -> dict:
    result = await discovery_agent.run(state['tables'], state['user_query'])
    
    return {
        "discovery_result": result
    }

async def gate_node(state: GraphState) -> dict:
    gate_result = gate_layer.evaluate(
        state['discovery_result'].scored_tables,
        state.get('security_passed', set())
    )

    if not gate_result.passed:
        raise RuntimeError(f"Gate Blocked {gate_result.reason}")

    logger.info("Gate Passed: %d tables", len(gate_result.tables))
    return {
        'gated_tables': gate_result.tables
    }

async def format_schema_node(state: GraphState) -> dict:
    formatted = await formatter_agent.format(state['gated_tables'])
    return {
        'formatted_schema': formatted
    }

async def generate_sql_node(state: GraphState) -> dict:
    """PARALLEL: Generates N SQL Candidates parallel."""

    logger.info("Generation Attempt: %d of %d", state['attempt'], MAX_RETRIES)

    generation = await generator_agent.generate(
        state['formatted_schema'],
        state['user_query'],
        retry_context=state.get('retry_context'),
        chat_history=state.get('chat_history'),
    )

    return {
        'generation':generation
    }

async def validate_node(state: GraphState) -> dict:
    validation = await validator_agent.validate(state['generation'], state['user_query'])

    if not validation.passed:
        return {
            'validation': validation,
            'retry_context': validation.retry_context,
            'attempt': state['attempt'] + 1
        }
    
    return {
        'validation': validation
    }

def should_retry(state: GraphState) -> str:
    validation = state['validation']

    if validation.passed:
        return 'explain'
    
    if state['attempt'] > MAX_RETRIES + 1:
        # -- not raising implementation error --- #
        logger.info("SHOULD_RETRY: attempts exceeded but validation not passed")
        return 'explain'

    logger.warning("Retry %d: %s", state["attempt"] - 1, state.get("retry_context"))

    return "generate_sql"

async def explain_node(state: GraphState) -> dict:
    best = state['validation'].best_candidate
    output = await explainer_agent.explain(
        best, state['validation'].all_results,
        state['user_query'],
    )

    history = list(state.get('chat_history') or [])
    history.append({"role": "user", "content": state['user_query']})
    history.append({"role": "assistant", "content": best.sql})

    return {
        'output': FinalOutput(
            sql=best.sql,
            explanation=output.explanation,
            safety_report=output.safety_report,
            optimization_hints=output.optimization_hints,
            candidate_scores=state["validation"].all_results,
        ),
        'chat_history': history,
    }
