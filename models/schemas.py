"""
Models/State for data flowing between all pipeline components.
"""

import operator
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Annotated
from typing_extensions import TypedDict

# --- Database/Schema --- #

class ColumnMetaData(BaseModel):
    column_name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    reference_table: Optional[str] = None
    reference_column: Optional[str] = None

class TableMetaData(BaseModel):
    table_name: str
    schema_name: str
    rows_count: Optional[int] = None
    columns: list[ColumnMetaData] = Field(default_factory=list)
    comments: Optional[str] = None

# --- discovery --- #

class ScoredTable(BaseModel):
    table: TableMetaData
    score: float
    found_by: list[str] = Field(default_factory=list)

class DiscoveryResult(BaseModel):
    top_tables: list[TableMetaData]
    scored_tables: list[ScoredTable]

# --- Schema Formatter --- #

class FormattedSchema(BaseModel):
    content: str
    table_names: list[str]
    token_estimate: int

# --- Query Generation --- #

class SQLCandidate(BaseModel):
    sql: str
    temperature: float
    prompt_variant: str # # "conservative" | "creative" | "rephrased"

class GenerationResult(BaseModel):
    candidates: list[SQLCandidate]

# --- Validation --- #

class ValidatorCheckResult(BaseModel):
    check_name: str
    passed: bool
    score: float = 0.0
    details: Optional[str] = None

class CandidateValidationResult(BaseModel):
    candidate: SQLCandidate
    checks: list[ValidatorCheckResult]
    total_score: float
    disqualified: bool = False

class ValidationResult(BaseModel):
    best_candidate: Optional[SQLCandidate] = None
    all_results: list[CandidateValidationResult]
    passed: bool
    retry_context: Optional[str] = None

# --- Final Output --- #
class FinalOutput(BaseModel):
    sql: str
    explanation: str
    safety_report: str
    optimization_hints: str
    candidate_scores: list[CandidateValidationResult]

# --- Graph State --- #

class GraphState(TypedDict, total=False):
    user_query: str
    tables: list[TableMetaData]

    # written by security_filter and discovery node (parallel branches)
    security_passed: Annotated[set[str], operator.or_]
    discovery_result: DiscoveryResult

    # gate onwards
    gated_tables: list[TableMetaData]
    formatted_schema: FormattedSchema
    generation: GenerationResult
    validation: ValidationResult
    retry_context: Optional[str]
    attempt: int
    output: FinalOutput