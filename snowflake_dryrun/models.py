from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

Severity = Literal["critical", "high", "medium", "low", "info"]
WarehouseSize = Literal[
    "XSMALL",
    "SMALL",
    "MEDIUM",
    "LARGE",
    "XLARGE",
    "XXLARGE",
    "XXXLARGE",
    "X4LARGE",
    "X5LARGE",
    "X6LARGE",
]


class Finding(BaseModel):
    code: str
    severity: Severity
    title: str
    detail: str
    operator_ids: list[int] = Field(default_factory=list)
    hint: str | None = None


class Rewrite(BaseModel):
    title: str
    reason: str
    sql: str
    finding_codes: list[str] = Field(default_factory=list)
    safe: bool = False


class PlanNode(BaseModel):
    id: int
    operation: str
    expressions: list[str] = Field(default_factory=list)
    objects: list[str] = Field(default_factory=list)
    alias: str | None = None
    parent_ids: list[int] = Field(default_factory=list)
    partitions_assigned: int | None = None
    partitions_total: int | None = None
    bytes_assigned: int | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class GlobalStats(BaseModel):
    partitions_total: int | None = None
    partitions_assigned: int | None = None
    bytes_assigned: int | None = None


class ParsedPlan(BaseModel):
    global_stats: GlobalStats = Field(default_factory=GlobalStats)
    nodes: list[PlanNode] = Field(default_factory=list)
    source: Literal["snowflake", "pasted_json", "synthetic"] = "synthetic"
    raw: dict[str, Any] = Field(default_factory=dict)


class WarehouseAdvice(BaseModel):
    given_size: WarehouseSize
    recommended_size: WarehouseSize
    estimated_seconds_on_given: float
    estimated_seconds_on_recommended: float
    credit_hours_on_given: float
    credit_hours_on_recommended: float
    work_score: float
    bytes_assigned: int | None = None
    rationale: list[str] = Field(default_factory=list)
    scale_note: str


class DryRunResult(BaseModel):
    sql: str
    source: str
    findings: list[Finding]
    plan: ParsedPlan
    warehouse: WarehouseAdvice
    static_notes: list[str] = Field(default_factory=list)
    connected: bool = False
    score: int = 100
    score_label: str = "healthy"
    rewrites: list[Rewrite] = Field(default_factory=list)
    advised_sql: str | None = None


class DryRunRequest(BaseModel):
    sql: str = ""
    explain_json: dict[str, Any] | str | None = None
    warehouse_size: WarehouseSize = "XSMALL"
    account: str | None = None
    user: str | None = None
    password: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    role: str | None = None
    authenticator: str | None = None

    model_config = {"populate_by_name": True}
