from __future__ import annotations

import json
from typing import Any

from snowflake_dryrun.models import GlobalStats, ParsedPlan, PlanNode


def parse_explain_payload(payload: dict[str, Any] | str | list[Any]) -> ParsedPlan:
    """Normalize Snowflake EXPLAIN USING JSON output into a ParsedPlan.

    Snowflake may return:
    - a JSON object with Operations / GlobalStats
    - a string containing that JSON
    - a result set like [{"EXPLAIN": "<json>"}] or [{"step": "<json>"}]
    """
    data = _unwrap(payload)
    stats_raw = data.get("GlobalStats") or data.get("globalStats") or {}
    ops = data.get("Operations") or data.get("operations") or []
    nodes = [_parse_node(op) for op in ops if isinstance(op, dict)]
    stats = GlobalStats(
        partitions_total=_int(stats_raw.get("partitionsTotal")),
        partitions_assigned=_int(stats_raw.get("partitionsAssigned")),
        bytes_assigned=_int(stats_raw.get("bytesAssigned")),
    )
    if stats.bytes_assigned is None:
        assigned = [n.bytes_assigned for n in nodes if n.bytes_assigned]
        stats.bytes_assigned = sum(assigned) if assigned else None
    return ParsedPlan(global_stats=stats, nodes=nodes, source="pasted_json", raw=data)


def _unwrap(payload: dict[str, Any] | str | list[Any]) -> dict[str, Any]:
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(payload, list):
        if not payload:
            return {}
        row = payload[0]
        if isinstance(row, dict):
            for key in ("EXPLAIN", "explain", "step", "PLAN", "JSON"):
                if key in row:
                    return _unwrap(row[key])
            # connector sometimes returns the JSON as the only value
            if len(row) == 1:
                return _unwrap(next(iter(row.values())))
        if isinstance(row, str):
            return _unwrap(row)
        return {}
    if isinstance(payload, dict) and "Operations" not in payload and "operations" not in payload:
        # Single-column wrapper objects
        for key in ("EXPLAIN", "explain", "plan"):
            if key in payload:
                return _unwrap(payload[key])
    return payload if isinstance(payload, dict) else {}


def _parse_node(op: dict[str, Any]) -> PlanNode:
    expressions = op.get("expressions") or op.get("Expressions") or []
    if isinstance(expressions, str):
        expressions = [expressions]
    objects = op.get("objects") or op.get("Objects") or []
    if isinstance(objects, str):
        objects = [objects]
    parents = op.get("parentOperators") or op.get("parent_operators") or []
    extra = {
        k: v
        for k, v in op.items()
        if k
        not in {
            "id",
            "operation",
            "expressions",
            "Expressions",
            "objects",
            "Objects",
            "alias",
            "parentOperators",
            "parent_operators",
            "partitionsAssigned",
            "partitionsTotal",
            "bytesAssigned",
        }
    }
    return PlanNode(
        id=int(op.get("id") or 0),
        operation=str(op.get("operation") or op.get("Operation") or "Unknown"),
        expressions=[str(x) for x in expressions],
        objects=[str(x) for x in objects],
        alias=op.get("alias"),
        parent_ids=[int(p) for p in parents],
        partitions_assigned=_int(op.get("partitionsAssigned")),
        partitions_total=_int(op.get("partitionsTotal")),
        bytes_assigned=_int(op.get("bytesAssigned")),
        extra=extra,
    )


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
