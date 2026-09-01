from __future__ import annotations

import json
import re
from typing import Any

from snowflake_dryrun.models import GlobalStats, ParsedPlan, PlanNode


def parse_explain_payload(payload: dict[str, Any] | str | list[Any] | None) -> ParsedPlan:
    """Normalize Snowflake EXPLAIN USING JSON output into a ParsedPlan.

    Snowflake may return:
    - a JSON object with Operations / GlobalStats
    - Operations as a nested array of steps: [[op, op, ...]]
    - a string containing that JSON (worksheet cell, SYSTEM$EXPLAIN_PLAN_JSON)
    - a result set like [{"EXPLAIN": "<json>"}] or [{"step": "<json>"}]
    """
    data = _unwrap(payload)
    stats_raw = data.get("GlobalStats") or data.get("globalStats") or {}
    ops = _flatten_operations(data.get("Operations") or data.get("operations") or [])
    nodes = [_parse_node(op) for op in ops]
    stats = GlobalStats(
        partitions_total=_int(stats_raw.get("partitionsTotal")),
        partitions_assigned=_int(stats_raw.get("partitionsAssigned")),
        bytes_assigned=_int(stats_raw.get("bytesAssigned")),
    )
    if stats.bytes_assigned is None:
        assigned = [n.bytes_assigned for n in nodes if n.bytes_assigned]
        stats.bytes_assigned = sum(assigned) if assigned else None
    return ParsedPlan(global_stats=stats, nodes=nodes, source="pasted_json", raw=data if isinstance(data, dict) else {})


def _unwrap(payload: Any) -> dict[str, Any]:
    payload = _coerce_json(payload)
    if isinstance(payload, list):
        if not payload:
            return {}
        if all(isinstance(item, dict) and ("operation" in item or "Operation" in item or "id" in item) for item in payload):
            return {"Operations": payload}
        row = payload[0]
        if isinstance(row, dict):
            for key in ("EXPLAIN", "explain", "step", "PLAN", "JSON", "queryPlan"):
                if key in row:
                    return _unwrap(row[key])
            if len(row) == 1:
                return _unwrap(next(iter(row.values())))
            nested = row.get("Operations") or row.get("operations")
            if nested is not None:
                return _unwrap(row)
        if isinstance(row, (str, list)):
            return _unwrap(row)
        return {}
    if isinstance(payload, dict) and "Operations" not in payload and "operations" not in payload:
        for key in ("EXPLAIN", "explain", "plan", "queryPlan", "query_plan"):
            if key in payload:
                return _unwrap(payload[key])
        data = payload.get("data")
        if isinstance(data, list) and data:
            return _unwrap(data)
    return payload if isinstance(payload, dict) else {}


def _coerce_json(payload: Any) -> Any:
    if payload is None:
        return {}
    if isinstance(payload, (dict, list)):
        return payload
    if not isinstance(payload, str):
        return payload
    text = payload.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start_obj = text.find("{")
    start_arr = text.find("[")
    starts = [i for i in (start_obj, start_arr) if i >= 0]
    if not starts:
        raise json.JSONDecodeError("No JSON object found in EXPLAIN output", text, 0)
    start = min(starts)
    chunk = text[start:]
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(chunk)
        return value
    except json.JSONDecodeError:
        end_obj = text.rfind("}")
        end_arr = text.rfind("]")
        end = max(end_obj, end_arr)
        if end > start:
            return json.loads(text[start : end + 1])
        raise


def _flatten_operations(ops: Any) -> list[dict[str, Any]]:
    if isinstance(ops, dict):
        if "operation" in ops or "Operation" in ops or "id" in ops:
            return [ops]
        nested = ops.get("Operations") or ops.get("operations")
        return _flatten_operations(nested) if nested is not None else []
    if isinstance(ops, str):
        try:
            return _flatten_operations(_coerce_json(ops))
        except json.JSONDecodeError:
            return []
    if not isinstance(ops, list):
        return []
    out: list[dict[str, Any]] = []
    for item in ops:
        if isinstance(item, dict) and ("operation" in item or "Operation" in item or "id" in item):
            out.append(item)
        else:
            out.extend(_flatten_operations(item))
    return out


def _parse_node(op: dict[str, Any]) -> PlanNode:
    expressions = op.get("expressions") or op.get("Expressions") or []
    if isinstance(expressions, str):
        expressions = [expressions]
    objects = op.get("objects") or op.get("Objects") or []
    if isinstance(objects, str):
        objects = [objects]
    parents = op.get("parentOperators") or op.get("parent_operators") or op.get("parent")
    if parents is None:
        parents = []
    if isinstance(parents, (int, str)):
        parents = [parents]
    extra = {
        k: v
        for k, v in op.items()
        if k
        not in {
            "id",
            "operation",
            "Operation",
            "expressions",
            "Expressions",
            "objects",
            "Objects",
            "alias",
            "parentOperators",
            "parent_operators",
            "parent",
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
