from __future__ import annotations

from snowflake_dryrun.models import Finding, ParsedPlan, PlanNode

CROSS_OPS = {"CROSSJOIN", "CARTESIANJOIN", "CARTESIANPRODUCT"}
JOIN_OPS = {
    "INNERJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
    "FULLOUTERJOIN",
    "JOIN",
    "HASHJOIN",
    "MERGEJOIN",
    "NESTEDLOOPJOIN",
    *CROSS_OPS,
}
SCAN_OPS = {"TABLESCAN", "EXTERNALSCAN", "VALUESCAN", "WITHSCAN"}


def analyze_plan(plan: ParsedPlan, sql: str = "") -> list[Finding]:
    findings: list[Finding] = []
    by_id = {n.id: n for n in plan.nodes}
    children = _children_map(plan.nodes)
    sql_u = sql.upper()

    findings.extend(_join_findings(plan.nodes, by_id, children))
    findings.extend(_flatten_findings(plan.nodes))
    findings.extend(_scan_findings(plan, by_id, children))
    findings.extend(_sort_window_findings(plan, by_id, children))
    findings.extend(_union_findings(plan.nodes))
    findings.extend(_complexity_findings(plan.nodes))
    if "SELECT *" in sql_u.replace("\n", " "):
        findings.append(
            Finding(
                code="SELECT_STAR",
                severity="medium",
                title="SELECT * expands every column",
                detail="The query projects all columns. That increases bytes scanned, shuffled, and returned even when later unused.",
                hint="List only the columns the consumer needs.",
            )
        )
    return _dedupe(findings)


def _children_map(nodes: list[PlanNode]) -> dict[int, list[PlanNode]]:
    children: dict[int, list[PlanNode]] = {n.id: [] for n in nodes}
    for n in nodes:
        for p in n.parent_ids:
            children.setdefault(p, []).append(n)
    return children


def _norm(op: str) -> str:
    return op.replace(" ", "").replace("_", "").upper()


def _join_findings(
    nodes: list[PlanNode], by_id: dict[int, PlanNode], children: dict[int, list[PlanNode]]
) -> list[Finding]:
    out: list[Finding] = []
    for node in nodes:
        op = _norm(node.operation)
        if op not in JOIN_OPS and "JOIN" not in op:
            continue
        exprs = " ".join(node.expressions).lower()
        has_pred = bool(exprs.strip()) and "no join predicate" not in exprs
        extra_on = str(node.extra.get("joinType") or node.extra.get("type") or "")
        cartesian_hint = "cartesian" in extra_on.lower() or "cross" in extra_on.lower()

        if op in CROSS_OPS or cartesian_hint or (not has_pred and "NATURAL" not in op):
            # InnerJoin with ON is fine
            if has_pred and op not in CROSS_OPS and not cartesian_hint:
                continue
            if not has_pred or op in CROSS_OPS or cartesian_hint:
                inputs = children.get(node.id, [])
                tables = []
                for child in inputs:
                    tables.extend(child.objects or ([child.alias] if child.alias else [child.operation]))
                table_note = " × ".join(tables) if tables else "two inputs"
                out.append(
                    Finding(
                        code="CROSS_JOIN",
                        severity="critical",
                        title="Cartesian / cross join — row explosion",
                        detail=(
                            f"Operator {node.id} ({node.operation}) has no join predicate. "
                            f"Output rows are the product of {table_note}. "
                            "This is the usual reason a dry-run looks cheap until the warehouse melts."
                        ),
                        operator_ids=[node.id],
                        hint="Add an ON / USING clause, or rewrite as a filtered INNER JOIN. If the cross product is intentional, add a LIMIT and comment why.",
                    )
                )
        elif "NESTEDLOOP" in op:
            out.append(
                Finding(
                    code="NESTED_LOOP_JOIN",
                    severity="high",
                    title="Nested-loop join",
                    detail=f"Operator {node.id} is a nested-loop join, which scales poorly as the inner input grows.",
                    operator_ids=[node.id],
                    hint="Ensure join keys are same type and clustering/pruning can reduce both sides.",
                )
            )
        elif op == "FULLOUTERJOIN":
            out.append(
                Finding(
                    code="FULL_OUTER_JOIN",
                    severity="medium",
                    title="Full outer join",
                    detail=f"Operator {node.id} is a FULL OUTER JOIN. Snowflake cannot prune as aggressively and both sides must be fully materialized.",
                    operator_ids=[node.id],
                    hint="Prefer LEFT JOIN plus an anti-join UNION if you only need unmatched rows from one side.",
                )
            )
    return out


def _flatten_findings(nodes: list[PlanNode]) -> list[Finding]:
    out: list[Finding] = []
    for node in nodes:
        op = _norm(node.operation)
        exprs = " ".join(node.expressions).upper()
        if op == "FLATTEN" or "FLATTEN(" in exprs or "LATERAL" in op:
            out.append(
                Finding(
                    code="FLATTEN_EXPLODE",
                    severity="high",
                    title="FLATTEN / LATERAL can multiply rows",
                    detail=(
                        f"Operator {node.id} expands nested arrays/objects. "
                        "Each input row can become thousands of output rows; a join after flatten compounds that."
                    ),
                    operator_ids=[node.id],
                    hint="Filter the variant before flattening, or flatten after reducing the driving table.",
                )
            )
    return out


def _scan_findings(
    plan: ParsedPlan, by_id: dict[int, PlanNode], children: dict[int, list[PlanNode]]
) -> list[Finding]:
    out: list[Finding] = []
    bytes_total = plan.global_stats.bytes_assigned or 0
    for node in plan.nodes:
        if _norm(node.operation) not in SCAN_OPS and "SCAN" not in _norm(node.operation):
            continue
        obj = ", ".join(node.objects) or node.alias or f"scan #{node.id}"
        pruned = None
        if node.partitions_total and node.partitions_assigned is not None:
            pruned = 1 - (node.partitions_assigned / max(node.partitions_total, 1))
        ancestors = _ancestors(node, by_id)
        has_filter = any(_norm(a.operation) == "FILTER" for a in ancestors)
        if node.bytes_assigned and node.bytes_assigned >= 5 * 1024**3:
            out.append(
                Finding(
                    code="LARGE_SCAN",
                    severity="high",
                    title="Large table scan",
                    detail=f"{obj} assigns about {_fmt_bytes(node.bytes_assigned)}. That dominates warehouse runtime.",
                    operator_ids=[node.id],
                    hint="Push predicates that match the clustering/micro-partition key (date, account_id, etc.).",
                )
            )
        elif bytes_total >= 10 * 1024**3 and node.bytes_assigned and node.bytes_assigned >= bytes_total * 0.4:
            out.append(
                Finding(
                    code="DOMINANT_SCAN",
                    severity="medium",
                    title="Scan dominates assigned bytes",
                    detail=f"{obj} is {_fmt_bytes(node.bytes_assigned)} of {_fmt_bytes(bytes_total)} assigned.",
                    operator_ids=[node.id],
                    hint="Confirm this table needs to be read in full for this dry-run.",
                )
            )
        if pruned is not None and pruned < 0.1 and (node.partitions_total or 0) >= 8:
            out.append(
                Finding(
                    code="WEAK_PRUNING",
                    severity="medium",
                    title="Little micro-partition pruning",
                    detail=(
                        f"{obj} assigns {node.partitions_assigned}/{node.partitions_total} partitions "
                        f"({pruned:.0%} pruned)."
                    ),
                    operator_ids=[node.id],
                    hint="Filter on clustered columns, or cluster/search-optimize the table if this query is frequent.",
                )
            )
        if not has_filter and (node.bytes_assigned or 0) >= 64 * 1024**2:
            out.append(
                Finding(
                    code="UNFILTERED_SCAN",
                    severity="medium",
                    title="Scan with no filter above it",
                    detail=f"{obj} has no Filter ancestor in the plan, so the warehouse likely reads the assigned partitions in full.",
                    operator_ids=[node.id],
                    hint="Add a WHERE/JOIN predicate that Snowflake can push into the scan.",
                )
            )
        if _norm(node.operation) == "EXTERNALSCAN":
            out.append(
                Finding(
                    code="EXTERNAL_SCAN",
                    severity="medium",
                    title="External table / stage scan",
                    detail=f"{obj} is an external scan. Runtime depends on warehouse size and remote listing, not only bytes.",
                    operator_ids=[node.id],
                    hint="Prefer a materialized Snowflake table or Iceberg table for repeated dry-runs of this shape.",
                )
            )
    return out


def _sort_window_findings(
    plan: ParsedPlan, by_id: dict[int, PlanNode], children: dict[int, list[PlanNode]]
) -> list[Finding]:
    out: list[Finding] = []
    bytes_total = plan.global_stats.bytes_assigned or 0
    for node in plan.nodes:
        op = _norm(node.operation)
        ancestors = _ancestors(node, by_id)
        has_limit = any(_norm(a.operation) == "LIMIT" for a in ancestors)
        if op in {"SORT", "ORDERBY"} and not has_limit:
            sev: str = "high" if bytes_total >= 1024**3 else "medium"
            out.append(
                Finding(
                    code="SORT_NO_LIMIT",
                    severity=sev,  # type: ignore[arg-type]
                    title="Global sort without LIMIT",
                    detail=f"Operator {node.id} sorts the stream. Sorts spill to local disk when they exceed warehouse memory.",
                    operator_ids=[node.id],
                    hint="If you only need a sample or top-N, add QUALIFY / LIMIT. Otherwise size the warehouse for the sort footprint.",
                )
            )
        if "WINDOW" in op:
            out.append(
                Finding(
                    code="WINDOW_FUNCTION",
                    severity="medium",
                    title="Window function",
                    detail=f"Operator {node.id} computes window functions over a partition. Uneven partitions cause stragglers.",
                    operator_ids=[node.id],
                    hint="Partition on a high-cardinality key that is already filtered; avoid ORDER BY on unbounded frames when possible.",
                )
            )
    return out


def _union_findings(nodes: list[PlanNode]) -> list[Finding]:
    out: list[Finding] = []
    for node in nodes:
        op = _norm(node.operation)
        if op == "UNION" or (op.startswith("UNION") and "ALL" not in op):
            out.append(
                Finding(
                    code="UNION_DEDUPE",
                    severity="low",
                    title="UNION deduplicates",
                    detail=f"Operator {node.id} is UNION, which sorts/hashes to remove duplicates.",
                    operator_ids=[node.id],
                    hint="Use UNION ALL if duplicates cannot occur or do not matter.",
                )
            )
    return out


def _complexity_findings(nodes: list[PlanNode]) -> list[Finding]:
    joins = [n for n in nodes if "JOIN" in _norm(n.operation)]
    if len(joins) >= 6:
        return [
            Finding(
                code="JOIN_HEAVY",
                severity="medium",
                title="Many joins in one statement",
                detail=f"The plan has {len(joins)} join operators. Compile time and optimizer join-order risk both rise.",
                operator_ids=[n.id for n in joins],
                hint="Break into CTEs materialized with CREATE TEMP TABLE if the optimizer picks a bad order.",
            )
        ]
    return []


def _ancestors(node: PlanNode, by_id: dict[int, PlanNode]) -> list[PlanNode]:
    out: list[PlanNode] = []
    seen: set[int] = set()
    stack = list(node.parent_ids)
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        parent = by_id.get(pid)
        if not parent:
            continue
        out.append(parent)
        stack.extend(parent.parent_ids)
    return out


def _fmt_bytes(n: int) -> str:
    step = 1024.0
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < step or unit == "TiB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= step
    return f"{n} B"


def _dedupe(findings: list[Finding]) -> list[Finding]:
    seen: set[tuple] = set()
    out: list[Finding] = []
    for f in findings:
        key = (f.code, tuple(f.operator_ids), f.title)
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    out.sort(key=lambda f: (order[f.severity], f.code))
    return out
