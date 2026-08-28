from __future__ import annotations

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from snowflake_dryrun.models import GlobalStats, ParsedPlan, PlanNode


def parse_sql(sql: str) -> exp.Expression:
    return parse_one(sql, read="snowflake")


def static_notes(sql: str) -> list[str]:
    notes: list[str] = []
    try:
        tree = parse_sql(sql)
    except ParseError as exc:
        return [f"SQL did not parse as Snowflake dialect: {exc}"]

    selects = list(tree.find_all(exp.Select))
    if any(s.find(exp.Star) for s in selects):
        notes.append("SELECT * will pull every column and can inflate scan bytes and network transfer.")
    if tree.find(exp.Window):
        notes.append("Window functions typically require a sort or partition shuffle of the input set.")
    if tree.find(exp.Distinct) or any(
        isinstance(s.args.get("distinct"), exp.Distinct) for s in selects if s.args.get("distinct")
    ):
        notes.append("DISTINCT forces a global aggregation and can spill if cardinality is high.")
    unions = list(tree.find_all(exp.Union))
    if any(not u.args.get("distinct") is False and u.args.get("distinct") for u in unions):
        notes.append("UNION (not UNION ALL) deduplicates the combined set.")
    return notes


def synthesize_plan(sql: str) -> ParsedPlan:
    """Build an EXPLAIN-shaped plan from the SQL AST when Snowflake is unavailable."""
    try:
        tree = parse_sql(sql)
    except ParseError:
        return ParsedPlan(
            nodes=[PlanNode(id=0, operation="ParseError", expressions=["Could not parse SQL"])],
            source="synthetic",
        )

    # Lift WHERE equalities onto comma joins so we do not emit a false CrossJoin.
    from snowflake_dryrun.advisor import _promote_comma_joins

    _promote_comma_joins(tree)

    nodes: list[PlanNode] = []
    next_id = 0

    def add(operation: str, **kwargs) -> PlanNode:
        nonlocal next_id
        node = PlanNode(id=next_id, operation=operation, **kwargs)
        nodes.append(node)
        next_id += 1
        return node

    result = add("Result")
    root_parent = result.id
    _walk(tree, add, parent_id=root_parent)

    bytes_assigned = _heuristic_bytes(tree)
    scans = [n for n in nodes if n.operation == "TableScan"]
    per = bytes_assigned // max(len(scans), 1)
    for scan in scans:
        scan.bytes_assigned = per
        scan.partitions_assigned = max(1, per // (16 * 1024 * 1024))
        scan.partitions_total = scan.partitions_assigned

    stats = GlobalStats(
        bytes_assigned=bytes_assigned,
        partitions_assigned=sum(n.partitions_assigned or 0 for n in scans),
        partitions_total=sum(n.partitions_total or 0 for n in scans),
    )
    raw = {
        "GlobalStats": {
            "partitionsTotal": stats.partitions_total,
            "partitionsAssigned": stats.partitions_assigned,
            "bytesAssigned": stats.bytes_assigned,
        },
        "Operations": [_node_to_explain(n) for n in nodes],
        "_synthetic": True,
    }
    return ParsedPlan(global_stats=stats, nodes=nodes, source="synthetic", raw=raw)


def _walk(node: exp.Expression, add, parent_id: int) -> None:
    if isinstance(node, (exp.Select, exp.Subquery, exp.Union)):
        if isinstance(node, exp.Union):
            union_op = add(
                "UnionAll" if node.args.get("distinct") is False else "Union",
                parent_ids=[parent_id],
            )
            if node.this:
                _walk(node.this, add, union_op.id)
            if node.expression:
                _walk(node.expression, add, union_op.id)
            return

        select = node.this if isinstance(node, exp.Subquery) else node
        if not isinstance(select, exp.Select):
            return

        current_parent = parent_id
        if select.find(exp.Window):
            win = add("WindowFunction", parent_ids=[current_parent], expressions=["window"])
            current_parent = win.id
        if select.args.get("limit"):
            lim = add("Limit", parent_ids=[current_parent], expressions=[select.args["limit"].sql(dialect="snowflake")])
            current_parent = lim.id
        if select.args.get("order"):
            sort = add("Sort", parent_ids=[current_parent], expressions=[select.args["order"].sql(dialect="snowflake")])
            current_parent = sort.id
        if select.args.get("distinct"):
            dist = add("Aggregate", parent_ids=[current_parent], expressions=["DISTINCT"])
            current_parent = dist.id
        if select.args.get("group"):
            agg = add(
                "Aggregate",
                parent_ids=[current_parent],
                expressions=[select.args["group"].sql(dialect="snowflake")],
            )
            current_parent = agg.id
        if select.args.get("having"):
            having = add(
                "Filter",
                parent_ids=[current_parent],
                expressions=[select.args["having"].sql(dialect="snowflake")],
            )
            current_parent = having.id
        if select.args.get("where"):
            filt = add(
                "Filter",
                parent_ids=[current_parent],
                expressions=[select.args["where"].sql(dialect="snowflake")],
            )
            current_parent = filt.id

        from_ = select.args.get("from_") or select.args.get("from")
        joins = list(select.args.get("joins") or [])
        if joins:
            _walk_joins(select, joins, from_, add, current_parent)
        elif from_:
            _walk_from_item(from_.this, add, current_parent)
        return

    if isinstance(node, exp.With):
        _walk(node.this, add, parent_id)
        return

    if node.this:
        _walk(node.this, add, parent_id)


def _walk_joins(select: exp.Select, joins: list[exp.Join], from_, add, parent_id: int) -> None:
    # Snowflake plans are built bottom-up; we emit join operators as a chain.
    current_parent = parent_id
    remaining = list(joins)
    # Innermost: leftmost table, then each join wrapping it.
    # For EXPLAIN-style trees, the join node is parent of both inputs.
    join_nodes: list[tuple[exp.Join, PlanNode]] = []
    for join in remaining:
        op_name, exprs = _join_descriptor(join)
        jn = add(op_name, parent_ids=[current_parent], expressions=exprs)
        join_nodes.append((join, jn))
        current_parent = jn.id

    # Attach left-most table to the innermost join (last created) or parent
    innermost_parent = join_nodes[-1][1].id if join_nodes else parent_id
    if from_:
        _walk_from_item(from_.this, add, innermost_parent)
    # Each join's right table hangs off that join node
    for join, jn in join_nodes:
        _walk_from_item(join.this, add, jn.id)
        if join.args.get("lateral") or _is_flatten(join.this):
            add(
                "Flatten",
                parent_ids=[jn.id],
                expressions=[join.this.sql(dialect="snowflake")],
                objects=["LATERAL/FLATTEN"],
            )


def _walk_from_item(item: exp.Expression, add, parent_id: int) -> None:
    if isinstance(item, exp.Alias):
        alias = item.alias
        inner = item.this
        if isinstance(inner, (exp.Select, exp.Subquery, exp.Union)):
            sub = add("Projection", parent_ids=[parent_id], alias=alias, expressions=["subquery"])
            _walk(inner, add, sub.id)
            return
        if isinstance(inner, exp.Table):
            add(
                "TableScan",
                parent_ids=[parent_id],
                objects=[inner.sql(dialect="snowflake")],
                alias=alias,
                expressions=[c.name for c in inner.parent.find_all(exp.Column)][:12] if inner.parent else [],
            )
            return
        if _is_flatten(inner):
            add("Flatten", parent_ids=[parent_id], alias=alias, expressions=[inner.sql(dialect="snowflake")])
            return
        _walk_from_item(inner, add, parent_id)
        return
    if isinstance(item, exp.Table):
        add("TableScan", parent_ids=[parent_id], objects=[item.sql(dialect="snowflake")])
        return
    if isinstance(item, (exp.Select, exp.Subquery, exp.Union)):
        _walk(item, add, parent_id)
        return
    if _is_flatten(item):
        add("Flatten", parent_ids=[parent_id], expressions=[item.sql(dialect="snowflake")])
        return
    if isinstance(item, exp.Join):
        # unexpected at this level
        return


def _is_flatten(node: exp.Expression) -> bool:
    sql = node.sql(dialect="snowflake").upper()
    return "FLATTEN(" in sql or "LATERAL FLATTEN" in sql or node.find(exp.Explode) is not None


def _join_descriptor(join: exp.Join) -> tuple[str, list[str]]:
    kind = (join.args.get("kind") or "").upper()
    side = (join.args.get("side") or "").upper()
    on = join.args.get("on")
    using = join.args.get("using")
    exprs: list[str] = []
    if on:
        exprs.append(on.sql(dialect="snowflake"))
    if using:
        exprs.append("USING " + using.sql(dialect="snowflake"))

    is_cross = kind == "CROSS" or (not on and not using and kind != "NATURAL")
    if is_cross:
        return "CrossJoin", exprs or ["no join predicate"]
    if side == "LEFT":
        return "LeftOuterJoin", exprs
    if side == "RIGHT":
        return "RightOuterJoin", exprs
    if side == "FULL":
        return "FullOuterJoin", exprs
    if kind == "INNER" or not kind:
        return "InnerJoin", exprs
    return f"{kind.title()}Join", exprs


def _heuristic_bytes(tree: exp.Expression) -> int:
    tables = list(tree.find_all(exp.Table))
    joins = list(tree.find_all(exp.Join))
    has_filter = tree.find(exp.Where) is not None
    base = 80 * 1024 * 1024  # 80 MiB per table as a conservative unknown
    total = max(len(tables), 1) * base
    if joins:
        total = int(total * (1.4 ** min(len(joins), 6)))
    if not has_filter:
        total = int(total * 1.8)
    if tree.find(exp.Star):
        total = int(total * 1.5)
    return total


def _node_to_explain(node: PlanNode) -> dict:
    d: dict = {
        "id": node.id,
        "operation": node.operation,
        "parentOperators": node.parent_ids,
    }
    if node.expressions:
        d["expressions"] = node.expressions
    if node.objects:
        d["objects"] = node.objects
    if node.alias:
        d["alias"] = node.alias
    if node.bytes_assigned is not None:
        d["bytesAssigned"] = node.bytes_assigned
    if node.partitions_assigned is not None:
        d["partitionsAssigned"] = node.partitions_assigned
    if node.partitions_total is not None:
        d["partitionsTotal"] = node.partitions_total
    return d
