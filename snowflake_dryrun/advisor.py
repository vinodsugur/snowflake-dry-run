from __future__ import annotations

from snowflake_dryrun.models import Finding, Rewrite
from snowflake_dryrun.sql_static import parse_sql
from sqlglot import exp
from sqlglot.errors import ParseError

def advise_sql(sql: str) -> tuple[list[Finding], list[Rewrite], str | None]:
    """SQL-level findings and semantically safe rewrites (join style, sargable filters)."""
    sql = (sql or "").strip()
    if not sql:
        return [], [], None
    try:
        tree = parse_sql(sql)
    except ParseError:
        return [], [], None

    findings: list[Finding] = []
    rewrites: list[Rewrite] = []

    findings.extend(_sql_pattern_findings(tree, sql))

    working = parse_sql(sql)
    changed_parts: list[str] = []
    codes: list[str] = []

    if _promote_comma_joins(working):
        changed_parts.append("comma joins → INNER JOIN … ON")
        codes.append("COMMA_JOIN")
        findings.append(
            Finding(
                code="COMMA_JOIN",
                severity="medium",
                title="Old-style comma join",
                detail=(
                    "The FROM clause lists tables with commas. Equality predicates in WHERE were treated as "
                    "join keys, but Snowflake (and this advisor) see a clearer plan when those keys sit in ON."
                ),
                hint="Rewrite as INNER JOIN … ON so join keys are explicit and cannot be dropped from WHERE by accident.",
            )
        )

    if _rewrite_sargable_filters(working):
        changed_parts.append("function-wrapped filters → range predicates")
        codes.append("NON_SARGABLE")

    if _rewrite_or_to_in(working):
        changed_parts.append("OR equalities → IN")
        codes.append("OR_PREDICATE")

    advised = working.sql(dialect="snowflake")
    original_norm = parse_sql(sql).sql(dialect="snowflake")
    advised_sql = advised if advised != original_norm else None
    if advised_sql:
        rewrites.append(
            Rewrite(
                title="Safer equivalent SQL",
                reason="; ".join(changed_parts) + ". Semantics are preserved; pruning and join type become obvious.",
                sql=advised_sql,
                finding_codes=codes,
                safe=True,
            )
        )

    union_sql = _union_all_suggestion(sql)
    if union_sql:
        rewrites.append(
            Rewrite(
                title="UNION ALL if duplicates are acceptable",
                reason="UNION hashes/sorts to dedupe. UNION ALL skips that work when duplicates cannot occur or do not matter.",
                sql=union_sql,
                finding_codes=["UNION_DEDUPE"],
                safe=False,
            )
        )

    limit_sql = _limit_suggestion(sql)
    if limit_sql:
        rewrites.append(
            Rewrite(
                title="Add LIMIT for exploration",
                reason="A global sort or unbounded scan on a large table is cheaper to inspect with LIMIT (not for production totals).",
                sql=limit_sql,
                finding_codes=["SORT_NO_LIMIT", "UNFILTERED_SCAN"],
                safe=False,
            )
        )

    return findings, rewrites, advised_sql


def score_findings(findings: list[Finding]) -> tuple[int, str]:
    score = 100
    weights = {"critical": 40, "high": 18, "medium": 8, "low": 3, "info": 0}
    for f in findings:
        score -= weights.get(f.severity, 5)
    score = max(0, min(100, score))
    if score >= 85:
        label = "healthy"
    elif score >= 65:
        label = "watch"
    elif score >= 40:
        label = "risky"
    else:
        label = "dangerous"
    return score, label


def _sql_pattern_findings(tree: exp.Expression, sql: str) -> list[Finding]:
    out: list[Finding] = []
    for like in tree.find_all(exp.Like):
        pattern = like.expression
        if isinstance(pattern, exp.Literal) and str(pattern.this).startswith("%"):
            out.append(
                Finding(
                    code="LEADING_WILDCARD",
                    severity="medium",
                    title="Leading-wildcard LIKE cannot prune",
                    detail=f"Predicate {like.sql(dialect='snowflake')} starts with '%', so Snowflake cannot use clustering or micro-partition min/max on that column.",
                    hint="Prefer a prefix match (LIKE 'foo%'), an equality, or search optimization / a dedicated search table.",
                )
            )

    if _has_non_sargable(tree):
        out.append(
            Finding(
                code="NON_SARGABLE",
                severity="high",
                title="Function wrapping a filter column",
                detail=(
                    "A WHERE/HAVING predicate applies YEAR, DATE, TO_DATE, or CAST to a column. "
                    "That hides micro-partition min/max values and often disables pruning."
                ),
                hint="Rewrite as a closed range on the raw column (the advisor suggests equivalent SQL when it can).",
            )
        )

    if _or_chain_length(tree) >= 3:
        out.append(
            Finding(
                code="OR_PREDICATE",
                severity="low",
                title="Chain of OR equalities",
                detail="Several OR'd equalities on the same column are harder to read and sometimes plan than a single IN list.",
                hint="Use col IN (...), or UNION ALL branches if the optimizer still over-scans.",
            )
        )

    if _looks_like_fact_without_time_filter(tree, sql):
        out.append(
            Finding(
                code="MISSING_TIME_FILTER",
                severity="medium",
                title="Fact-style table with no time filter",
                detail=(
                    "The statement reads a table whose name looks like a fact or event log, but there is no date/time "
                    "predicate. Warehouses then scan every micro-partition that clustering cannot skip."
                ),
                hint="Add a range on the clustering/event date (for example event_date >= DATEADD(day, -7, CURRENT_DATE())).",
            )
        )
    return out


def _has_non_sargable(tree: exp.Expression) -> bool:
    for where in list(tree.find_all(exp.Where)) + list(tree.find_all(exp.Having)):
        if where.this and _walk_non_sargable(where.this):
            return True
    return False


def _walk_non_sargable(node: exp.Expression) -> bool:
    if isinstance(node, (exp.And, exp.Or, exp.Not, exp.Paren)):
        return any(_walk_non_sargable(c) for c in node.iter_expressions() if isinstance(c, exp.Expression))
    if isinstance(node, exp.Predicate) or isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        wrapped = node.this
        if _is_column_wrapper(wrapped):
            return True
    return False


def _is_column_wrapper(node: exp.Expression | None) -> bool:
    if node is None:
        return False
    if isinstance(node, (exp.Year, exp.Month, exp.Day, exp.Date, exp.TsOrDsToDate, exp.Cast, exp.Anonymous, exp.DateTrunc)):
        return node.find(exp.Column) is not None
    return False


def _or_chain_length(tree: exp.Expression) -> int:
    best = 0
    for node in tree.find_all(exp.Or):
        parts = _flatten_or(node)
        best = max(best, len(parts) if len(parts) > 1 else 0)
    return best


def _looks_like_fact_without_time_filter(tree: exp.Expression, sql: str) -> bool:
    names = " ".join(t.sql(dialect="snowflake") for t in tree.find_all(exp.Table)).upper()
    factish = any(
        token in names
        for token in ("FACT", "EVENTS", "EVENT_", "PAGE_VIEW", "CLICK", "ORDERS", "TRANSACTIONS", "LOGS")
    )
    if not factish:
        return False
    sql_u = sql.upper()
    time_tokens = (
        "DATE",
        "TIME",
        "DAY",
        "MONTH",
        "YEAR",
        "TS",
        "TIMESTAMP",
        "CREATED",
        "UPDATED",
        "EVENT_DATE",
        "ORDER_DATE",
    )
    where = tree.find(exp.Where)
    if not where:
        return True
    where_sql = where.sql(dialect="snowflake").upper()
    return not any(tok in where_sql for tok in time_tokens) and "CURRENT_DATE" not in sql_u


def _select_from(select: exp.Select):
    return select.args.get("from_") or select.args.get("from")


def _promote_comma_joins(tree: exp.Expression) -> bool:
    changed = False
    for select in tree.find_all(exp.Select):
        if _promote_comma_joins_in_select(select):
            changed = True
    return changed


def _promote_comma_joins_in_select(select: exp.Select) -> bool:
    joins = list(select.args.get("joins") or [])
    if not joins:
        return False
    where = select.args.get("where")
    if not where or where.this is None:
        return False

    from_ = _select_from(select)
    seen = []
    if from_ and from_.this is not None:
        alias = _alias_or_name(from_.this)
        if alias:
            seen.append(alias)

    predicates = _flatten_and(where.this)
    used: set[int] = set()
    changed = False

    for join in joins:
        kind = (join.args.get("kind") or "").upper()
        right = _alias_or_name(join.this)
        if join.args.get("on") or join.args.get("using") or kind in {"CROSS", "NATURAL"}:
            if right:
                seen.append(right)
            continue
        if not right:
            continue
        matching = []
        for i, pred in enumerate(predicates):
            if i in used:
                continue
            if _eq_links(pred, seen, right):
                matching.append(i)
        if matching:
            on_expr = predicates[matching[0]]
            for i in matching[1:]:
                on_expr = exp.and_(on_expr, predicates[i])
            join.set("on", on_expr.copy())
            join.set("kind", "INNER")
            used.update(matching)
            changed = True
        seen.append(right)

    if not changed:
        return False
    leftover = [p for i, p in enumerate(predicates) if i not in used]
    if leftover:
        combined = leftover[0]
        for extra in leftover[1:]:
            combined = exp.and_(combined, extra)
        where.set("this", combined)
    else:
        select.set("where", None)
    return True


def _rewrite_sargable_filters(tree: exp.Expression) -> bool:
    changed = False
    for where in list(tree.find_all(exp.Where)) + list(tree.find_all(exp.Having)):
        if where.this is None:
            continue
        new, did = _rewrite_pred(where.this)
        if did:
            where.set("this", new)
            changed = True
    return changed


def _rewrite_pred(node: exp.Expression) -> tuple[exp.Expression, bool]:
    if isinstance(node, exp.And):
        left, lch = _rewrite_pred(node.this)
        right, rch = _rewrite_pred(node.expression)
        node.set("this", left)
        node.set("expression", right)
        return node, lch or rch
    if isinstance(node, exp.Or):
        left, lch = _rewrite_pred(node.this)
        right, rch = _rewrite_pred(node.expression)
        node.set("this", left)
        node.set("expression", right)
        return node, lch or rch
    if isinstance(node, exp.Paren):
        inner, ch = _rewrite_pred(node.this)
        node.set("this", inner)
        return node, ch

    replacement = _sargable_replacement(node)
    if replacement is not None:
        return replacement, True
    return node, False


def _sargable_replacement(node: exp.Expression) -> exp.Expression | None:
    if not isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return None
    wrapped = node.this
    lit = node.expression
    if not isinstance(lit, exp.Literal):
        return None
    col = _wrapped_column(wrapped)
    if col is None:
        return None

    if isinstance(wrapped, exp.Year) and isinstance(node, exp.EQ) and not lit.is_string:
        try:
            year = int(lit.this)
        except (TypeError, ValueError):
            return None
        start = exp.Literal.string(f"{year}-01-01")
        end = exp.Literal.string(f"{year + 1}-01-01")
        return exp.and_(exp.GTE(this=col.copy(), expression=start), exp.LT(this=col.copy(), expression=end))

    if isinstance(wrapped, (exp.TsOrDsToDate, exp.Date, exp.Cast)) and lit.is_string:
        date_s = str(lit.this)
        start = exp.Literal.string(date_s)
        if isinstance(node, exp.EQ):
            nxt = exp.Anonymous(this="DATEADD", expressions=[exp.Var(this="day"), exp.Literal.number(1), start.copy()])
            return exp.and_(exp.GTE(this=col.copy(), expression=start), exp.LT(this=col.copy(), expression=nxt))
        if isinstance(node, exp.GTE):
            return exp.GTE(this=col.copy(), expression=start)
        if isinstance(node, exp.GT):
            nxt = exp.Anonymous(this="DATEADD", expressions=[exp.Var(this="day"), exp.Literal.number(1), start.copy()])
            return exp.GTE(this=col.copy(), expression=nxt)
        if isinstance(node, exp.LT):
            return exp.LT(this=col.copy(), expression=start)
        if isinstance(node, exp.LTE):
            nxt = exp.Anonymous(this="DATEADD", expressions=[exp.Var(this="day"), exp.Literal.number(1), start.copy()])
            return exp.LT(this=col.copy(), expression=nxt)
    return None


def _wrapped_column(node: exp.Expression) -> exp.Column | None:
    if not _is_column_wrapper(node):
        return None
    cols = list(node.find_all(exp.Column))
    if len(cols) != 1:
        return None
    return cols[0]


def _rewrite_or_to_in(tree: exp.Expression) -> bool:
    changed = False
    for where in tree.find_all(exp.Where):
        if where.this is None:
            continue
        new, did = _or_to_in(where.this)
        if did:
            where.set("this", new)
            changed = True
    return changed


def _or_to_in(node: exp.Expression) -> tuple[exp.Expression, bool]:
    if isinstance(node, exp.And):
        left, lch = _or_to_in(node.this)
        right, rch = _or_to_in(node.expression)
        node.set("this", left)
        node.set("expression", right)
        return node, lch or rch
    if isinstance(node, exp.Paren):
        inner, ch = _or_to_in(node.this)
        node.set("this", inner)
        return node, ch
    if isinstance(node, exp.Or):
        parts = _flatten_or(node)
        grouped = _in_from_eq_parts(parts)
        if grouped is not None:
            return grouped, True
        changed = False
        new_parts = []
        for p in parts:
            np, ch = _or_to_in(p)
            changed = changed or ch
            new_parts.append(np)
        if not changed:
            return node, False
        combined = new_parts[0]
        for extra in new_parts[1:]:
            combined = exp.or_(combined, extra)
        return combined, True
    return node, False


def _in_from_eq_parts(parts: list[exp.Expression]) -> exp.Expression | None:
    if len(parts) < 2:
        return None
    col_sql = None
    lits: list[exp.Expression] = []
    for p in parts:
        if not isinstance(p, exp.EQ):
            return None
        if not isinstance(p.this, exp.Column) or not isinstance(p.expression, exp.Literal):
            return None
        key = p.this.sql(dialect="snowflake")
        if col_sql is None:
            col_sql = key
            col = p.this
        elif key != col_sql:
            return None
        lits.append(p.expression)
    if col_sql is None or len(lits) < 2:
        return None
    return exp.In(this=col.copy(), expressions=lits)


def _union_all_suggestion(sql: str) -> str | None:
    try:
        tree = parse_sql(sql)
    except ParseError:
        return None
    found = False
    for union in tree.find_all(exp.Union):
        if union.args.get("distinct") is False:
            continue
        union.set("distinct", False)
        found = True
    if not found:
        return None
    return tree.sql(dialect="snowflake")


def _limit_suggestion(sql: str) -> str | None:
    try:
        tree = parse_sql(sql)
    except ParseError:
        return None
    root = tree
    if isinstance(root, exp.Union):
        return None
    select = root if isinstance(root, exp.Select) else root.find(exp.Select)
    if not isinstance(select, exp.Select):
        return None
    if select.args.get("limit"):
        return None
    has_sort = select.args.get("order") is not None
    has_star = any(isinstance(e, exp.Star) or e.find(exp.Star) for e in select.expressions)
    if not (has_sort or has_star):
        return None
    select.set("limit", exp.Limit(expression=exp.Literal.number(1000)))
    return tree.sql(dialect="snowflake")


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.And):
        return _flatten_and(node.this) + _flatten_and(node.expression)
    if isinstance(node, exp.Paren):
        return _flatten_and(node.this)
    return [node]


def _flatten_or(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.Or):
        return _flatten_or(node.this) + _flatten_or(node.expression)
    if isinstance(node, exp.Paren):
        return _flatten_or(node.this)
    return [node]


def _alias_or_name(node: exp.Expression) -> str | None:
    if node is None:
        return None
    name = getattr(node, "alias_or_name", None)
    if isinstance(name, str) and name:
        return name
    if isinstance(node, exp.Alias):
        return node.alias
    if isinstance(node, exp.Table):
        return node.name
    if isinstance(node, exp.Subquery):
        return node.alias
    return None


def _eq_links(pred: exp.Expression, left_aliases: list[str], right_alias: str) -> bool:
    if not isinstance(pred, exp.EQ):
        return False
    left_t = _column_table(pred.this)
    right_t = _column_table(pred.expression)
    if not left_t or not right_t:
        return False
    left_set = {a.upper() for a in left_aliases}
    r = right_alias.upper()
    return (left_t.upper() in left_set and right_t.upper() == r) or (
        right_t.upper() in left_set and left_t.upper() == r
    )


def _column_table(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Column):
        table = node.table
        if isinstance(table, str) and table:
            return table
        if isinstance(table, exp.Identifier):
            return table.name
        if table:
            return str(table)
    return None
