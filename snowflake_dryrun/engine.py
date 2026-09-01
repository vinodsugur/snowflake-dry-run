from __future__ import annotations

from snowflake_dryrun.advisor import advise_sql, score_findings
from snowflake_dryrun.analyzer import analyze_plan
from snowflake_dryrun.models import DryRunRequest, DryRunResult, ParsedPlan
from snowflake_dryrun.plan import parse_explain_payload
from snowflake_dryrun.snowflake_client import fetch_plan_or_none
from snowflake_dryrun.sql_static import static_notes, synthesize_plan
from snowflake_dryrun.warehouse import advise_warehouse


def run_dry_run(req: DryRunRequest, *, allow_snowflake: bool = True) -> DryRunResult:
    sql = (req.sql or "").strip()
    notes = static_notes(sql) if sql else []
    plan: ParsedPlan
    connected = False
    source = "synthetic"
    connect_error: str | None = None

    if req.explain_json:
        try:
            plan = parse_explain_payload(req.explain_json)
        except Exception as exc:  # noqa: BLE001 — surface malformed EXPLAIN to the UI
            notes.append(f"EXPLAIN JSON could not be parsed: {exc}")
            plan = synthesize_plan(sql) if sql else ParsedPlan()
            source = "synthetic"
        else:
            plan.source = "pasted_json"
            source = "pasted_json"
            if not plan.nodes:
                notes.append(
                    "Pasted EXPLAIN JSON had no operators after parsing. "
                    "Copy the JSON cell from EXPLAIN USING JSON (not tabular EXPLAIN)."
                )
            elif sql:
                notes.append("Plan operators and byte stats come from the pasted EXPLAIN JSON.")
    elif allow_snowflake and sql:
        creds = req.model_dump(by_alias=True)
        raw, connect_error = fetch_plan_or_none(sql, creds)
        if raw:
            plan = parse_explain_payload(raw)
            plan.source = "snowflake"
            source = "snowflake"
            connected = True
        else:
            plan = synthesize_plan(sql) if sql else ParsedPlan()
            source = "synthetic"
    else:
        plan = synthesize_plan(sql) if sql else ParsedPlan()
        source = "synthetic"

    if connect_error:
        notes.append(f"Snowflake EXPLAIN was not used: {connect_error}")
    if source == "synthetic" and sql:
        notes.append(
            "Plan is synthesized from the SQL parse tree (no live EXPLAIN). "
            "Connect to Snowflake or paste EXPLAIN USING JSON output for partition/byte stats."
        )

    findings = analyze_plan(plan, sql=sql)
    sql_findings, rewrites, advised_sql = advise_sql(sql)
    findings = _merge_findings(findings, sql_findings)
    score, score_label = score_findings(findings)
    warehouse = advise_warehouse(plan, req.warehouse_size, [f.code for f in findings])
    return DryRunResult(
        sql=sql,
        source=source,
        findings=findings,
        plan=plan,
        warehouse=warehouse,
        static_notes=notes,
        connected=connected,
        score=score,
        score_label=score_label,
        rewrites=rewrites,
        advised_sql=advised_sql,
    )


def _merge_findings(plan_findings, sql_findings):
    from snowflake_dryrun.analyzer import _dedupe

    return _dedupe(list(plan_findings) + list(sql_findings))
