from snowflake_dryrun.engine import run_dry_run
from snowflake_dryrun.models import DryRunRequest
from snowflake_dryrun.plan import parse_explain_payload


def test_cross_join_is_critical():
    req = DryRunRequest(
        sql="SELECT * FROM analytics.orders o CROSS JOIN analytics.customers c",
        warehouse_size="XSMALL",
    )
    result = run_dry_run(req, allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "CROSS_JOIN" in codes
    assert result.findings[0].severity == "critical"
    assert result.warehouse.recommended_size != "XSMALL"


def test_filtered_inner_join_is_clean():
    sql = """
    SELECT o.order_id, c.email
    FROM analytics.orders o
    INNER JOIN analytics.customers c ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2024-01-01'
    LIMIT 100
    """
    result = run_dry_run(DryRunRequest(sql=sql, warehouse_size="MEDIUM"), allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "CROSS_JOIN" not in codes
    assert result.plan.nodes
    assert any(n.operation == "InnerJoin" for n in result.plan.nodes)


def test_flatten_flagged():
    sql = """
    SELECT u.user_id, f.value
    FROM raw.events u,
    LATERAL FLATTEN(input => u.payload:items) f
    """
    result = run_dry_run(DryRunRequest(sql=sql), allow_snowflake=False)
    assert any(f.code in {"FLATTEN_EXPLODE", "CROSS_JOIN"} for f in result.findings)


def test_pasted_explain_json_cartesian():
    payload = {
        "GlobalStats": {
            "partitionsTotal": 120,
            "partitionsAssigned": 120,
            "bytesAssigned": 12 * 1024**3,
        },
        "Operations": [
            {"id": 0, "operation": "Result"},
            {
                "id": 1,
                "parentOperators": [0],
                "operation": "InnerJoin",
                "expressions": [],
            },
            {
                "id": 2,
                "parentOperators": [1],
                "operation": "TableScan",
                "objects": ["DB.SC.ORDERS"],
                "partitionsAssigned": 80,
                "partitionsTotal": 80,
                "bytesAssigned": 8 * 1024**3,
            },
            {
                "id": 3,
                "parentOperators": [1],
                "operation": "TableScan",
                "objects": ["DB.SC.CUSTOMERS"],
                "partitionsAssigned": 40,
                "partitionsTotal": 40,
                "bytesAssigned": 4 * 1024**3,
            },
        ],
    }
    plan = parse_explain_payload(payload)
    assert plan.global_stats.bytes_assigned == 12 * 1024**3
    result = run_dry_run(
        DryRunRequest(sql="select 1", explain_json=payload, warehouse_size="XSMALL"),
        allow_snowflake=False,
    )
    assert any(f.code == "CROSS_JOIN" for f in result.findings)
    assert any(f.code == "WEAK_PRUNING" for f in result.findings)
    assert result.source == "pasted_json"
    assert result.warehouse.estimated_seconds_on_given > result.warehouse.estimated_seconds_on_recommended


def test_explain_string_unwrap():
    inner = {
        "GlobalStats": {"bytesAssigned": 100},
        "Operations": [{"id": 0, "operation": "Result"}],
    }
    wrapped = [{"EXPLAIN": __import__("json").dumps(inner)}]
    plan = parse_explain_payload(wrapped)
    assert plan.nodes[0].operation == "Result"
    assert plan.global_stats.bytes_assigned == 100


def test_sort_without_limit():
    sql = "SELECT * FROM fact.page_views WHERE event_date >= '2023-01-01' ORDER BY event_ts DESC"
    result = run_dry_run(DryRunRequest(sql=sql, warehouse_size="SMALL"), allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "SORT_NO_LIMIT" in codes
    assert "SELECT_STAR" in codes


def test_empty_rejected_by_model_still_runs_notes():
    result = run_dry_run(DryRunRequest(sql="SELECT 1"), allow_snowflake=False)
    assert result.source == "synthetic"
    assert result.warehouse.given_size == "XSMALL"
