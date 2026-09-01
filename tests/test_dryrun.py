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


def test_snowflake_nested_operations_array():
    payload = {
        "GlobalStats": {
            "partitionsTotal": 2,
            "partitionsAssigned": 2,
            "bytesAssigned": 1024,
        },
        "Operations": [
            [
                {"id": 0, "operation": "Result", "expressions": ["Z1.ID", "Z2.ID"]},
                {
                    "id": 1,
                    "parentOperators": [0],
                    "operation": "InnerJoin",
                    "expressions": ["joinKey: (Z2.ID = Z1.ID)"],
                },
                {
                    "id": 2,
                    "parentOperators": [1],
                    "operation": "TableScan",
                    "objects": ["TESTDB.TEMPORARY_DOC_TEST.Z2"],
                    "partitionsAssigned": 1,
                    "partitionsTotal": 1,
                    "bytesAssigned": 512,
                },
                {
                    "id": 3,
                    "parentOperators": [1],
                    "operation": "JoinFilter",
                    "expressions": ["joinKey: (Z2.ID = Z1.ID)"],
                },
                {
                    "id": 4,
                    "parentOperators": [3],
                    "operation": "TableScan",
                    "objects": ["TESTDB.TEMPORARY_DOC_TEST.Z1"],
                    "partitionsAssigned": 1,
                    "partitionsTotal": 1,
                    "bytesAssigned": 512,
                },
            ]
        ],
    }
    plan = parse_explain_payload(payload)
    assert [n.operation for n in plan.nodes] == [
        "Result",
        "InnerJoin",
        "TableScan",
        "JoinFilter",
        "TableScan",
    ]
    assert plan.global_stats.bytes_assigned == 1024
    result = run_dry_run(
        DryRunRequest(sql="select 1", explain_json=payload, warehouse_size="XSMALL"),
        allow_snowflake=False,
    )
    assert result.source == "pasted_json"
    assert len(result.plan.nodes) == 5
    assert any("pasted EXPLAIN JSON" in n for n in result.static_notes)


def test_explain_json_worksheet_cell_string():
    inner = {
        "GlobalStats": {"bytesAssigned": 2048, "partitionsAssigned": 1, "partitionsTotal": 1},
        "Operations": [[{"id": 0, "operation": "Result"}, {"id": 1, "parent": 0, "operation": "TableScan"}]],
    }
    cell = 'EXPLAIN\n' + __import__("json").dumps(inner, separators=(",", ":"))
    plan = parse_explain_payload(cell)
    assert len(plan.nodes) == 2
    assert plan.nodes[1].parent_ids == [0]
    assert plan.nodes[1].operation == "TableScan"


def test_sort_without_limit():
    sql = "SELECT * FROM fact.page_views WHERE event_date >= '2023-01-01' ORDER BY event_ts DESC"
    result = run_dry_run(DryRunRequest(sql=sql, warehouse_size="SMALL"), allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "SORT_NO_LIMIT" in codes
    assert "SELECT_STAR" in codes


def test_explain_bytes_runtime_near_scan_io():
    """12 GiB EXPLAIN IO on MEDIUM should be ~seconds/minutes, not 40+ minutes."""
    payload = {
        "GlobalStats": {
            "partitionsTotal": 120,
            "partitionsAssigned": 120,
            "bytesAssigned": 12 * 1024**3,
        },
        "Operations": [
            [
                {"id": 0, "operation": "Result"},
                {
                    "id": 1,
                    "parentOperators": [0],
                    "operation": "InnerJoin",
                    "expressions": ["joinKey: (a = b)"],
                },
                {
                    "id": 2,
                    "parentOperators": [1],
                    "operation": "TableScan",
                    "objects": ["DB.SC.ORDERS"],
                    "bytesAssigned": 8 * 1024**3,
                    "partitionsAssigned": 80,
                    "partitionsTotal": 80,
                },
                {
                    "id": 3,
                    "parentOperators": [1],
                    "operation": "TableScan",
                    "objects": ["DB.SC.CUSTOMERS"],
                    "bytesAssigned": 4 * 1024**3,
                    "partitionsAssigned": 40,
                    "partitionsTotal": 40,
                },
            ]
        ],
    }
    result = run_dry_run(
        DryRunRequest(sql="select 1 from orders o join customers c on o.id = c.id", explain_json=payload, warehouse_size="MEDIUM"),
        allow_snowflake=False,
    )
    assert result.source == "pasted_json"
    assert "CROSS_JOIN" not in {f.code for f in result.findings}
    # 12 GiB / (1.25 GiB/s * 4^0.85) ≈ 3s plus light join factor — well under 10 minutes.
    assert result.warehouse.estimated_seconds_on_given < 600
    assert result.warehouse.estimated_seconds_on_given >= 2


def test_empty_rejected_by_model_still_runs_notes():
    result = run_dry_run(DryRunRequest(sql="SELECT 1"), allow_snowflake=False)
    assert result.source == "synthetic"
    assert result.warehouse.given_size == "XSMALL"
    assert result.score == 100
    assert result.score_label == "healthy"


def test_comma_join_becomes_inner_join():
    sql = """
    SELECT o.order_id, c.email
    FROM analytics.orders o, analytics.customers c
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= '2024-01-01'
    """
    result = run_dry_run(DryRunRequest(sql=sql, warehouse_size="MEDIUM"), allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "CROSS_JOIN" not in codes
    assert "COMMA_JOIN" in codes
    assert any(n.operation == "InnerJoin" for n in result.plan.nodes)
    assert result.advised_sql
    assert "INNER JOIN" in result.advised_sql.upper()
    assert "ON" in result.advised_sql.upper()
    assert any(rw.safe for rw in result.rewrites)


def test_year_filter_rewritten_to_range():
    sql = "SELECT * FROM fact.page_views WHERE YEAR(event_date) = 2024"
    result = run_dry_run(DryRunRequest(sql=sql), allow_snowflake=False)
    codes = {f.code for f in result.findings}
    assert "NON_SARGABLE" in codes
    assert result.advised_sql
    assert "YEAR" not in result.advised_sql.upper()
    assert "2024-01-01" in result.advised_sql
    assert "2025-01-01" in result.advised_sql


def test_tablescan_clustering_key_filter():
    payload = {
        "GlobalStats": {"bytesAssigned": 50 * 1024**2, "partitionsAssigned": 10, "partitionsTotal": 100},
        "Operations": [
            [
                {"id": 0, "operation": "Result"},
                {"id": 1, "parentOperators": [0], "operation": "Filter", "expressions": ["D.EVENT_DATE >= '2024-01-01'"]},
                {
                    "id": 4,
                    "parentOperators": [1],
                    "operation": "TableScan",
                    "objects": ["ANALYTICS.FACT.EVENTS"],
                    "expressions": ["EVENT_DATE", "filter:(EVENT_DATE >= '2024-01-01')"],
                    "partitionsAssigned": 10,
                    "partitionsTotal": 100,
                    "bytesAssigned": 50 * 1024**2,
                },
            ]
        ],
    }
    result = run_dry_run(
        DryRunRequest(sql="SELECT * FROM analytics.fact.events WHERE event_date >= '2024-01-01'", explain_json=payload),
        allow_snowflake=False,
    )
    codes = {f.code for f in result.findings}
    assert "CLUSTERING_KEY_FILTER" in codes
    assert "UNFILTERED_SCAN" not in codes
    cluster = next(f for f in result.findings if f.code == "CLUSTERING_KEY_FILTER")
    assert cluster.operator_ids == [4]
    assert cluster.severity == "info"
    assert "clustering" in cluster.title.lower()


def test_or_equalities_become_in():
    sql = "SELECT COUNT(*) FROM t WHERE status = 'X' OR status = 'Y' OR status = 'Z'"
    result = run_dry_run(DryRunRequest(sql=sql), allow_snowflake=False)
    assert any(f.code == "OR_PREDICATE" for f in result.findings)
    assert result.advised_sql
    assert "IN" in result.advised_sql.upper()

