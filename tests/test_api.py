from fastapi.testclient import TestClient

from snowflake_dryrun.app import app

client = TestClient(app)


def test_health():
    assert client.get("/api/health").json() == {"status": "ok"}


def test_index():
    res = client.get("/")
    assert res.status_code == 200
    assert "Snowflake Query Advisor" in res.text


def test_dry_run_endpoint():
    res = client.post(
        "/api/dry-run",
        json={
            "sql": "SELECT * FROM a CROSS JOIN b",
            "warehouse_size": "SMALL",
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert any(f["code"] == "CROSS_JOIN" for f in body["findings"])
    assert body["warehouse"]["given_size"] == "SMALL"
    assert body["score"] < 85
    assert body["score_label"] == "dangerous"


def test_dry_run_comma_join_rewrite():
    res = client.post(
        "/api/dry-run",
        json={
            "sql": (
                "SELECT o.id, c.email FROM analytics.orders o, analytics.customers c "
                "WHERE o.customer_id = c.customer_id AND o.order_date >= '2024-01-01'"
            ),
            "warehouse_size": "MEDIUM",
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["advised_sql"]
    assert "INNER JOIN" in body["advised_sql"].upper()
    assert any(f["code"] == "COMMA_JOIN" for f in body["findings"])
    assert not any(f["code"] == "CROSS_JOIN" for f in body["findings"])


def test_dry_run_nested_explain_json():
    payload = {
        "GlobalStats": {"bytesAssigned": 4096, "partitionsAssigned": 2, "partitionsTotal": 2},
        "Operations": [
            [
                {"id": 0, "operation": "Result"},
                {"id": 1, "parentOperators": [0], "operation": "TableScan", "objects": ["DB.SC.T"]},
            ]
        ],
    }
    res = client.post(
        "/api/dry-run",
        json={"sql": "SELECT * FROM t", "warehouse_size": "SMALL", "explain_json": payload},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["source"] == "pasted_json"
    assert [n["operation"] for n in body["plan"]["nodes"]] == ["Result", "TableScan"]


def test_dry_run_requires_input():
    res = client.post("/api/dry-run", json={"sql": ""})
    assert res.status_code == 400


