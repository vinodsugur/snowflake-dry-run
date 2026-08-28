from fastapi.testclient import TestClient

from snowflake_dryrun.app import app

client = TestClient(app)


def test_health():
    assert client.get("/api/health").json() == {"status": "ok"}


def test_index():
    res = client.get("/")
    assert res.status_code == 200
    assert "Snowflake Dry Run" in res.text


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


def test_dry_run_requires_input():
    res = client.post("/api/dry-run", json={"sql": ""})
    assert res.status_code == 400
