# Snowflake Dry Run

Analyze a Snowflake query without running it: compile an `EXPLAIN USING JSON` plan (live, pasted, or synthesized from SQL), highlight cartesian joins and other row explosions, and estimate runtime on a named warehouse.

## What it does

1. **Plan** — Uses Snowflake `EXPLAIN USING JSON` when you provide account credentials. Otherwise it builds an EXPLAIN-shaped tree from the SQL (sqlglot, Snowflake dialect), or accepts pasted EXPLAIN JSON.
2. **Findings** — Flags cross/cartesian joins, join predicates missing, `FLATTEN` / `LATERAL` row multiplication, unfiltered or poorly pruned scans, global sorts without `LIMIT`, window functions, and `SELECT *`.
3. **Warehouse** — You pick the size the query would run on (`XSMALL` … `X4LARGE`). The tool estimates elapsed seconds and credit-hours on that size, and recommends a size for the same work.

Estimates are **not** a Query Profile. Confirm on Snowflake before changing production warehouses.

## Run locally

Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn snowflake_dryrun.app:app --host 127.0.0.1 --port 48731
```

Open http://127.0.0.1:48731

### CLI

```bash
snowflake-dryrun "SELECT * FROM a CROSS JOIN b" --warehouse-size MEDIUM
snowflake-dryrun --sql-file query.sql --explain-json explain.json --json
```

Optional environment variables for a live EXPLAIN: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`.

### Tests

```bash
pytest -q
```

## HTTP API

`POST /api/dry-run`

```json
{
  "sql": "SELECT * FROM orders o CROSS JOIN customers c",
  "warehouse_size": "MEDIUM",
  "explain_json": null,
  "account": null,
  "user": null,
  "password": null,
  "warehouse": null,
  "database": null,
  "schema": null
}
```

If `explain_json` is set, it is the plan of record. If Snowflake credentials are set and EXPLAIN succeeds, partition and byte stats come from Snowflake. Otherwise the SQL AST is used.

## How sizing works

Work is `bytesAssigned` (or a heuristic scan size) multiplied for joins, sorts, windows, flatten, and cartesian products. Runtime assumes ~80 MiB/s mixed throughput on X-Small, scaling sub-linearly with credit count. A cartesian join floors the recommendation at MEDIUM because byte estimates understate exploded cardinality.
